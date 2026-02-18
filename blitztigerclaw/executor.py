"""DAG executor for BlitzTigerClaw v0.4.0.

Replaces the linear left-fold with graph traversal.
Executes independent nodes concurrently via asyncio.gather.
Supports operator fusion, multi-input nodes, and isolated contexts.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from blitztigerclaw.context import Context
from blitztigerclaw.dag import ExecutionDAG, DagNode
from blitztigerclaw.schema import DataSchema
from blitztigerclaw.steps import StepRegistry


@dataclass
class NodeResult:
    """Output of a single DAG node execution."""

    node_id: str
    data: list[dict[str, Any]]
    schema: DataSchema
    duration_ms: float
    errors: list[str] = field(default_factory=list)


class DagExecutor:
    """Executes an optimized DAG with concurrent independent nodes.

    Flow:
    1. Compute parallel groups (topological levels)
    2. For each level:
       - Single node: execute directly
       - Multiple nodes: execute concurrently via asyncio.gather
    3. Each node gets its own isolated Context copy
    4. Final result comes from the leaf node(s)
    """

    def __init__(self, dag: ExecutionDAG, verbose: bool = False):
        self.dag = dag
        self.verbose = verbose
        self._results: dict[str, NodeResult] = {}

    async def execute(self, context: Context) -> Context:
        """Execute the full DAG, returning the updated Context."""
        groups = self.dag.parallel_groups()
        total = len(self.dag.nodes)
        node_index = 0

        if self.verbose and any(len(g) > 1 for g in groups):
            print("  [DAG MODE]")

        for group in groups:
            if len(group) == 1:
                node_index += 1
                await self._execute_node(group[0], context, node_index, total)
            else:
                # Concurrent execution of independent nodes
                if self.verbose:
                    print(f"  [parallel: {', '.join(self.dag.nodes[n].step_type for n in group)}]")
                tasks = []
                for nid in group:
                    node_index += 1
                    tasks.append(self._execute_node(nid, context, node_index, total))
                await asyncio.gather(*tasks)

        # Set final data from leaf node(s)
        leaves = self.dag.leaves()
        if len(leaves) == 1:
            context.set_data(self._results[leaves[0]].data)
        elif leaves:
            merged: list[dict] = []
            for lid in leaves:
                merged.extend(self._results[lid].data)
            context.set_data(merged)

        return context

    async def _execute_node(
        self,
        node_id: str,
        context: Context,
        step_num: int,
        total: int,
    ) -> None:
        """Execute a single DAG node."""
        node = self.dag.nodes[node_id]

        if self.verbose:
            fused_tag = ""
            if node.step_type == "_fused":
                ops = [op["type"] for op in node.config.get("_fused_ops", [])]
                fused_tag = f" (fused: {'+'.join(ops)})"
            print(
                f"  [{step_num}/{total}] {node.step_type}{fused_tag}...",
                end=" ",
                flush=True,
            )

        # Gather input data from predecessors
        input_data = self._gather_inputs(node_id, context)

        # Create isolated Context for this node
        node_context = Context(
            vars=dict(context.vars),
            pipeline_name=context.pipeline_name,
        )
        node_context.set_data(input_data)

        # For multi-input nodes, populate the inputs dict with secondary inputs.
        # Primary input (preds[0]) is already in context.data via _gather_inputs.
        preds = self.dag.predecessors(node_id)
        if len(preds) > 1:
            primary_source = preds[0]
            in_edges = self.dag.in_edges(node_id)
            for edge in in_edges:
                if edge.source in self._results and edge.source != primary_source:
                    node_context.inputs[edge.port] = self._results[edge.source].data

        # Execute
        start = time.time()

        if node.step_type == "_fused":
            result = await self._execute_fused(node, node_context)
        else:
            result = await self._execute_step(node, node_context)

        duration_ms = (time.time() - start) * 1000

        # Store result
        schema = DataSchema.infer(result) if result else DataSchema.unknown()
        self._results[node_id] = NodeResult(
            node_id=node_id,
            data=result,
            schema=schema,
            duration_ms=duration_ms,
        )

        # Propagate vars back to main context
        for k, v in node_context.vars.items():
            context.vars[k] = v

        # Log step result
        context.log_step(step_num - 1, node.step_type, len(result), duration_ms)

        if self.verbose:
            print(f"{len(result)} rows in {duration_ms:.0f}ms")

    def _gather_inputs(self, node_id: str, context: Context) -> list[dict]:
        """Collect input data for a node from its predecessors."""
        preds = self.dag.predecessors(node_id)

        if not preds:
            # Root node — use initial Context data
            return list(context.data)

        if len(preds) == 1:
            return self._results[preds[0]].data

        # Multiple predecessors — use first predecessor as primary data
        # (other inputs are available via context.inputs)
        return self._results[preds[0]].data

    async def _execute_step(
        self,
        node: DagNode,
        context: Context,
    ) -> list[dict[str, Any]]:
        """Execute a single step with strategy dispatch."""
        # Strip planner annotations from config (keys starting with _)
        config = {
            k: v for k, v in node.config.items()
            if not (isinstance(k, str) and k.startswith("_"))
        }

        step_class = StepRegistry.get(node.step_type)
        step = step_class(config, context)

        if node.strategy == "streaming" and step.supports_streaming():
            result = []
            async for row in step.execute_stream():
                result.append(row)
            return result
        elif node.strategy == "async":
            return await step.execute_async()
        elif node.strategy == "multiprocess":
            return await step.execute_pooled()
        else:
            return await step.execute()

    async def _execute_fused(
        self,
        node: DagNode,
        context: Context,
    ) -> list[dict[str, Any]]:
        """Execute a fused node — multiple operations in one sequential pass.

        Avoids per-step overhead (context.set_data, metrics, checkpoint)
        between the fused operations.
        """
        fused_ops = node.config.get("_fused_ops", [])
        data = list(context.data)

        for op in fused_ops:
            op_type = op["type"]
            op_config = op["config"]

            sub_context = Context(
                vars=dict(context.vars),
                pipeline_name=context.pipeline_name,
            )
            sub_context.set_data(data)

            step_class = StepRegistry.get(op_type)
            step = step_class(op_config, sub_context)
            data = await step.execute()

            # Propagate vars
            for k, v in sub_context.vars.items():
                context.vars[k] = v

        return data

    def get_node_result(self, node_id: str) -> NodeResult | None:
        """Get the execution result for a specific node (for debugging)."""
        return self._results.get(node_id)

    def get_execution_summary(self) -> list[dict]:
        """Summary of all executed nodes for metrics."""
        return [
            {
                "node_id": r.node_id,
                "step_type": self.dag.nodes[r.node_id].step_type
                if r.node_id in self.dag.nodes else "_unknown",
                "rows": len(r.data),
                "duration_ms": round(r.duration_ms, 1),
                "schema_width": r.schema.width,
                "errors": len(r.errors),
            }
            for r in self._results.values()
        ]
