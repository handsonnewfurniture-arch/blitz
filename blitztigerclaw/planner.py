"""Query planner for BlitzTigerClaw v0.4.0.

Compiles pipeline definitions into optimized ExecutionDAGs.

Optimization passes (applied in order):
  1. Operator fusion   — merge adjacent row-level steps into single-pass nodes
  2. Filter pushdown   — move filters closer to data sources
  3. Projection tracking — determine which fields each node needs
  4. Strategy annotation — assign execution strategy per node
  5. Parallelism detection — group independent nodes for concurrent execution
"""

from __future__ import annotations

import re
from typing import Any

from blitztigerclaw.dag import ExecutionDAG, DagNode, DagEdge
from blitztigerclaw.steps import StepRegistry, discover as _discover_steps


class Planner:
    """Compiles and optimizes pipeline execution plans."""

    # ------------------------------------------------------------------
    # Compilation: YAML -> DAG
    # ------------------------------------------------------------------

    def compile_linear(self, steps: list[tuple[str, dict]]) -> ExecutionDAG:
        """Compile a linear step list (backward-compatible YAML) into a DAG chain."""
        dag = ExecutionDAG()
        prev_id = None

        for i, (step_type, config) in enumerate(steps):
            node_id = f"s{i}_{step_type}"
            dag.add_node(DagNode(id=node_id, step_type=step_type, config=dict(config)))
            if prev_id is not None:
                dag.add_edge(prev_id, node_id)
            prev_id = node_id

        return dag

    def compile_graph(self, graph_def: dict, variables: dict | None = None) -> ExecutionDAG:
        """Compile an explicit graph definition into a DAG.

        Graph YAML format:
            graph:
              fetch_data:
                step: fetch
                config: {url: "..."}
              clean_it:
                step: clean
                after: fetch_data
                config: {trim: [name]}
              merge_node:
                step: join
                after: [branch_a, branch_b]
                config: {on: id, how: inner}
        """
        dag = ExecutionDAG()

        # First pass: create all nodes
        for node_id, node_def in graph_def.items():
            step_type = node_def.get("step") or node_def.get("type", "unknown")
            config = dict(node_def.get("config", {}))
            dag.add_node(DagNode(id=node_id, step_type=step_type, config=config))

        # Second pass: create edges
        for node_id, node_def in graph_def.items():
            after = node_def.get("after", [])
            if isinstance(after, str):
                after = [after]
            for i, pred_id in enumerate(after):
                if pred_id not in dag.nodes:
                    raise ValueError(
                        f"Node '{node_id}' references unknown predecessor '{pred_id}'"
                    )
                port = f"input_{i}" if len(after) > 1 else "default"
                dag.add_edge(pred_id, node_id, port=port)

        return dag

    # ------------------------------------------------------------------
    # Optimization: DAG -> optimized DAG
    # ------------------------------------------------------------------

    def optimize(self, dag: ExecutionDAG) -> ExecutionDAG:
        """Apply all optimization passes in order."""
        _discover_steps()
        dag = self._pass_fuse_operators(dag)
        dag = self._pass_push_filters(dag)
        dag = self._pass_track_projections(dag)
        dag = self._pass_annotate_strategies(dag)
        dag = self._pass_assign_levels(dag)
        return dag

    # ------------------------------------------------------------------
    # Pass 1: Operator Fusion
    # ------------------------------------------------------------------

    _BREAKS_FUSION = frozenset({"sort", "dedupe", "limit"})

    def _pass_fuse_operators(self, dag: ExecutionDAG) -> ExecutionDAG:
        """Merge chains of adjacent row-level steps into fused nodes.

        transform -> clean -> transform (all row-level, no sort/dedupe/limit)
        becomes a single _fused node that executes all three in one pass.
        """
        changed = True
        while changed:
            changed = False
            for node_id in list(dag.topological_sort()):
                if node_id not in dag.nodes:
                    continue
                node = dag.nodes[node_id]

                if not self._is_fusable(node):
                    continue

                succs = dag.successors(node_id)
                if len(succs) != 1:
                    continue

                succ_id = succs[0]
                if succ_id not in dag.nodes:
                    continue
                succ = dag.nodes[succ_id]

                if not self._is_fusable(succ):
                    continue
                if len(dag.predecessors(succ_id)) != 1:
                    continue

                # Fuse: collect ops from both nodes
                node_ops = node.config.get("_fused_ops") or [
                    {"type": node.step_type, "config": self._clean_config(node.config)}
                ]
                succ_ops = succ.config.get("_fused_ops") or [
                    {"type": succ.step_type, "config": self._clean_config(succ.config)}
                ]

                node.step_type = "_fused"
                node.config = {"_fused_ops": node_ops + succ_ops}

                # Rewire: succ's downstream now connects to node
                dag.redirect_edges(succ_id, node_id)
                # Remove succ and the edge between node -> succ
                dag.edges = [
                    e for e in dag.edges
                    if not (e.source == node_id and e.target == succ_id)
                ]
                del dag.nodes[succ_id]
                changed = True
                break  # Restart after mutation

        return dag

    def _is_fusable(self, node: DagNode) -> bool:
        """Check if a node can participate in operator fusion.

        Reads StepMeta.fusable — no hardcoded step type set.
        """
        if node.step_type == "_fused":
            # All constituent ops must themselves be fusable
            for op in node.config.get("_fused_ops", []):
                op_type = op["type"]
                try:
                    op_meta = StepRegistry.get_meta(op_type)
                except ValueError:
                    return False
                if not op_meta.fusable:
                    return False
                if any(k in op["config"] for k in op_meta.streaming_breakers):
                    return False
            return True

        try:
            meta = StepRegistry.get_meta(node.step_type)
        except ValueError:
            return False

        if not meta.fusable:
            return False
        if meta.streaming_breakers and any(
            k in node.config for k in meta.streaming_breakers
        ):
            return False
        return True

    # ------------------------------------------------------------------
    # Pass 2: Filter Pushdown
    # ------------------------------------------------------------------

    def _pass_push_filters(self, dag: ExecutionDAG) -> ExecutionDAG:
        """Move pure-filter transforms closer to data sources.

        A transform with ONLY a filter can be pushed past upstream
        select-only transforms (filter runs on more columns, select
        then runs on fewer rows).
        """
        changed = True
        while changed:
            changed = False
            for node_id in list(dag.topological_sort()):
                if node_id not in dag.nodes:
                    continue
                node = dag.nodes[node_id]

                # Only push pure filter transforms
                if node.step_type != "transform":
                    continue
                real_keys = {k for k in node.config if not k.startswith("_")}
                if real_keys != {"filter"}:
                    continue

                preds = dag.predecessors(node_id)
                if len(preds) != 1:
                    continue
                pred_id = preds[0]
                pred = dag.nodes[pred_id]

                # Can push past select-only transform
                if pred.step_type != "transform":
                    continue
                pred_keys = {k for k in pred.config if not k.startswith("_")}
                if pred_keys != {"select"}:
                    continue

                # Verify single-successor / single-predecessor
                if len(dag.successors(pred_id)) != 1:
                    continue
                if len(dag.predecessors(node_id)) != 1:
                    continue

                dag.swap_adjacent(pred_id, node_id)
                changed = True
                break

        return dag

    # ------------------------------------------------------------------
    # Pass 3: Projection Tracking
    # ------------------------------------------------------------------

    def _pass_track_projections(self, dag: ExecutionDAG) -> ExecutionDAG:
        """Walk backward through DAG to determine which fields each node needs.

        Annotates nodes with _needed_fields for downstream optimizers.
        """
        order = dag.topological_sort()
        needed: dict[str, set[str] | None] = {}

        for node_id in reversed(order):
            node = dag.nodes[node_id]
            succs = dag.successors(node_id)

            # Gather downstream needs
            downstream: set[str] | None = set()
            for s in succs:
                s_needs = needed.get(s)
                if s_needs is None:
                    downstream = None
                    break
                downstream |= s_needs

            # What does this node consume?
            node_needs = self._infer_field_needs(node)

            if downstream is None or node_needs is None:
                needed[node_id] = None
            else:
                needed[node_id] = downstream | node_needs

            # Annotate
            if needed[node_id] is not None:
                node.config["_needed_fields"] = sorted(needed[node_id])

        return dag

    def _infer_field_needs(self, node: DagNode) -> set[str] | None:
        """Return the set of input fields a node reads. None = all/unknown.

        Reads StepMeta.is_source — source nodes need no input fields.
        aggregate still has custom logic for group_by/functions introspection.
        """
        c = node.config

        # aggregate: introspect group_by + function arguments
        if node.step_type == "aggregate":
            needs: set[str] = set()
            gb = c.get("group_by", [])
            if isinstance(gb, str):
                gb = [gb]
            needs.update(gb)
            for func_str in c.get("functions", {}).values():
                m = re.search(r"\((\w+)\)", func_str)
                if m:
                    needs.add(m.group(1))
            return needs

        # Fused nodes are too complex to introspect
        if node.step_type == "_fused":
            return None

        # Source nodes produce data — they don't read input fields
        try:
            meta = StepRegistry.get_meta(node.step_type)
        except ValueError:
            return None
        if meta.is_source:
            return set()

        return None  # Unknown

    # ------------------------------------------------------------------
    # Pass 4: Strategy Annotation
    # ------------------------------------------------------------------

    def _pass_annotate_strategies(self, dag: ExecutionDAG) -> ExecutionDAG:
        """Assign execution strategy to each node based on type."""
        for node in dag.nodes.values():
            node.strategy = self._decide_strategy(node)
        return dag

    @staticmethod
    def _decide_strategy(node: DagNode) -> str:
        """Generic strategy selection via StepMeta — no hardcoded step names."""
        st = node.step_type
        est = node.estimated_rows or 0

        if st == "_fused":
            return "sync"

        try:
            meta = StepRegistry.get_meta(st)
        except ValueError:
            return "sync"

        chosen = meta.default_strategy
        for threshold, strategy in meta.strategy_escalations:
            if est > threshold:
                if strategy == "streaming" and meta.streaming_breakers:
                    if any(k in node.config for k in meta.streaming_breakers):
                        continue
                chosen = strategy

        return chosen

    # ------------------------------------------------------------------
    # Pass 5: Parallelism Detection
    # ------------------------------------------------------------------

    def _pass_assign_levels(self, dag: ExecutionDAG) -> ExecutionDAG:
        """Assign parallel execution levels to each node."""
        for level, group in enumerate(dag.parallel_groups()):
            for node_id in group:
                dag.nodes[node_id].parallel_level = level
        return dag

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_config(config: dict) -> dict:
        """Remove planner-internal keys from config."""
        return {k: v for k, v in config.items() if not k.startswith("_")}
