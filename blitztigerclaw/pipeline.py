from __future__ import annotations

import time
from typing import TYPE_CHECKING

from blitztigerclaw.context import Context
from blitztigerclaw.optimizer import Optimizer
from blitztigerclaw.planner import Planner
from blitztigerclaw.executor import DagExecutor
from blitztigerclaw.exceptions import StepError
from blitztigerclaw.checkpoint import CheckpointManager

if TYPE_CHECKING:
    from blitztigerclaw.parser import PipelineDefinition

from blitztigerclaw.steps import StepRegistry, discover as _discover_steps

_discover_steps()
from blitztigerclaw.tps.metrics import MetricsStore
from blitztigerclaw.tps.kanban import KanbanBoard
from blitztigerclaw.tps.change_detector import ChangeDetector


class Pipeline:
    """Executes a parsed pipeline definition.

    v0.4.0: DAG execution model — compiles YAML into an optimized
    directed acyclic graph via the Planner, then executes with
    DagExecutor (concurrent independent nodes, operator fusion).

    Falls back to legacy sequential/streaming execution for
    checkpoint resume.

    TPS principles: KAIZEN (metrics), KANBAN (board), JIT, MUDA.
    """

    def __init__(
        self,
        definition: "PipelineDefinition",
        verbose: bool = False,
        kanban_id: str | None = None,
        resume: bool = False,
    ):
        self.definition = definition
        self.verbose = verbose
        self.optimizer = Optimizer()
        self.metrics = MetricsStore()
        self.change_detector = ChangeDetector()
        self.kanban_id = kanban_id
        self.kanban = KanbanBoard()
        self.resume = resume
        self.checkpoint = (
            CheckpointManager(definition.name)
            if definition.checkpoint or resume
            else None
        )

    async def run(self) -> Context:
        context = Context(
            vars=dict(self.definition.vars),
            pipeline_name=self.definition.name,
        )
        context.vars["_pipeline_name"] = self.definition.name

        started_at = time.time()
        status = "completed"
        error_message = None

        # KANBAN: Mark in progress
        if self.kanban_id:
            self.kanban.update_state(self.kanban_id, "in_progress")

        # Resume from checkpoint: fall back to legacy sequential executor
        start_step = 0
        use_legacy = False
        if self.resume and self.checkpoint:
            saved = self.checkpoint.load()
            if saved is not None:
                start_step = saved["completed_step"] + 1
                context.set_data(saved["data"])
                context.vars.update(saved.get("vars", {}))
                for r in saved.get("results", []):
                    context.log_step(
                        r.get("step_index", 0),
                        r.get("step_type", "unknown"),
                        r.get("row_count", 0),
                        r.get("duration_ms", 0),
                        r.get("errors", []),
                    )
                if self.verbose:
                    print(
                        f"  Resuming from step {start_step + 1} "
                        f"({len(context.data)} rows from checkpoint)"
                    )
                use_legacy = True  # Checkpoint resume uses legacy path

        try:
            if use_legacy:
                await self._run_sequential(context, start_step)
            else:
                await self._run_dag(context)

        except Exception as e:
            status = "failed"
            error_message = str(e)
            raise
        finally:
            finished_at = time.time()

            # KAIZEN: Record metrics
            try:
                pipeline_hash = self.change_detector.get_pipeline_hash(
                    str(self.definition)
                )
                await self.metrics.record_run(
                    pipeline_name=self.definition.name,
                    pipeline_hash=pipeline_hash,
                    started_at=started_at,
                    finished_at=finished_at,
                    total_rows=len(context.data),
                    total_duration_ms=context.elapsed_ms(),
                    status=status,
                    error_message=error_message,
                    steps=[
                        {
                            "step_type": r.step_type,
                            "row_count": r.row_count,
                            "duration_ms": r.duration_ms,
                            "errors": r.errors,
                        }
                        for r in context.results
                    ],
                    memory_peak_mb=context.memory_peak_mb,
                    peak_buffer_rows=context.peak_buffer_rows,
                )
            except Exception:
                pass  # Don't fail pipeline if metrics recording fails
            finally:
                try:
                    await self.metrics.close()
                except Exception:
                    pass

            # KANBAN: Update state
            if self.kanban_id:
                state = "done" if status == "completed" else "failed"
                self.kanban.update_state(
                    self.kanban_id,
                    state,
                    error=error_message,
                    summary=context.summary(),
                )

            # Clear checkpoint on success
            if status == "completed" and self.checkpoint:
                self.checkpoint.clear()

        return context

    async def _run_dag(self, context: Context):
        """v0.4.0: DAG execution path — compile, optimize, execute."""
        planner = Planner()

        # Compile to DAG
        if self.definition.graph:
            dag = planner.compile_graph(self.definition.graph, self.definition.vars)
        else:
            step_list = [
                (s.step_type, s.config) for s in self.definition.steps
            ]
            dag = planner.compile_linear(step_list)

        # Optimize
        dag = planner.optimize(dag)

        if self.verbose:
            # Show optimization results
            fused_count = sum(
                1 for n in dag.nodes.values() if n.step_type == "_fused"
            )
            parallel_count = sum(
                1 for g in dag.parallel_groups() if len(g) > 1
            )
            if fused_count or parallel_count:
                parts = []
                if fused_count:
                    parts.append(f"{fused_count} fused")
                if parallel_count:
                    parts.append(f"{parallel_count} parallel groups")
                print(f"  [optimizer: {', '.join(parts)}]")

        # Execute
        executor = DagExecutor(dag, verbose=self.verbose)
        await executor.execute(context)

    async def _run_sequential(self, context: Context, start_step: int = 0):
        """Standard step-by-step execution."""
        for i, step_def in enumerate(self.definition.steps):
            if i < start_step:
                continue

            step_name = step_def.step_type
            if self.verbose:
                print(
                    f"  [{i + 1}/{len(self.definition.steps)}] {step_name}...",
                    end=" ",
                    flush=True,
                )

            step_class = StepRegistry.get(step_name)
            step = step_class(step_def.config, context)

            strategy = self.optimizer.decide(step_name, step_def.config, context)

            start = time.time()
            errors = []

            try:
                if strategy == "streaming" and step.supports_streaming():
                    result = []
                    async for row in step.execute_stream():
                        result.append(row)
                elif strategy == "async":
                    result = await step.execute_async()
                elif strategy == "multiprocess":
                    result = await step.execute_pooled()
                else:
                    result = await step.execute()
            except Exception as e:
                # Save checkpoint on failure
                if self.checkpoint and self.definition.checkpoint:
                    self.checkpoint.save(
                        step_index=i - 1 if i > 0 else 0,
                        data=context.data,
                        vars=context.vars,
                        results=[
                            {
                                "step_index": r.step_index,
                                "step_type": r.step_type,
                                "row_count": r.row_count,
                                "duration_ms": r.duration_ms,
                                "errors": r.errors,
                            }
                            for r in context.results
                        ],
                    )

                if self.definition.on_error == "stop":
                    raise StepError(step_name, str(e)) from e
                elif self.definition.on_error == "skip":
                    errors.append(str(e))
                    result = context.data
                else:
                    raise StepError(step_name, str(e)) from e

            duration_ms = (time.time() - start) * 1000

            # JIT: Check if data changed (skip downstream if unchanged)
            jit_skipped = False
            if self.definition.jit and step_name not in ("guard", "load"):
                current_hash = self.change_detector.compute_hash(result)
                if not self.change_detector.has_changed(
                    self.definition.name, i, current_hash
                ):
                    jit_skipped = True
                    context.jit_steps_skipped += 1
                    if self.verbose:
                        print(f"(JIT: unchanged, skipping) ", end="")
                else:
                    self.change_detector.save_hash(
                        self.definition.name, i, current_hash
                    )

            # MUDA: Warn on dead steps (no data produced)
            if (
                not result
                and step_name not in ("guard", "load")
                and self.verbose
            ):
                print(f"(MUDA: no data) ", end="")

            context.set_data(result)
            context.log_step(i, step_name, len(result), duration_ms, errors)

            # Save checkpoint after each successful step
            if self.checkpoint and self.definition.checkpoint:
                self.checkpoint.save(
                    step_index=i,
                    data=context.data,
                    vars=context.vars,
                    results=[
                        {
                            "step_index": r.step_index,
                            "step_type": r.step_type,
                            "row_count": r.row_count,
                            "duration_ms": r.duration_ms,
                            "errors": r.errors,
                        }
                        for r in context.results
                    ],
                )

            if self.verbose:
                print(f"{len(result)} rows in {duration_ms:.0f}ms")

    async def _run_streaming(self, context: Context):
        """Streaming pipeline execution — data flows step-by-step without full materialization.

        Each step processes rows as they arrive from the previous step.
        Falls back to sequential for steps that don't support streaming.
        """
        if self.verbose:
            print("  [STREAMING MODE]")

        # Execute first step normally to seed the pipeline
        first_step_def = self.definition.steps[0]
        step_class = StepRegistry.get(first_step_def.step_type)
        step = step_class(first_step_def.config, context)

        start = time.time()
        result = await step.execute_async()
        duration_ms = (time.time() - start) * 1000
        context.set_data(result)
        context.log_step(0, first_step_def.step_type, len(result), duration_ms, [])

        if self.verbose:
            print(
                f"  [1/{len(self.definition.steps)}] "
                f"{first_step_def.step_type}... {len(result)} rows in {duration_ms:.0f}ms"
            )

        # Stream remaining steps
        for i, step_def in enumerate(self.definition.steps[1:], start=1):
            step_name = step_def.step_type
            if self.verbose:
                print(
                    f"  [{i + 1}/{len(self.definition.steps)}] {step_name} (streaming)...",
                    end=" ",
                    flush=True,
                )

            step_class = StepRegistry.get(step_name)
            step = step_class(step_def.config, context)

            start = time.time()

            if step.supports_streaming():
                collected = []
                async for row in step.execute_stream():
                    collected.append(row)
                result = collected
            else:
                result = await step.execute()

            duration_ms = (time.time() - start) * 1000
            context.set_data(result)
            context.log_step(i, step_name, len(result), duration_ms, [])

            if self.verbose:
                print(f"{len(result)} rows in {duration_ms:.0f}ms")
