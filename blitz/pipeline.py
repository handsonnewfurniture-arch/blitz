from __future__ import annotations

import time
from typing import TYPE_CHECKING

from blitz.context import Context
from blitz.optimizer import Optimizer
from blitz.exceptions import StepError

if TYPE_CHECKING:
    from blitz.parser import PipelineDefinition

# Import step modules to trigger registration
import blitz.steps.fetch
import blitz.steps.transform
import blitz.steps.load
import blitz.steps.scrape
import blitz.steps.shell
import blitz.steps.file
import blitz.steps.guard
import blitz.steps.railway
import blitz.steps.netlify
import blitz.steps.github

from blitz.steps import StepRegistry
from blitz.tps.metrics import MetricsStore
from blitz.tps.kanban import KanbanBoard
from blitz.tps.change_detector import ChangeDetector


class Pipeline:
    """Executes a parsed pipeline definition step by step.

    Integrates Toyota Production System principles:
    - KAIZEN: Records metrics for every run
    - KANBAN: Tracks pipeline state on board
    - JIT: Skips steps when data unchanged
    - MUDA: Warns on dead/wasteful steps
    """

    def __init__(
        self,
        definition: "PipelineDefinition",
        verbose: bool = False,
        kanban_id: str | None = None,
    ):
        self.definition = definition
        self.verbose = verbose
        self.optimizer = Optimizer()
        self.metrics = MetricsStore()
        self.change_detector = ChangeDetector()
        self.kanban_id = kanban_id
        self.kanban = KanbanBoard()

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

        try:
            for i, step_def in enumerate(self.definition.steps):
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
                    if strategy == "async":
                        result = await step.execute_async()
                    elif strategy == "multiprocess":
                        result = await step.execute_pooled()
                    else:
                        result = await step.execute()
                except Exception as e:
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

                if self.verbose:
                    print(f"{len(result)} rows in {duration_ms:.0f}ms")

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
                )
            except Exception:
                pass  # Don't fail pipeline if metrics recording fails

            # KANBAN: Update state
            if self.kanban_id:
                state = "done" if status == "completed" else "failed"
                self.kanban.update_state(
                    self.kanban_id,
                    state,
                    error=error_message,
                    summary=context.summary(),
                )

        return context
