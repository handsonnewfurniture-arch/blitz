from __future__ import annotations
from typing import TYPE_CHECKING

from blitztigerclaw.steps import StepRegistry, discover as _discover_steps

if TYPE_CHECKING:
    from blitztigerclaw.context import Context


class Optimizer:
    """Decides the best execution strategy for each step based on type and data size.

    v0.2.0: Added 'streaming' strategy for steps that support it.
    v0.5.0: Reads strategy from StepMeta — no hardcoded step names.
    """

    def decide(self, step_type: str, config: dict, context: "Context") -> str:
        """Returns: 'streaming', 'async', 'sync', 'multiprocess', or 'batched'."""
        _discover_steps()

        try:
            meta = StepRegistry.get_meta(step_type)
        except ValueError:
            return "sync"

        # Check escalations (thresholds in ascending order)
        row_count = len(context.data)
        chosen = meta.default_strategy
        for threshold, strategy in meta.strategy_escalations:
            if row_count > threshold:
                # streaming_breakers can suppress a streaming escalation
                if strategy == "streaming" and meta.streaming_breakers:
                    if any(k in config for k in meta.streaming_breakers):
                        continue
                chosen = strategy

        return chosen

    def should_stream_pipeline(self, step_configs: list[tuple[str, dict]]) -> bool:
        """Decide if the entire pipeline should use streaming execution.

        Returns True if all steps support streaming and data is expected to be large.
        """
        _discover_steps()

        for step_type, config in step_configs:
            try:
                meta = StepRegistry.get_meta(step_type)
            except ValueError:
                return False

            if meta.streaming == "no":
                return False
            # conditional streaming respects breakers
            if meta.streaming == "conditional" and meta.streaming_breakers:
                if any(k in config for k in meta.streaming_breakers):
                    return False

        return len(step_configs) >= 2
