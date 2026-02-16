from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from blitz.context import Context


class Optimizer:
    """Decides the best execution strategy for each step based on type and data size.

    v0.2.0: Added 'streaming' strategy for steps that support it.
    """

    def decide(self, step_type: str, config: dict, context: "Context") -> str:
        """Returns: 'streaming', 'async', 'sync', 'multiprocess', or 'batched'."""

        if step_type in ("fetch", "scrape"):
            return "async"

        if step_type == "transform":
            # Streaming for row-level ops on large datasets
            if len(context.data) > 5_000:
                needs_collect = any(
                    k in config for k in ("sort", "dedupe", "limit")
                )
                if not needs_collect:
                    return "streaming"
            if len(context.data) > 50_000:
                return "multiprocess"
            return "sync"

        if step_type == "load":
            if len(context.data) > 10_000:
                return "batched"
            return "sync"

        if step_type == "parallel":
            return "async"

        return "sync"

    def should_stream_pipeline(self, step_configs: list[tuple[str, dict]]) -> bool:
        """Decide if the entire pipeline should use streaming execution.

        Returns True if all steps support streaming and data is expected to be large.
        """
        streamable_types = {"fetch", "transform", "load"}
        for step_type, config in step_configs:
            if step_type not in streamable_types:
                return False
            # Transform with sort/dedupe/limit breaks streaming
            if step_type == "transform" and any(
                k in config for k in ("sort", "dedupe", "limit")
            ):
                return False
        return len(step_configs) >= 2
