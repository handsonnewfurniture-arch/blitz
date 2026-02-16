from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from blitz.context import Context


class Optimizer:
    """Decides the best execution strategy for each step based on type and data size."""

    def decide(self, step_type: str, config: dict, context: "Context") -> str:
        """Returns: 'async', 'sync', 'multiprocess', or 'batched'."""

        if step_type in ("fetch", "scrape"):
            return "async"

        if step_type == "transform":
            if len(context.data) > 50_000:
                return "multiprocess"
            return "sync"

        if step_type == "load":
            if len(context.data) > 10_000:
                return "batched"
            return "sync"

        return "sync"
