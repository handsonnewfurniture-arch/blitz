from dataclasses import dataclass, field
from typing import Any
import sys
import time


@dataclass
class StepResult:
    step_index: int
    step_type: str
    row_count: int
    duration_ms: float
    errors: list[str] = field(default_factory=list)


@dataclass
class Context:
    """Shared runtime context that flows data between pipeline steps.

    v0.2.0: Added memory tracking (memory_mb, peak_buffer_rows).
    """

    data: list[dict[str, Any]] = field(default_factory=list)
    vars: dict[str, Any] = field(default_factory=dict)
    results: list[StepResult] = field(default_factory=list)
    _start_time: float = field(default_factory=time.time)
    pipeline_name: str = ""
    jit_steps_skipped: int = 0
    # v0.2.0: Memory tracking
    memory_peak_mb: float = 0.0
    peak_buffer_rows: int = 0
    streaming_mode: bool = False

    def set_data(self, data: list[dict[str, Any]]):
        self.data = data
        self._track_memory()

    def elapsed_ms(self) -> float:
        return (time.time() - self._start_time) * 1000

    def log_step(self, index: int, step_type: str, row_count: int,
                 duration_ms: float, errors: list[str] | None = None):
        self.results.append(StepResult(
            step_index=index,
            step_type=step_type,
            row_count=row_count,
            duration_ms=duration_ms,
            errors=errors or [],
        ))

    def _track_memory(self):
        """Track peak memory usage of the data list."""
        current_mb = sys.getsizeof(self.data) / (1024 * 1024)
        # Rough estimate: each row ~200 bytes average
        if self.data:
            sample_size = min(100, len(self.data))
            avg_row_size = sum(
                sys.getsizeof(row) for row in self.data[:sample_size]
            ) / sample_size
            current_mb = (avg_row_size * len(self.data)) / (1024 * 1024)
        if current_mb > self.memory_peak_mb:
            self.memory_peak_mb = current_mb
        if len(self.data) > self.peak_buffer_rows:
            self.peak_buffer_rows = len(self.data)

    def summary(self) -> dict:
        result = {
            "total_rows": len(self.data),
            "steps_completed": len(self.results),
            "total_duration_ms": self.elapsed_ms(),
            "memory_peak_mb": round(self.memory_peak_mb, 2),
            "peak_buffer_rows": self.peak_buffer_rows,
            "streaming_mode": self.streaming_mode,
            "steps": [
                {
                    "type": r.step_type,
                    "rows": r.row_count,
                    "ms": round(r.duration_ms, 1),
                    "errors": len(r.errors),
                }
                for r in self.results
            ],
        }
        if self.jit_steps_skipped > 0:
            result["jit_steps_skipped"] = self.jit_steps_skipped
        return result
