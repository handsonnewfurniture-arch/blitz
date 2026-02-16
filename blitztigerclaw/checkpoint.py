"""Checkpoint manager for BlitzTigerClaw v0.2.0.

Saves pipeline state after each step so that failed pipelines can
resume from the last successful step instead of restarting.

State is persisted in ~/.blitztigerclaw/checkpoints/ as JSON files.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

CHECKPOINT_DIR = Path.home() / ".blitztigerclaw" / "checkpoints"


class CheckpointManager:
    """Save and restore pipeline execution state."""

    def __init__(self, pipeline_name: str, checkpoint_dir: Path = CHECKPOINT_DIR):
        self._dir = checkpoint_dir / _safe_name(pipeline_name)
        self._pipeline_name = pipeline_name

    def save(
        self,
        step_index: int,
        data: list[dict[str, Any]],
        vars: dict[str, Any],
        results: list[dict],
    ):
        """Save checkpoint after successful step completion."""
        self._dir.mkdir(parents=True, exist_ok=True)

        state = {
            "pipeline_name": self._pipeline_name,
            "completed_step": step_index,
            "timestamp": time.time(),
            "data_count": len(data),
            "vars": _serialize_vars(vars),
            "results": results,
        }

        # Write metadata
        meta_path = self._dir / "checkpoint.json"
        meta_path.write_text(json.dumps(state, indent=2, default=str))

        # Write data separately (can be large)
        data_path = self._dir / "data.json"
        data_path.write_text(json.dumps(data, default=str))

    def load(self) -> dict | None:
        """Load the most recent checkpoint. Returns None if no checkpoint exists."""
        meta_path = self._dir / "checkpoint.json"
        data_path = self._dir / "data.json"

        if not meta_path.exists() or not data_path.exists():
            return None

        meta = json.loads(meta_path.read_text())
        data = json.loads(data_path.read_text())

        return {
            "completed_step": meta["completed_step"],
            "timestamp": meta["timestamp"],
            "data": data,
            "vars": meta.get("vars", {}),
            "results": meta.get("results", []),
        }

    def clear(self):
        """Remove checkpoint files for this pipeline."""
        if self._dir.exists():
            for f in self._dir.iterdir():
                f.unlink()
            self._dir.rmdir()

    @property
    def exists(self) -> bool:
        return (self._dir / "checkpoint.json").exists()

    @property
    def info(self) -> dict | None:
        """Get checkpoint metadata without loading data."""
        meta_path = self._dir / "checkpoint.json"
        if not meta_path.exists():
            return None
        return json.loads(meta_path.read_text())


def _safe_name(name: str) -> str:
    """Convert pipeline name to filesystem-safe directory name."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)


def _serialize_vars(vars: dict) -> dict:
    """Serialize vars dict, converting non-JSON-safe values to strings."""
    result = {}
    for k, v in vars.items():
        if k.startswith("_"):
            continue  # Skip internal vars
        try:
            json.dumps(v)
            result[k] = v
        except (TypeError, ValueError):
            result[k] = str(v)
    return result
