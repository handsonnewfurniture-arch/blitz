"""JIT: Just-In-Time change detection via content hashing.

Tracks data hashes per pipeline+step in ~/.blitz/hashes.json.
Enables skipping steps when data hasn't changed since last run.
Also provides pipeline-level deduplication (MUDA).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

HASH_FILE = Path.home() / ".blitz" / "hashes.json"


class ChangeDetector:
    """Track data hashes to enable JIT skip-unchanged processing."""

    def __init__(self, path: str | Path = HASH_FILE):
        self.path = Path(path)

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {}

    def _save(self, data: dict):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2))

    def compute_hash(self, data: list[dict[str, Any]]) -> str:
        """SHA-256 of canonical JSON representation of data."""
        canonical = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def has_changed(
        self, pipeline_name: str, step_index: int, current_hash: str
    ) -> bool:
        """Compare current hash against stored hash for this pipeline+step.

        Returns True if data has changed (or no previous hash exists).
        """
        store = self._load()
        key = f"{pipeline_name}:step_{step_index}"
        previous = store.get(key)
        return previous != current_hash

    def save_hash(self, pipeline_name: str, step_index: int, hash_val: str):
        """Store the hash after a successful step execution."""
        store = self._load()
        key = f"{pipeline_name}:step_{step_index}"
        store[key] = hash_val
        self._save(store)

    def get_pipeline_hash(self, pipeline_yaml_str: str) -> str:
        """Hash the entire pipeline definition for deduplication (MUDA)."""
        return hashlib.sha256(pipeline_yaml_str.encode()).hexdigest()[:16]

    def clear(self, pipeline_name: str | None = None):
        """Clear stored hashes. If pipeline_name given, clear only that pipeline."""
        if pipeline_name is None:
            self._save({})
            return

        store = self._load()
        prefix = f"{pipeline_name}:"
        store = {k: v for k, v in store.items() if not k.startswith(prefix)}
        self._save(store)
