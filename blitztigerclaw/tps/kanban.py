"""KANBAN: Visual workflow board and pull-based pipeline queue.

Manages pipeline state in ~/.blitztigerclaw/kanban.json.
States: backlog -> in_progress -> done | failed
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Literal

KANBAN_FILE = Path.home() / ".blitztigerclaw" / "kanban.json"

State = Literal["backlog", "in_progress", "done", "failed"]


class KanbanBoard:
    """JSON-backed Kanban board for pipeline tracking."""

    def __init__(self, path: str | Path = KANBAN_FILE):
        self.path = Path(path)

    def _load(self) -> dict:
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {"items": []}

    def _save(self, data: dict):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, indent=2, default=str))

    def add(
        self,
        pipeline_file: str,
        pipeline_name: str,
        variables: dict[str, Any] | None = None,
    ) -> str:
        """Add a pipeline to backlog. Returns item ID."""
        data = self._load()
        item_id = uuid.uuid4().hex[:8]
        data["items"].append(
            {
                "id": item_id,
                "pipeline_file": pipeline_file,
                "pipeline_name": pipeline_name,
                "variables": variables or {},
                "state": "backlog",
                "created_at": time.time(),
                "updated_at": time.time(),
                "error": None,
                "summary": None,
            }
        )
        self._save(data)
        return item_id

    def pull_next(self) -> dict | None:
        """Pull the oldest backlog item into in_progress. Returns the item or None."""
        data = self._load()
        for item in data["items"]:
            if item["state"] == "backlog":
                item["state"] = "in_progress"
                item["updated_at"] = time.time()
                self._save(data)
                return item
        return None

    def update_state(
        self,
        item_id: str,
        state: State,
        error: str | None = None,
        summary: dict | None = None,
    ):
        """Move an item to a new state."""
        data = self._load()
        for item in data["items"]:
            if item["id"] == item_id:
                item["state"] = state
                item["updated_at"] = time.time()
                if error:
                    item["error"] = error
                if summary:
                    item["summary"] = summary
                break
        self._save(data)

    def get_board(self) -> dict[str, list[dict]]:
        """Return items grouped by state for display."""
        data = self._load()
        board: dict[str, list[dict]] = {
            "backlog": [],
            "in_progress": [],
            "done": [],
            "failed": [],
        }
        for item in data["items"]:
            state = item.get("state", "backlog")
            if state in board:
                board[state].append(item)
        return board

    def clear_done(self, older_than_hours: int = 24):
        """Remove completed items older than threshold (MUDA: waste elimination)."""
        data = self._load()
        cutoff = time.time() - (older_than_hours * 3600)
        data["items"] = [
            item
            for item in data["items"]
            if not (
                item["state"] in ("done", "failed")
                and item.get("updated_at", 0) < cutoff
            )
        ]
        self._save(data)

    def get_item(self, item_id: str) -> dict | None:
        """Get a specific item by ID."""
        data = self._load()
        for item in data["items"]:
            if item["id"] == item_id:
                return item
        return None
