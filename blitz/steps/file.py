from __future__ import annotations

import csv
import glob
import json
import os
from typing import Any

from blitz.steps import BaseStep, StepRegistry


@StepRegistry.register("file")
class FileStep(BaseStep):
    """Read files as pipeline input or write pipeline data to files."""

    async def execute(self) -> list[dict[str, Any]]:
        action = self.config.get("action", "read")

        if action == "read":
            return self._read()
        elif action == "glob":
            return self._glob()
        elif action == "write":
            return self._write()
        else:
            raise ValueError(f"Unknown file action: {action}")

    def _read(self) -> list[dict]:
        path = self.config.get("path", "")
        fmt = self.config.get("format", "auto")

        if fmt == "auto":
            if path.endswith(".json"):
                fmt = "json"
            elif path.endswith(".csv"):
                fmt = "csv"
            else:
                fmt = "text"

        if fmt == "json":
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, list):
                return data
            return [data]

        elif fmt == "csv":
            with open(path, "r") as f:
                reader = csv.DictReader(f)
                return list(reader)

        elif fmt == "text":
            with open(path, "r") as f:
                lines = f.read().strip().splitlines()
            return [{"line": line, "_index": i} for i, line in enumerate(lines)]

        return []

    def _glob(self) -> list[dict]:
        pattern = self.config.get("path", "*")
        return [
            {
                "path": p,
                "name": os.path.basename(p),
                "size": os.path.getsize(p),
                "ext": os.path.splitext(p)[1],
            }
            for p in sorted(glob.glob(pattern, recursive=True))
            if os.path.isfile(p)
        ]

    def _write(self) -> list[dict]:
        data = self.context.data
        path = self.config.get("path", "output.json")
        fmt = self.config.get("format", "auto")

        if fmt == "auto":
            if path.endswith(".csv"):
                fmt = "csv"
            else:
                fmt = "json"

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        if fmt == "json":
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
        elif fmt == "csv" and data:
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
                writer.writeheader()
                writer.writerows(data)

        return data
