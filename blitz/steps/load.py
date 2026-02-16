from __future__ import annotations

import csv
import json
import os
from typing import Any, AsyncIterator

from blitz.steps import BaseStep, StepRegistry
from blitz.stream import BatchBuffer


@StepRegistry.register("load")
class LoadStep(BaseStep):
    """Load data into SQLite, CSV, JSON, or stdout.

    v0.2.0: SQLite WAL mode + PRAGMAs for 2-5x write performance,
    streaming batch inserts via execute_stream.
    """

    async def execute(self) -> list[dict[str, Any]]:
        target = self.config.get("target", "stdout")
        data = self.context.data

        if not data:
            return data

        if target.startswith("sqlite:"):
            await self._load_sqlite(target, data)
        elif target.startswith("csv://") or target.endswith(".csv"):
            self._load_csv(target, data)
        elif target.startswith("json://") or target.endswith(".json"):
            self._load_json(target, data)
        elif target == "stdout":
            self._load_stdout(data)
        else:
            self._load_stdout(data)

        return data

    async def execute_stream(self) -> AsyncIterator[dict[str, Any]]:
        """Streaming load: batch-inserts rows as they arrive."""
        target = self.config.get("target", "stdout")
        data = self.context.data

        if not data:
            return

        if target.startswith("sqlite:"):
            await self._load_sqlite_streaming(target, data)
        else:
            await self.execute()

        for item in data:
            yield item

    def supports_streaming(self) -> bool:
        target = self.config.get("target", "stdout")
        return target.startswith("sqlite:")

    async def _load_sqlite(self, target: str, data: list[dict]):
        import aiosqlite

        db_path = target.replace("sqlite:///", "").replace("sqlite://", "")
        table = self.config.get("table", "data")
        mode = self.config.get("mode", "insert")
        key = self.config.get("key", None)
        batch_size = self.config.get("batch_size", 1000)

        columns = list(data[0].keys())

        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        async with aiosqlite.connect(db_path) as db:
            # v0.2.0: Performance PRAGMAs
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute("PRAGMA cache_size=-8000")
            await db.execute("PRAGMA temp_store=MEMORY")
            await db.execute("PRAGMA mmap_size=268435456")  # 256MB mmap

            # Auto-create table
            col_defs = ", ".join(
                f'"{c}" TEXT' if c != key else f'"{c}" TEXT PRIMARY KEY'
                for c in columns
            )
            await db.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join("?" for _ in columns)

            if mode == "upsert" and key:
                update_cols = ", ".join(
                    f'"{c}" = excluded."{c}"' for c in columns if c != key
                )
                sql = (
                    f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) '
                    f'ON CONFLICT("{key}") DO UPDATE SET {update_cols}'
                )
            elif mode == "replace":
                sql = f'INSERT OR REPLACE INTO "{table}" ({col_list}) VALUES ({placeholders})'
            else:
                sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'

            # Batched executemany for performance
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                values = [
                    tuple(str(row.get(c, "")) if row.get(c) is not None else None
                          for c in columns)
                    for row in batch
                ]
                await db.executemany(sql, values)

            await db.commit()

    async def _load_sqlite_streaming(self, target: str, data: list[dict]):
        """Streaming SQLite inserts using BatchBuffer."""
        import aiosqlite

        db_path = target.replace("sqlite:///", "").replace("sqlite://", "")
        table = self.config.get("table", "data")
        mode = self.config.get("mode", "insert")
        key = self.config.get("key", None)
        batch_size = self.config.get("batch_size", 1000)

        if not data:
            return

        columns = list(data[0].keys())

        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        async with aiosqlite.connect(db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute("PRAGMA cache_size=-8000")
            await db.execute("PRAGMA temp_store=MEMORY")
            await db.execute("PRAGMA mmap_size=268435456")

            col_defs = ", ".join(
                f'"{c}" TEXT' if c != key else f'"{c}" TEXT PRIMARY KEY'
                for c in columns
            )
            await db.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')

            col_list = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join("?" for _ in columns)

            if mode == "upsert" and key:
                update_cols = ", ".join(
                    f'"{c}" = excluded."{c}"' for c in columns if c != key
                )
                sql = (
                    f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders}) '
                    f'ON CONFLICT("{key}") DO UPDATE SET {update_cols}'
                )
            elif mode == "replace":
                sql = f'INSERT OR REPLACE INTO "{table}" ({col_list}) VALUES ({placeholders})'
            else:
                sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'

            buffer = BatchBuffer(size=batch_size)

            for row in data:
                buffer.add(row)
                if buffer.full:
                    batch = buffer.flush()
                    values = [
                        tuple(
                            str(r.get(c, "")) if r.get(c) is not None else None
                            for c in columns
                        )
                        for r in batch
                    ]
                    await db.executemany(sql, values)

            # Flush remaining
            if buffer.count > 0:
                batch = buffer.flush()
                values = [
                    tuple(
                        str(r.get(c, "")) if r.get(c) is not None else None
                        for c in columns
                    )
                    for r in batch
                ]
                await db.executemany(sql, values)

            await db.commit()

    def _load_csv(self, target: str, data: list[dict]):
        path = target.replace("csv://", "")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        columns = list(data[0].keys())
        mode = "a" if self.config.get("mode") == "append" else "w"

        with open(path, mode, newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            if mode == "w" or os.path.getsize(path) == 0:
                writer.writeheader()
            writer.writerows(data)

    def _load_json(self, target: str, data: list[dict]):
        path = target.replace("json://", "")
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)

    def _load_stdout(self, data: list[dict]):
        limit = self.config.get("preview", 20)
        shown = data[:limit]

        if not shown:
            print("  (no data)")
            return

        columns = list(shown[0].keys())

        # Calculate column widths
        widths = {c: len(c) for c in columns}
        for row in shown:
            for c in columns:
                val = str(row.get(c, ""))
                widths[c] = min(max(widths[c], len(val)), 40)

        # Print header
        header = " | ".join(c.ljust(widths[c])[:widths[c]] for c in columns)
        print(f"  {header}")
        print(f"  {'-' * len(header)}")

        # Print rows
        for row in shown:
            line = " | ".join(
                str(row.get(c, "")).ljust(widths[c])[:widths[c]]
                for c in columns
            )
            print(f"  {line}")

        if len(data) > limit:
            print(f"  ... and {len(data) - limit} more rows")
