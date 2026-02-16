"""KAIZEN: Continuous improvement metrics store.

Tracks every pipeline run in ~/.blitz/metrics.db (SQLite).
Provides performance trends, bottleneck detection, and historical averages.

v0.2.0: Connection reuse (single connection per MetricsStore instance),
        SQLite PRAGMAs for write performance, memory tracking.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import aiosqlite

METRICS_DB = Path.home() / ".blitz" / "metrics.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_name TEXT NOT NULL,
    pipeline_hash TEXT,
    started_at REAL NOT NULL,
    finished_at REAL NOT NULL,
    total_rows INTEGER NOT NULL,
    total_duration_ms REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'completed',
    error_message TEXT,
    steps_json TEXT NOT NULL,
    memory_peak_mb REAL DEFAULT 0,
    peak_buffer_rows INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_pipeline_name ON pipeline_runs(pipeline_name);
CREATE INDEX IF NOT EXISTS idx_started_at ON pipeline_runs(started_at);
"""


class MetricsStore:
    """Async SQLite store for pipeline run metrics.

    v0.2.0: Reuses a single connection for the lifetime of the store,
    avoiding repeated open/close overhead.
    """

    def __init__(self, db_path: str | Path = METRICS_DB):
        self.db_path = Path(db_path)
        self._conn: aiosqlite.Connection | None = None

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(str(self.db_path))
            # Performance PRAGMAs
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
            await self._conn.execute("PRAGMA cache_size=-8000")  # 8MB
            await self._conn.execute("PRAGMA temp_store=MEMORY")
            await self._conn.executescript(SCHEMA)
        return self._conn

    async def close(self):
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def record_run(
        self,
        pipeline_name: str,
        pipeline_hash: str,
        started_at: float,
        finished_at: float,
        total_rows: int,
        total_duration_ms: float,
        status: str,
        error_message: str | None,
        steps: list[dict],
        memory_peak_mb: float = 0,
        peak_buffer_rows: int = 0,
    ):
        db = await self._get_conn()
        await db.execute(
            """INSERT INTO pipeline_runs
               (pipeline_name, pipeline_hash, started_at, finished_at,
                total_rows, total_duration_ms, status, error_message, steps_json,
                memory_peak_mb, peak_buffer_rows)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pipeline_name,
                pipeline_hash,
                started_at,
                finished_at,
                total_rows,
                total_duration_ms,
                status,
                error_message,
                json.dumps(steps, default=str),
                memory_peak_mb,
                peak_buffer_rows,
            ),
        )
        await db.commit()

    async def get_history(self, pipeline_name: str, limit: int = 20) -> list[dict]:
        """Get recent runs for a pipeline."""
        db = await self._get_conn()
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT id, pipeline_name, started_at, finished_at,
                      total_rows, total_duration_ms, status, error_message, steps_json,
                      memory_peak_mb, peak_buffer_rows
               FROM pipeline_runs
               WHERE pipeline_name = ?
               ORDER BY started_at DESC
               LIMIT ?""",
            (pipeline_name, limit),
        )
        rows = await cursor.fetchall()
        return [
            {
                "id": r["id"],
                "pipeline_name": r["pipeline_name"],
                "started_at": r["started_at"],
                "total_rows": r["total_rows"],
                "total_duration_ms": round(r["total_duration_ms"], 1),
                "status": r["status"],
                "error_message": r["error_message"],
                "steps": json.loads(r["steps_json"]),
                "memory_peak_mb": r["memory_peak_mb"],
                "peak_buffer_rows": r["peak_buffer_rows"],
            }
            for r in rows
        ]

    async def get_step_averages(
        self, pipeline_name: str, window: int = 10
    ) -> dict[str, dict]:
        """Get average row count and duration per step type over recent runs.

        Used by JIDOKA Andon for anomaly detection.
        Returns: {step_type: {avg_rows, avg_ms, run_count}}
        """
        db = await self._get_conn()
        cursor = await db.execute(
            """SELECT steps_json FROM pipeline_runs
               WHERE pipeline_name = ? AND status = 'completed'
               ORDER BY started_at DESC LIMIT ?""",
            (pipeline_name, window),
        )
        rows = await cursor.fetchall()

        if not rows:
            return {}

        # Aggregate step metrics
        step_totals: dict[str, dict] = {}
        for (steps_json,) in rows:
            steps = json.loads(steps_json)
            for step in steps:
                st = step.get("step_type", "unknown")
                if st not in step_totals:
                    step_totals[st] = {"total_rows": 0, "total_ms": 0.0, "count": 0}
                step_totals[st]["total_rows"] += step.get("row_count", 0)
                step_totals[st]["total_ms"] += step.get("duration_ms", 0)
                step_totals[st]["count"] += 1

        return {
            st: {
                "avg_rows": round(v["total_rows"] / v["count"]),
                "avg_ms": round(v["total_ms"] / v["count"], 1),
                "run_count": v["count"],
            }
            for st, v in step_totals.items()
        }

    async def detect_bottlenecks(self, pipeline_name: str) -> list[dict]:
        """Identify slowest steps and suggest optimizations.

        Returns: [{step_type, avg_ms, pct_of_total, suggestion}]
        """
        averages = await self.get_step_averages(pipeline_name)
        if not averages:
            return []

        total_avg_ms = sum(v["avg_ms"] for v in averages.values())
        if total_avg_ms == 0:
            return []

        bottlenecks = []
        for step_type, stats in averages.items():
            pct = (stats["avg_ms"] / total_avg_ms) * 100
            suggestion = None

            if pct > 60:
                if step_type == "fetch":
                    suggestion = "Increase 'parallel' or add caching"
                elif step_type == "transform":
                    suggestion = "Reduce data before this step with filter/limit"
                elif step_type == "load":
                    suggestion = "Increase 'batch_size' or use async target"
                elif step_type == "scrape":
                    suggestion = "Increase 'parallel' or narrow selectors"
                else:
                    suggestion = "This step dominates execution time"

            bottlenecks.append(
                {
                    "step_type": step_type,
                    "avg_ms": stats["avg_ms"],
                    "pct_of_total": round(pct, 1),
                    "suggestion": suggestion,
                }
            )

        bottlenecks.sort(key=lambda x: x["avg_ms"], reverse=True)
        return bottlenecks

    async def get_all_metrics_summary(self) -> list[dict]:
        """Summary across all pipelines for `blitz metrics`."""
        db = await self._get_conn()
        cursor = await db.execute(
            """SELECT
                   pipeline_name,
                   COUNT(*) as run_count,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as success_count,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as fail_count,
                   AVG(total_duration_ms) as avg_duration_ms,
                   AVG(total_rows) as avg_rows,
                   MAX(started_at) as last_run,
                   AVG(memory_peak_mb) as avg_memory_mb
               FROM pipeline_runs
               GROUP BY pipeline_name
               ORDER BY last_run DESC"""
        )
        rows = await cursor.fetchall()

        return [
            {
                "pipeline": r[0],
                "runs": r[1],
                "success": r[2],
                "failed": r[3],
                "avg_ms": round(r[4], 1),
                "avg_rows": round(r[5]),
                "last_run": r[6],
                "avg_memory_mb": round(r[7] or 0, 1),
            }
            for r in rows
        ]
