"""Dataset merge/join step — SQL-style JOIN on shared keys.

Supports inner, left, and outer joins with SQLite, CSV, JSON, and
context-based right-side data sources.
"""

from __future__ import annotations

import csv
import json
import os
from typing import Any

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry


@StepRegistry.register("join")
class JoinStep(BaseStep):
    """Combine two datasets on a shared key.

    YAML usage:
    - join:
        right: sqlite:///customers.db
        right_table: customers
        on: customer_id
        how: left
        select_right: [name, email]
        prefix_right: "customer_"

    Or with separate keys:
    - join:
        right: json:///data.json
        left_on: cust_id
        right_on: customer_id
        how: inner
    """

    meta = StepMeta(
        description="Dataset merge/join — SQL-style JOIN on shared keys",
        config_docs={
            "right": "string — right-side source (sqlite:///path, csv:///path, json:///path, context:step_N)",
            "right_table": "string — table name for SQLite source",
            "on": "string — join key (same name both sides)",
            "left_on": "string — left key (when names differ)",
            "right_on": "string — right key (when names differ)",
            "how": "string — inner | left | outer (default inner)",
            "select_right": "list[string] — only keep these right-side fields",
            "prefix_right": "string — prefix right-side fields to avoid collision",
        },
    )

    async def execute(self) -> list[dict[str, Any]]:
        left_data = self.context.data
        if not left_data:
            return []

        how = self.config.get("how", "inner")
        left_key = self.config.get("left_on") or self.config.get("on")
        right_key = self.config.get("right_on") or self.config.get("on")

        if not left_key or not right_key:
            raise ValueError("Join requires 'on' or 'left_on'/'right_on' keys")

        # Load right-side data
        right_data = self._load_right()

        # Filter right-side fields if select_right specified
        select_right = self.config.get("select_right")
        if select_right:
            # Always keep the join key
            keep = set(select_right)
            keep.add(right_key)
            right_data = [{k: r.get(k) for k in keep} for r in right_data]

        # Build hash index on right side: key -> list of rows
        right_index: dict[Any, list[dict]] = {}
        for row in right_data:
            k = row.get(right_key)
            if k is not None:
                right_index.setdefault(k, []).append(row)

        prefix = self.config.get("prefix_right", "")

        # Perform join
        if how == "inner":
            return self._inner_join(left_data, right_index, left_key, right_key, prefix)
        elif how == "left":
            return self._left_join(left_data, right_index, left_key, right_key, prefix)
        elif how == "outer":
            return self._outer_join(left_data, right_index, right_data, left_key, right_key, prefix)
        else:
            raise ValueError(f"Unknown join type: {how}. Use inner, left, or outer.")

    def _load_right(self) -> list[dict]:
        """Load right-side data from the configured source.

        v0.4.0: In DAG mode, check context.inputs for predecessor data first.
        """
        # DAG multi-input: use data from a predecessor node
        if self.context.inputs:
            for port_name, data in self.context.inputs.items():
                if port_name != "default":
                    return list(data)

        source = self.config.get("right", "")

        if source.startswith("sqlite:"):
            return self._load_sqlite(source)
        elif source.startswith("csv:"):
            return self._load_csv(source)
        elif source.startswith("json:"):
            return self._load_json(source)
        elif source.startswith("context:"):
            return self._load_context(source)
        else:
            raise ValueError(
                f"Unknown right-side source: {source!r}. "
                f"Use sqlite:///path, csv:///path, json:///path, or context:step_N"
            )

    def _load_sqlite(self, source: str) -> list[dict]:
        import sqlite3

        db_path = source.replace("sqlite:///", "").replace("sqlite://", "")
        table = self.config.get("right_table", "data")

        if not os.path.exists(db_path):
            raise FileNotFoundError(f"SQLite database not found: {db_path}")

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(f'SELECT * FROM "{table}"')
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def _load_csv(source: str) -> list[dict]:
        path = source.replace("csv:///", "").replace("csv://", "")
        with open(path, "r") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def _load_json(source: str) -> list[dict]:
        path = source.replace("json:///", "").replace("json://", "")
        with open(path, "r") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return [data]

    def _load_context(self, source: str) -> list[dict]:
        """Load from a previous step's cached result in context."""
        # context:step_N -> look at context results
        step_ref = source.replace("context:", "")
        step_idx = int(step_ref.replace("step_", ""))
        # Return current context data (the user is responsible for chaining correctly)
        return list(self.context.data)

    @staticmethod
    def _merge_row(
        left: dict, right: dict | None, right_key: str, prefix: str,
    ) -> dict:
        """Merge a left row with a right row, applying prefix."""
        out = dict(left)
        if right is None:
            return out
        for k, v in right.items():
            if k == right_key:
                continue  # Don't duplicate join key
            out_key = f"{prefix}{k}" if prefix else k
            out[out_key] = v
        return out

    def _inner_join(
        self,
        left: list[dict],
        right_index: dict,
        left_key: str,
        right_key: str,
        prefix: str,
    ) -> list[dict]:
        result = []
        for row in left:
            k = row.get(left_key)
            if k in right_index:
                for right_row in right_index[k]:
                    result.append(self._merge_row(row, right_row, right_key, prefix))
        return result

    def _left_join(
        self,
        left: list[dict],
        right_index: dict,
        left_key: str,
        right_key: str,
        prefix: str,
    ) -> list[dict]:
        result = []
        for row in left:
            k = row.get(left_key)
            if k in right_index:
                for right_row in right_index[k]:
                    result.append(self._merge_row(row, right_row, right_key, prefix))
            else:
                result.append(self._merge_row(row, None, right_key, prefix))
        return result

    def _outer_join(
        self,
        left: list[dict],
        right_index: dict,
        right_data: list[dict],
        left_key: str,
        right_key: str,
        prefix: str,
    ) -> list[dict]:
        result = []
        matched_right_keys = set()

        # Left side + matches
        for row in left:
            k = row.get(left_key)
            if k in right_index:
                matched_right_keys.add(k)
                for right_row in right_index[k]:
                    result.append(self._merge_row(row, right_row, right_key, prefix))
            else:
                result.append(self._merge_row(row, None, right_key, prefix))

        # Unmatched right rows
        for right_row in right_data:
            k = right_row.get(right_key)
            if k not in matched_right_keys:
                out = {}
                # Fill left fields with None
                if left:
                    for lk in left[0].keys():
                        out[lk] = None
                # Add right fields
                for rk, rv in right_row.items():
                    if rk == right_key:
                        out[left_key] = rv  # Map to left key name
                    else:
                        out_key = f"{prefix}{rk}" if prefix else rk
                        out[out_key] = rv
                result.append(out)

        return result
