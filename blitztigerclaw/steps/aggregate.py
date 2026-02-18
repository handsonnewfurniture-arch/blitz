"""SQL-style GROUP BY + aggregation step.

Reduces datasets using sum, avg, min, max, count, count_distinct.
Supports optional group_by, having filter, and sort.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry
from blitztigerclaw.utils.expr import compile_expr

_AGG_RE = re.compile(r"^(sum|avg|min|max|count|count_distinct)\((\w+)\)$")


@StepRegistry.register("aggregate")
class AggregateStep(BaseStep):

    meta = StepMeta(
        strategy_escalations=((50_000, "multiprocess"),),
        description="SQL-style GROUP BY + aggregation (sum, avg, min, max, count, count_distinct)",
        config_docs={
            "group_by": "list[string] — fields to group on (optional for global agg)",
            "functions": "dict — {alias: 'func(field)'} e.g. {'total': 'sum(revenue)'}",
            "having": "string — post-aggregation filter expression",
            "sort": "string — sort results (e.g. 'total desc')",
        },
    )
    """GROUP BY + aggregation functions.

    YAML usage:
    - aggregate:
        group_by: [category, region]
        functions:
          total_revenue: "sum(revenue)"
          avg_price: "avg(price)"
          num_orders: "count(id)"
          unique_customers: "count_distinct(customer_id)"
        having: "total_revenue > 1000"
        sort: "total_revenue desc"
    """

    async def execute(self) -> list[dict[str, Any]]:
        data = self.context.data
        if not data:
            return []

        group_by = self.config.get("group_by", [])
        if isinstance(group_by, str):
            group_by = [group_by]
        functions = self.config.get("functions", {})

        if not functions:
            return data

        # Parse aggregation function definitions
        parsed_funcs = {}
        for alias, func_str in functions.items():
            match = _AGG_RE.match(func_str.strip())
            if not match:
                raise ValueError(
                    f"Invalid aggregation: {func_str!r}. "
                    f"Expected: sum|avg|min|max|count|count_distinct(field)"
                )
            parsed_funcs[alias] = (match.group(1), match.group(2))

        # Single-pass grouping
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for row in data:
            key = tuple(row.get(k) for k in group_by) if group_by else ()
            groups[key].append(row)

        # Compute aggregates per group
        result = []
        for group_key, rows in groups.items():
            out: dict[str, Any] = {}

            # Add group_by fields
            for i, field in enumerate(group_by):
                out[field] = group_key[i]

            # Compute each aggregate
            for alias, (func, field) in parsed_funcs.items():
                out[alias] = self._compute_agg(func, field, rows)

            result.append(out)

        # Having filter
        if "having" in self.config:
            expr = compile_expr(self.config["having"])
            result = [row for row in result if expr(row)]

        # Sort
        if "sort" in self.config:
            parts = self.config["sort"].split()
            sort_field = parts[0]
            descending = len(parts) > 1 and parts[1].lower() == "desc"
            result.sort(
                key=lambda r: (r.get(sort_field) is None, r.get(sort_field, 0)),
                reverse=descending,
            )

        return result

    @staticmethod
    def _compute_agg(func: str, field: str, rows: list[dict]) -> Any:
        """Compute a single aggregate function over a group of rows."""
        if func == "count":
            return sum(1 for r in rows if r.get(field) is not None)

        if func == "count_distinct":
            return len({r.get(field) for r in rows if r.get(field) is not None})

        # For numeric aggregates, collect non-null numeric values
        values = []
        for r in rows:
            v = r.get(field)
            if v is not None:
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    pass

        if not values:
            return None

        if func == "sum":
            return sum(values)
        if func == "avg":
            return sum(values) / len(values)
        if func == "min":
            return min(values)
        if func == "max":
            return max(values)

        return None
