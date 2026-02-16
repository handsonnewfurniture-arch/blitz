from __future__ import annotations

from typing import Any

from blitz.steps import BaseStep, StepRegistry
from blitz.utils.expr import compile_expr
from blitz.utils.jsonpath import jsonpath_extract


@StepRegistry.register("transform")
class TransformStep(BaseStep):
    """Data transformation: flatten, select, rename, filter, sort, dedupe, compute, limit."""

    async def execute(self) -> list[dict[str, Any]]:
        data = list(self.context.data)

        # 1. Flatten — extract nested data via JSONPath
        if "flatten" in self.config:
            data = self._flatten(data, self.config["flatten"])

        # 2. Select — keep only specified fields
        if "select" in self.config:
            fields = self.config["select"]
            data = [{k: row.get(k) for k in fields} for row in data]

        # 3. Rename — rename fields
        if "rename" in self.config:
            mapping = self.config["rename"]
            data = [
                {mapping.get(k, k): v for k, v in row.items()}
                for row in data
            ]

        # 4. Filter — keep rows matching expression
        if "filter" in self.config:
            expr = compile_expr(self.config["filter"])
            data = [row for row in data if expr(row)]

        # 5. Compute — add new computed fields
        if "compute" in self.config:
            for field_name, expression in self.config["compute"].items():
                expr = compile_expr(expression)
                for row in data:
                    row[field_name] = expr(row)

        # 6. Sort
        if "sort" in self.config:
            parts = self.config["sort"].split()
            sort_field = parts[0]
            descending = len(parts) > 1 and parts[1].lower() == "desc"
            data.sort(
                key=lambda r: (r.get(sort_field) is None, r.get(sort_field, 0)),
                reverse=descending,
            )

        # 7. Dedupe
        if "dedupe" in self.config:
            keys = self.config["dedupe"]
            if isinstance(keys, str):
                keys = [keys]
            seen = set()
            deduped = []
            for row in data:
                key = tuple(row.get(k) for k in keys)
                if key not in seen:
                    seen.add(key)
                    deduped.append(row)
            data = deduped

        # 8. Limit
        if "limit" in self.config:
            data = data[: self.config["limit"]]

        return data

    def _flatten(self, data: list[dict], path: str) -> list[dict]:
        """Extract nested data from each row using JSONPath and flatten into a list."""
        result = []
        for row in data:
            extracted = jsonpath_extract(row, path)
            if isinstance(extracted, list):
                for item in extracted:
                    if isinstance(item, dict):
                        result.append(item)
                    else:
                        result.append({"value": item})
            elif isinstance(extracted, dict):
                result.append(extracted)
            elif extracted is not None:
                result.append({"value": extracted})
        return result
