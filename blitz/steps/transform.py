from __future__ import annotations

from typing import Any, AsyncIterator

from blitz.steps import BaseStep, StepRegistry
from blitz.utils.expr import compile_expr
from blitz.utils.jsonpath import jsonpath_extract


@StepRegistry.register("transform")
class TransformStep(BaseStep):
    """Data transformation: flatten, select, rename, filter, sort, dedupe, compute, limit.

    v0.2.0: Streaming support for row-level operations (select, rename, filter, compute).
    Automatically collects only when sort/dedupe/limit is needed.
    """

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

    async def execute_stream(self) -> AsyncIterator[dict[str, Any]]:
        """Streaming transform: applies row-level ops without materializing.

        Row-level ops (select, rename, filter, compute, flatten) stream through.
        Collection ops (sort, dedupe, limit) force materialization.
        """
        needs_collect = any(
            k in self.config for k in ("sort", "dedupe", "limit")
        )

        if needs_collect:
            # Fall back to batch execution for operations that need full dataset
            result = await self.execute()
            for item in result:
                yield item
            return

        # Stream row-level operations
        select_fields = self.config.get("select")
        rename_map = self.config.get("rename")
        filter_expr = compile_expr(self.config["filter"]) if "filter" in self.config else None
        compute_exprs = {}
        if "compute" in self.config:
            for field_name, expression in self.config["compute"].items():
                compute_exprs[field_name] = compile_expr(expression)

        flatten_path = self.config.get("flatten")

        for row in self.context.data:
            # Flatten
            if flatten_path:
                extracted = jsonpath_extract(row, flatten_path)
                if isinstance(extracted, list):
                    rows = [
                        item if isinstance(item, dict) else {"value": item}
                        for item in extracted
                    ]
                elif isinstance(extracted, dict):
                    rows = [extracted]
                elif extracted is not None:
                    rows = [{"value": extracted}]
                else:
                    rows = []
            else:
                rows = [row]

            for r in rows:
                # Select
                if select_fields:
                    r = {k: r.get(k) for k in select_fields}

                # Rename
                if rename_map:
                    r = {rename_map.get(k, k): v for k, v in r.items()}

                # Filter
                if filter_expr and not filter_expr(r):
                    continue

                # Compute
                for field_name, expr in compute_exprs.items():
                    r[field_name] = expr(r)

                yield r

    def supports_streaming(self) -> bool:
        # Streaming only when no collection ops needed
        return not any(k in self.config for k in ("sort", "dedupe", "limit"))

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
