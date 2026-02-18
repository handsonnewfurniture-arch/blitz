"""Data cleaning & type coercion step — POKA-YOKE applied to data itself.

Guard validates but doesn't fix. Clean fixes.
All operations are row-level and streamable.
"""

from __future__ import annotations

from typing import Any, AsyncIterator

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry

_TRUTHY = frozenset({"true", "1", "yes", "on", "t", "y"})
_FALSY = frozenset({"false", "0", "no", "off", "f", "n"})


def _coerce_bool(value: Any) -> bool:
    """Coerce a value to bool. Handles truthy/falsy strings."""
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in _TRUTHY:
        return True
    if s in _FALSY:
        return False
    return bool(value)


_COERCE_MAP = {
    "int": int,
    "float": float,
    "bool": _coerce_bool,
    "str": str,
}


@StepRegistry.register("clean")
class CleanStep(BaseStep):
    """Data cleaning and type coercion.

    YAML usage:
    - clean:
        coerce:
          age: int
          price: float
          active: bool
        defaults:
          status: "pending"
          score: 0
        trim: [name, email]
        lowercase: [email]
        uppercase: [country_code]
        replace:
          name: {"\\t": " ", "  ": " "}
        drop_nulls: [id, email]
        drop_empty: [name]
        rename: {old_field: new_field}
    """

    meta = StepMeta(
        strategy_escalations=((5_000, "streaming"),),
        streaming="yes",
        fusable=True,
        description="Data cleaning & type coercion — POKA-YOKE for data",
        config_docs={
            "coerce": "dict — type casting {field: int|float|bool|str}",
            "defaults": "dict — fill missing/null values {field: default}",
            "trim": "list[string] — strip whitespace from fields",
            "lowercase": "list[string] — lowercase string fields",
            "uppercase": "list[string] — uppercase string fields",
            "replace": "dict — string replacement {field: {old: new}}",
            "drop_nulls": "list[string] — drop rows where fields are null",
            "drop_empty": "list[string] — drop rows where fields are empty",
            "rename": "dict — rename fields {old: new}",
        },
    )

    async def execute(self) -> list[dict[str, Any]]:
        data = list(self.context.data)
        if not data:
            return []

        # Apply operations in config order
        coerce = self.config.get("coerce", {})
        defaults = self.config.get("defaults", {})
        trim_fields = self.config.get("trim", [])
        lowercase_fields = self.config.get("lowercase", [])
        uppercase_fields = self.config.get("uppercase", [])
        replace_map = self.config.get("replace", {})
        drop_nulls = self.config.get("drop_nulls", [])
        drop_empty = self.config.get("drop_empty", [])
        rename = self.config.get("rename", {})

        result = []
        for row in data:
            row = self._clean_row(
                row, coerce, defaults, trim_fields, lowercase_fields,
                uppercase_fields, replace_map, rename,
            )
            if self._should_drop(row, drop_nulls, drop_empty):
                continue
            result.append(row)

        return result

    async def execute_stream(self) -> AsyncIterator[dict[str, Any]]:
        """Streaming clean: processes rows one at a time."""
        coerce = self.config.get("coerce", {})
        defaults = self.config.get("defaults", {})
        trim_fields = self.config.get("trim", [])
        lowercase_fields = self.config.get("lowercase", [])
        uppercase_fields = self.config.get("uppercase", [])
        replace_map = self.config.get("replace", {})
        drop_nulls = self.config.get("drop_nulls", [])
        drop_empty = self.config.get("drop_empty", [])
        rename = self.config.get("rename", {})

        for row in self.context.data:
            row = self._clean_row(
                row, coerce, defaults, trim_fields, lowercase_fields,
                uppercase_fields, replace_map, rename,
            )
            if self._should_drop(row, drop_nulls, drop_empty):
                continue
            yield row

    def supports_streaming(self) -> bool:
        return True

    @staticmethod
    def _clean_row(
        row: dict,
        coerce: dict,
        defaults: dict,
        trim_fields: list,
        lowercase_fields: list,
        uppercase_fields: list,
        replace_map: dict,
        rename: dict,
    ) -> dict:
        """Apply all cleaning operations to a single row."""
        row = dict(row)  # Shallow copy

        # 1. Coerce types
        for field, target_type in coerce.items():
            if field in row and row[field] is not None:
                converter = _COERCE_MAP.get(target_type)
                if converter:
                    try:
                        row[field] = converter(row[field])
                    except (ValueError, TypeError):
                        pass  # Keep original on failure

        # 2. Fill defaults for missing/null
        for field, default in defaults.items():
            if row.get(field) is None:
                row[field] = default

        # 3. Trim whitespace
        for field in trim_fields:
            if field in row and isinstance(row[field], str):
                row[field] = row[field].strip()

        # 4. Lowercase
        for field in lowercase_fields:
            if field in row and isinstance(row[field], str):
                row[field] = row[field].lower()

        # 5. Uppercase
        for field in uppercase_fields:
            if field in row and isinstance(row[field], str):
                row[field] = row[field].upper()

        # 6. String replacement
        for field, replacements in replace_map.items():
            if field in row and isinstance(row[field], str):
                for old, new in replacements.items():
                    row[field] = row[field].replace(old, new)

        # 7. Rename fields
        if rename:
            row = {rename.get(k, k): v for k, v in row.items()}

        return row

    @staticmethod
    def _should_drop(row: dict, drop_nulls: list, drop_empty: list) -> bool:
        """Check if a row should be dropped."""
        for field in drop_nulls:
            if row.get(field) is None:
                return True
        for field in drop_empty:
            if row.get(field) == "":
                return True
        return False
