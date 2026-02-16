"""JIDOKA + POKA-YOKE: Quality gate step.

The Andon cord — stops the production line when quality fails.
Validates data schema, required fields, row counts, nulls, and anomalies.
"""

from __future__ import annotations

from typing import Any

from blitz.steps import BaseStep, StepRegistry
from blitz.exceptions import QualityGateError, AndonAlert

TYPE_MAP = {
    "int": int,
    "float": (int, float),
    "str": str,
    "bool": bool,
    "list": list,
    "dict": dict,
}


@StepRegistry.register("guard")
class GuardStep(BaseStep):
    """Quality gate: validate data schema and detect anomalies.

    YAML usage:
    - guard:
        schema:
          id: int
          name: str
          email: str
        required: [id, name]
        expect_rows: "10..1000"
        expect_no_nulls: [id, name]
        andon: true
    """

    async def execute(self) -> list[dict[str, Any]]:
        data = self.context.data
        errors: list[str] = []

        if "schema" in self.config:
            errors.extend(self._validate_schema(data))

        if "required" in self.config:
            errors.extend(self._validate_required(data))

        if "expect_rows" in self.config:
            errors.extend(self._validate_row_count(data))

        if "expect_no_nulls" in self.config:
            errors.extend(self._validate_no_nulls(data))

        if self.config.get("andon"):
            andon_errors = await self._andon_check(data)
            errors.extend(andon_errors)

        if errors:
            msg = "Quality gate FAILED:\n" + "\n".join(f"  - {e}" for e in errors)
            raise QualityGateError(msg)

        return data

    def _validate_schema(self, data: list[dict]) -> list[str]:
        """POKA-YOKE: Validate field types against schema definition."""
        schema = self.config["schema"]
        errors = []
        sample_size = min(len(data), 100)

        for i, row in enumerate(data[:sample_size]):
            for field, expected_type_name in schema.items():
                if field not in row:
                    continue
                value = row[field]
                if value is None:
                    continue

                expected_type = TYPE_MAP.get(expected_type_name)
                if expected_type and not isinstance(value, expected_type):
                    # Try coercion
                    try:
                        if expected_type_name == "int":
                            int(value)
                        elif expected_type_name == "float":
                            float(value)
                    except (ValueError, TypeError):
                        errors.append(
                            f"Row {i}: '{field}' expected {expected_type_name}, "
                            f"got {type(value).__name__} ({value!r})"
                        )
                        if len(errors) >= 10:
                            errors.append("(truncated, more errors exist)")
                            return errors

        return errors

    def _validate_required(self, data: list[dict]) -> list[str]:
        """POKA-YOKE: Ensure required fields exist in all rows."""
        required = self.config["required"]
        if isinstance(required, str):
            required = [required]

        errors = []
        for i, row in enumerate(data):
            for field in required:
                if field not in row:
                    errors.append(f"Row {i}: missing required field '{field}'")
                    if len(errors) >= 10:
                        errors.append("(truncated)")
                        return errors
        return errors

    def _validate_row_count(self, data: list[dict]) -> list[str]:
        """JIDOKA: Ensure row count is within expected range."""
        spec = str(self.config["expect_rows"])
        count = len(data)

        if ".." in spec:
            parts = spec.split("..")
            min_rows = int(parts[0])
            max_rows = int(parts[1])
            if count < min_rows:
                return [f"Expected at least {min_rows} rows, got {count}"]
            if count > max_rows:
                return [f"Expected at most {max_rows} rows, got {count}"]
        else:
            expected = int(spec)
            if count != expected:
                return [f"Expected exactly {expected} rows, got {count}"]

        return []

    def _validate_no_nulls(self, data: list[dict]) -> list[str]:
        """POKA-YOKE: Ensure specified fields have no null values."""
        fields = self.config["expect_no_nulls"]
        if isinstance(fields, str):
            fields = [fields]

        errors = []
        for field in fields:
            null_count = sum(1 for row in data if row.get(field) is None)
            if null_count > 0:
                errors.append(
                    f"Field '{field}' has {null_count} null values "
                    f"({null_count}/{len(data)} rows)"
                )
        return errors

    async def _andon_check(self, data: list[dict]) -> list[str]:
        """JIDOKA Andon: Compare current metrics against historical averages.

        Triggers alert if row count deviates >50% from average.
        """
        errors = []

        try:
            from blitz.tps.metrics import MetricsStore

            metrics = MetricsStore()
            pipeline_name = self.context.vars.get(
                "_pipeline_name", self.context.pipeline_name
            )
            if not pipeline_name:
                return []

            averages = await metrics.get_step_averages(pipeline_name)
            if not averages:
                return []  # No history yet

            # Check total data size against last step's average
            total_avg_rows = sum(v["avg_rows"] for v in averages.values())
            if total_avg_rows > 0:
                current = len(data)
                deviation = abs(current - total_avg_rows) / total_avg_rows
                if deviation > 0.5:
                    direction = "more" if current > total_avg_rows else "fewer"
                    errors.append(
                        f"Andon: {current} rows is {deviation:.0%} {direction} than "
                        f"average ({total_avg_rows}). Possible anomaly."
                    )
        except Exception:
            pass  # Don't fail the pipeline if metrics are unavailable

        return errors
