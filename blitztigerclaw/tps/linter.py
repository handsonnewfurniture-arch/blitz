"""POKA-YOKE: Static pipeline linter.

Analyzes pipeline YAML files for common mistakes, missing quality gates,
and anti-patterns before execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from blitztigerclaw.parser import parse_pipeline, PipelineDefinition
from blitztigerclaw.steps import StepRegistry


@dataclass
class LintResult:
    level: Literal["error", "warning", "suggestion"]
    step_index: int | None
    message: str
    tps_principle: str


class PipelineLinter:
    """Static analysis for pipeline YAML files."""

    REQUIRED_CONFIGS: dict[str, list[str]] = {
        "fetch": ["url", "urls"],
        "load": ["target"],
        "railway": ["action"],
        "netlify": ["action"],
        "github": ["action"],
        "scrape": ["url", "urls"],
    }

    def lint(self, file_path: str) -> list[LintResult]:
        """Run all checks and return findings."""
        try:
            definition = parse_pipeline(file_path)
        except Exception as e:
            return [LintResult("error", None, f"Parse error: {e}", "POKA-YOKE")]

        results: list[LintResult] = []
        results.extend(self._check_step_types(definition))
        results.extend(self._check_fetch_without_guard(definition))
        results.extend(self._check_load_first(definition))
        results.extend(self._check_duplicate_steps(definition))
        results.extend(self._check_missing_required_config(definition))
        results.extend(self._check_no_terminal_step(definition))
        results.extend(self._check_empty_pipeline(definition))
        return results

    def _check_step_types(self, definition: PipelineDefinition) -> list[LintResult]:
        """Warn on unregistered step types."""
        results = []
        available = StepRegistry.list_types()
        for i, step in enumerate(definition.steps):
            if step.step_type not in available:
                results.append(LintResult(
                    "error", i,
                    f"Unknown step type '{step.step_type}'. "
                    f"Available: {', '.join(available)}",
                    "POKA-YOKE",
                ))
        return results

    def _check_fetch_without_guard(
        self, definition: PipelineDefinition
    ) -> list[LintResult]:
        """POKA-YOKE: Suggest guard step after fetch/scrape before load."""
        results = []
        steps = definition.steps

        for i in range(len(steps) - 1):
            current = steps[i].step_type
            next_step = steps[i + 1].step_type

            if current in ("fetch", "scrape") and next_step == "load":
                results.append(LintResult(
                    "suggestion", i,
                    f"Consider adding a 'guard' step between '{current}' "
                    f"and 'load' for data validation (JIDOKA)",
                    "JIDOKA",
                ))

        return results

    def _check_load_first(self, definition: PipelineDefinition) -> list[LintResult]:
        """MUDA: Warn if load step is first (no data to load)."""
        results = []
        if definition.steps and definition.steps[0].step_type == "load":
            results.append(LintResult(
                "warning", 0,
                "First step is 'load' but no data has been produced yet. "
                "Consider adding a data-producing step first.",
                "MUDA",
            ))
        return results

    def _check_duplicate_steps(
        self, definition: PipelineDefinition
    ) -> list[LintResult]:
        """MUDA: Warn on identical consecutive steps."""
        results = []
        steps = definition.steps

        for i in range(len(steps) - 1):
            if (
                steps[i].step_type == steps[i + 1].step_type
                and steps[i].config == steps[i + 1].config
            ):
                results.append(LintResult(
                    "warning", i,
                    f"Steps {i + 1} and {i + 2} are identical "
                    f"'{steps[i].step_type}' steps. Possible waste.",
                    "MUDA",
                ))

        return results

    def _check_missing_required_config(
        self, definition: PipelineDefinition
    ) -> list[LintResult]:
        """POKA-YOKE: Verify required config keys per step type."""
        results = []

        for i, step in enumerate(definition.steps):
            required_keys = self.REQUIRED_CONFIGS.get(step.step_type)
            if required_keys is None:
                continue

            # Check if at least one of the required keys exists
            has_any = any(k in step.config for k in required_keys)
            if not has_any:
                results.append(LintResult(
                    "error", i,
                    f"Step '{step.step_type}' requires at least one of: "
                    f"{', '.join(required_keys)}",
                    "POKA-YOKE",
                ))

        return results

    def _check_no_terminal_step(
        self, definition: PipelineDefinition
    ) -> list[LintResult]:
        """Suggest adding a load/output step if pipeline has none."""
        results = []
        has_output = any(
            step.step_type in ("load", "file")
            for step in definition.steps
        )
        if not has_output:
            results.append(LintResult(
                "suggestion", None,
                "Pipeline has no output step (load/file). "
                "Data will be computed but not saved.",
                "MUDA",
            ))
        return results

    def _check_empty_pipeline(
        self, definition: PipelineDefinition
    ) -> list[LintResult]:
        """Basic sanity check."""
        if len(definition.steps) == 0:
            return [LintResult("error", None, "Pipeline has no steps", "POKA-YOKE")]
        return []
