"""Conditional routing step — route data through different processing paths.

Supports two modes:
  - Field-match: branch on a field value (e.g. on: "status")
  - Expression-match: branch on when expressions (first match wins)
"""

from __future__ import annotations

import asyncio
from typing import Any

from blitztigerclaw.steps import BaseStep, StepRegistry
from blitztigerclaw.utils.expr import compile_expr
from blitztigerclaw.context import Context


@StepRegistry.register("branch")
class BranchStep(BaseStep):
    """Conditional data routing with sub-pipelines per route.

    YAML usage (field-match mode):
    - branch:
        on: "status"
        routes:
          active:
            - transform: { compute: { tier: "'premium'" } }
          inactive:
            - load: { target: stdout }
          _default:
            - load: { target: stdout }
        merge: true

    YAML usage (expression-match mode):
    - branch:
        routes:
          high_value:
            when: "revenue > 10000"
            steps:
              - transform: { compute: { tier: "'enterprise'" } }
          _default:
            steps:
              - transform: { compute: { tier: "'starter'" } }
        merge: true
    """

    async def execute(self) -> list[dict[str, Any]]:
        data = self.context.data
        if not data:
            return []

        routes = self.config.get("routes", {})
        merge = self.config.get("merge", True)
        field = self.config.get("on")

        if not routes:
            return data

        # Determine mode: field-match or expression-match
        if field:
            partitions = self._partition_by_field(data, field, routes)
        else:
            partitions = self._partition_by_expr(data, routes)

        # Execute each route's sub-pipeline concurrently
        tasks = []
        route_names = []
        for route_name, rows in partitions.items():
            if not rows:
                continue
            steps_config = self._get_route_steps(routes, route_name)
            if not steps_config:
                # No steps defined — pass through
                tasks.append(self._passthrough(rows))
            else:
                tasks.append(self._run_sub_pipeline(rows, steps_config))
            route_names.append(route_name)

        if not tasks:
            return []

        route_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge or return last
        if merge:
            merged = []
            for result in route_results:
                if isinstance(result, Exception):
                    merged.append({"_error": str(result)})
                elif isinstance(result, list):
                    merged.extend(result)
            return merged
        else:
            # Return last successful result
            for result in reversed(route_results):
                if isinstance(result, list):
                    return result
            return []

    def _partition_by_field(
        self,
        data: list[dict],
        field: str,
        routes: dict,
    ) -> dict[str, list[dict]]:
        """Partition rows by field value into route buckets."""
        partitions: dict[str, list[dict]] = {}
        for row in data:
            value = str(row.get(field, ""))
            if value in routes:
                partitions.setdefault(value, []).append(row)
            elif "_default" in routes:
                partitions.setdefault("_default", []).append(row)
        return partitions

    def _partition_by_expr(
        self,
        data: list[dict],
        routes: dict,
    ) -> dict[str, list[dict]]:
        """Partition rows by evaluating 'when' expressions. First match wins."""
        # Compile when expressions (skip _default)
        compiled = []
        for route_name, route_def in routes.items():
            if route_name == "_default":
                continue
            if isinstance(route_def, dict) and "when" in route_def:
                expr = compile_expr(route_def["when"])
                compiled.append((route_name, expr))

        partitions: dict[str, list[dict]] = {}
        for row in data:
            matched = False
            for route_name, expr in compiled:
                if expr(row):
                    partitions.setdefault(route_name, []).append(row)
                    matched = True
                    break
            if not matched and "_default" in routes:
                partitions.setdefault("_default", []).append(row)

        return partitions

    def _get_route_steps(self, routes: dict, route_name: str) -> list[dict]:
        """Extract the step definitions for a route."""
        route_def = routes.get(route_name, {})

        # Expression mode: {when: "...", steps: [...]}
        if isinstance(route_def, dict) and "steps" in route_def:
            return route_def["steps"]

        # Field mode: route value is directly a list of steps
        if isinstance(route_def, list):
            return route_def

        # _default with steps key
        if isinstance(route_def, dict) and route_name == "_default" and "steps" in route_def:
            return route_def["steps"]

        return []

    async def _run_sub_pipeline(
        self,
        data: list[dict],
        steps_config: list[dict],
    ) -> list[dict[str, Any]]:
        """Execute a list of steps as a mini sub-pipeline."""
        # Create a sub-context with the partitioned data
        sub_context = Context(
            vars=dict(self.context.vars),
            pipeline_name=self.context.pipeline_name,
        )
        sub_context.set_data(data)

        for step_def in steps_config:
            if not isinstance(step_def, dict) or len(step_def) != 1:
                continue
            step_type = list(step_def.keys())[0]
            step_config = step_def[step_type] or {}

            step_class = StepRegistry.get(step_type)
            step = step_class(step_config, sub_context)
            result = await step.execute()
            sub_context.set_data(result)

        return sub_context.data

    @staticmethod
    async def _passthrough(data: list[dict]) -> list[dict]:
        return data
