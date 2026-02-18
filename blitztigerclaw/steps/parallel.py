"""DAG parallel step for BlitzTigerClaw v0.2.0.

Executes multiple step branches concurrently, then merges results.
Enables parallel independent operations (e.g., fetching from multiple APIs
simultaneously before combining results).

YAML usage:
    - parallel:
        branches:
          - fetch:
              url: https://api1.com/data
          - fetch:
              url: https://api2.com/data
        merge: concat    # concat | zip | dict
"""

from __future__ import annotations

import asyncio
from typing import Any

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry


@StepRegistry.register("parallel")
class ParallelStep(BaseStep):
    """Execute multiple step branches concurrently and merge results."""

    meta = StepMeta(
        default_strategy="async",
        description="Async step orchestration — run multiple tasks concurrently",
        config_docs={
            "tasks": "list — parallel task definitions",
        },
    )

    async def execute(self) -> list[dict[str, Any]]:
        branches = self.config.get("branches", [])
        merge_mode = self.config.get("merge", "concat")

        if not branches:
            return self.context.data

        # Build step instances for each branch
        tasks = []
        for branch_def in branches:
            if not isinstance(branch_def, dict) or len(branch_def) != 1:
                continue

            step_type = list(branch_def.keys())[0]
            step_config = branch_def[step_type] or {}

            step_class = StepRegistry.get(step_type)
            step = step_class(step_config, self.context)
            tasks.append(self._run_branch(step, step_type))

        # Execute all branches concurrently
        branch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Merge results based on merge mode
        return self._merge(branch_results, merge_mode)

    async def _run_branch(
        self, step: BaseStep, step_type: str
    ) -> list[dict[str, Any]]:
        """Run a single branch step."""
        try:
            return await step.execute()
        except Exception as e:
            return [{"_error": str(e), "_step": step_type}]

    def _merge(
        self,
        branch_results: list,
        mode: str,
    ) -> list[dict[str, Any]]:
        """Merge results from parallel branches."""
        # Filter out exceptions
        valid_results = []
        for result in branch_results:
            if isinstance(result, Exception):
                valid_results.append([{"_error": str(result)}])
            elif isinstance(result, list):
                valid_results.append(result)
            else:
                valid_results.append([])

        if mode == "concat":
            # Concatenate all results into a single list
            merged = []
            for result in valid_results:
                merged.extend(result)
            return merged

        elif mode == "zip":
            # Zip rows from each branch (stops at shortest)
            if not valid_results:
                return []
            min_len = min(len(r) for r in valid_results)
            merged = []
            for i in range(min_len):
                combined = {}
                for branch_idx, result in enumerate(valid_results):
                    for k, v in result[i].items():
                        key = f"branch_{branch_idx}_{k}" if k in combined else k
                        combined[key] = v
                merged.append(combined)
            return merged

        elif mode == "dict":
            # Each branch becomes a keyed entry
            return [
                {f"branch_{i}": result}
                for i, result in enumerate(valid_results)
            ]

        else:
            # Default: concat
            merged = []
            for result in valid_results:
                merged.extend(result)
            return merged

    def supports_streaming(self) -> bool:
        return False
