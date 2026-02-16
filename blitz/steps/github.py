"""GitHub platform integration step.

Wraps the `gh` CLI to check notifications, issues, PRs, repos, and workflows.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from blitz.steps import BaseStep, StepRegistry


@StepRegistry.register("github")
class GitHubStep(BaseStep):
    """Interact with GitHub via gh CLI.

    YAML usage:
    - github:
        action: notifications | issues | pr_list | repo_info | workflow_runs
        repo: handsonnewfurniture-arch/hands-on-automation
        state: open
        limit: 20
    """

    async def execute(self) -> list[dict[str, Any]]:
        action = self.config.get("action", "notifications")
        repo = self.config.get("repo", "")
        state = self.config.get("state", "open")
        limit = self.config.get("limit", 20)

        commands = {
            "notifications": "gh api notifications 2>&1",
            "issues": (
                f"gh issue list --repo {repo} --state {state} --limit {limit} "
                f"--json number,title,state,author,createdAt 2>&1"
                if repo
                else f"gh issue list --state {state} --limit {limit} "
                     f"--json number,title,state,author,createdAt 2>&1"
            ),
            "pr_list": (
                f"gh pr list --repo {repo} --state {state} --limit {limit} "
                f"--json number,title,state,author,createdAt 2>&1"
                if repo
                else f"gh pr list --state {state} --limit {limit} "
                     f"--json number,title,state,author,createdAt 2>&1"
            ),
            "repo_info": (
                f"gh repo view {repo} --json name,description,stargazerCount,"
                f"forkCount,isPrivate,defaultBranchRef 2>&1"
                if repo
                else "gh repo list --json name,description,isPrivate --limit 20 2>&1"
            ),
            "workflow_runs": (
                f"gh run list --repo {repo} --limit {limit} "
                f"--json databaseId,displayTitle,status,conclusion,createdAt 2>&1"
                if repo
                else f"gh run list --limit {limit} "
                     f"--json databaseId,displayTitle,status,conclusion,createdAt 2>&1"
            ),
        }

        if action not in commands:
            return [{"_error": f"Unknown github action: {action}",
                     "_available": list(commands.keys())}]

        cmd = commands[action]
        return await self._run_command(cmd, action)

    async def _run_command(self, cmd: str, action: str) -> list[dict[str, Any]]:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return [{"_error": "GitHub command timed out", "_action": action}]

        output = stdout.decode("utf-8", errors="replace").strip()

        # Try parsing as JSON
        try:
            data = json.loads(output)
            if isinstance(data, list):
                if data and isinstance(data[0], dict):
                    return data
                return [{"value": item} for item in data]
            if isinstance(data, dict):
                return [data]
        except (json.JSONDecodeError, ValueError):
            pass

        # Parse line-based output
        if output:
            return [
                {"_action": action, "line": line, "_index": i}
                for i, line in enumerate(output.splitlines())
                if line.strip()
            ]

        err = stderr.decode("utf-8", errors="replace").strip()
        if err:
            return [{"_action": action, "_error": err}]

        return [{"_action": action, "_status": "no_data"}]
