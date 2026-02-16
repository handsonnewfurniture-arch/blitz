"""Railway platform integration step.

Wraps the `railway` CLI to check status, view logs, deploy, and manage variables.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from blitztigerclaw.steps import BaseStep, StepRegistry


@StepRegistry.register("railway")
class RailwayStep(BaseStep):
    """Interact with Railway platform via CLI.

    YAML usage:
    - railway:
        action: status | logs | deploy | variables
        project: giving-wonder
        service: consciousness-revolution
        lines: 50
    """

    async def execute(self) -> list[dict[str, Any]]:
        action = self.config.get("action", "status")
        lines = self.config.get("lines", 50)

        commands = {
            "status": "railway status --json 2>/dev/null || railway status",
            "logs": f"railway logs --limit {lines} 2>&1",
            "deploy": "railway up --detach 2>&1",
            "variables": "railway variables --json 2>/dev/null || railway variables",
        }

        if action not in commands:
            return [{"_error": f"Unknown railway action: {action}",
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
            return [{"_error": "Railway command timed out", "_action": action}]

        output = stdout.decode("utf-8", errors="replace").strip()

        # Try parsing as JSON
        try:
            data = json.loads(output)
            if isinstance(data, list):
                return data
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

        return [{"_action": action, "_status": "ok", "_returncode": proc.returncode}]
