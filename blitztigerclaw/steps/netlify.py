"""Netlify platform integration step.

Wraps the `netlify` CLI to check sites, deploy, and view status.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from blitztigerclaw.steps import BaseStep, StepRegistry


@StepRegistry.register("netlify")
class NetlifyStep(BaseStep):
    """Interact with Netlify platform via CLI.

    YAML usage:
    - netlify:
        action: sites | status | deploy | logs
        site: trinity-consciousness
        dir: ./dist
        prod: true
    """

    async def execute(self) -> list[dict[str, Any]]:
        action = self.config.get("action", "sites")
        site = self.config.get("site", "")
        deploy_dir = self.config.get("dir", ".")
        prod = self.config.get("prod", False)

        commands = {
            "sites": "netlify sites:list --json 2>/dev/null || netlify sites:list",
            "status": f"netlify status --json 2>/dev/null || netlify status",
            "deploy": self._build_deploy_cmd(deploy_dir, prod, site),
            "logs": f"netlify logs 2>&1 | head -50",
        }

        if action not in commands:
            return [{"_error": f"Unknown netlify action: {action}",
                     "_available": list(commands.keys())}]

        cmd = commands[action]
        return await self._run_command(cmd, action)

    def _build_deploy_cmd(self, deploy_dir: str, prod: bool, site: str) -> str:
        cmd = f"netlify deploy --dir {deploy_dir}"
        if prod:
            cmd += " --prod"
        if site:
            cmd += f" --site {site}"
        cmd += " 2>&1"
        return cmd

    async def _run_command(self, cmd: str, action: str) -> list[dict[str, Any]]:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return [{"_error": "Netlify command timed out", "_action": action}]

        output = stdout.decode("utf-8", errors="replace").strip()

        # Try parsing as JSON
        try:
            data = json.loads(output)
            if isinstance(data, list):
                return [
                    {k: v for k, v in item.items()
                     if k in ("name", "id", "url", "ssl_url", "admin_url",
                              "state", "updated_at", "account_slug")}
                    if isinstance(item, dict) else {"value": item}
                    for item in data
                ]
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

        return [{"_action": action, "_status": "ok"}]
