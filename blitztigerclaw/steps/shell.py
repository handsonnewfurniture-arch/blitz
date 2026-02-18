from __future__ import annotations

import asyncio
import shlex
from typing import Any

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry


@StepRegistry.register("shell")
class ShellStep(BaseStep):
    """Run a shell command and capture output."""

    meta = StepMeta(
        is_source=True,
        description="Execute shell commands",
        config_docs={
            "command": "string — shell command to execute",
            "timeout": "int — timeout in seconds (default 60)",
            "capture": "string — output mode: lines | raw | json (default lines)",
        },
    )

    async def execute(self) -> list[dict[str, Any]]:
        command = self.config.get("command", "")
        timeout = self.config.get("timeout", 60)
        capture = self.config.get("capture", "lines")

        proc = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            return [{"_error": f"Command timed out after {timeout}s", "_command": command}]

        output = stdout.decode("utf-8", errors="replace")
        err = stderr.decode("utf-8", errors="replace")

        if capture == "lines":
            return [
                {"line": line, "_index": i}
                for i, line in enumerate(output.strip().splitlines())
                if line.strip()
            ]
        elif capture == "json":
            import json
            try:
                data = json.loads(output)
                if isinstance(data, list):
                    return data
                return [data]
            except json.JSONDecodeError:
                return [{"_raw": output, "_error": "Not valid JSON"}]
        else:
            return [{
                "_stdout": output,
                "_stderr": err,
                "_returncode": proc.returncode,
                "_command": command,
            }]
