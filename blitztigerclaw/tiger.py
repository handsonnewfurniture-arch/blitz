"""Tiger — Autonomous AI agent for BlitzTigerClaw.

Powered by Claude API with tool_use. Tiger can understand natural language goals,
generate YAML pipelines, validate them, execute them, check metrics, detect
failures, and self-heal — all autonomously.

Usage:
    agent = TigerAgent(api_key="sk-ant-...")
    response = agent.run_goal("Fetch 10 pages from jsonplaceholder and store in posts.db")
    agent.chat("What pipelines have I created?")
    agent.monitor(interval=60)
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from blitztigerclaw.tiger_tools import TOOL_SCHEMAS, TOOL_HANDLERS, TIGER_DIR

TIGER_CONFIG_DIR = Path.home() / ".blitztigerclaw"
TIGER_GOALS_FILE = TIGER_CONFIG_DIR / "tiger_goals.json"
TIGER_API_KEY_FILE = TIGER_CONFIG_DIR / "tiger_api_key"

DEFAULT_MODEL = "claude-sonnet-4-5-20250929"
MAX_TOOL_ROUNDS = 25


# ---------------------------------------------------------------------------
# System prompt — teaches Tiger everything about BlitzTigerClaw
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are Tiger, the autonomous AI agent for BlitzTigerClaw — a 100x efficiency \
data automation framework.

You have full access to create, validate, execute, and monitor BlitzTigerClaw \
pipelines through your tools. You operate autonomously: given a high-level goal, \
you plan the pipeline, create it, validate it, execute it, and report results.

## BlitzTigerClaw Pipeline Format

Pipelines are YAML files with this structure:

```yaml
name: pipeline_name
description: optional description
jit: false          # JIT change detection
checkpoint: false   # Enable checkpoint/resume
on_error: stop      # stop or skip

vars:
  key: value

steps:
  - step_type:
      config_key: config_value
```

## Available Step Types

### fetch
Async HTTP fetching with parallel requests.
Config: url (string), urls (list), parallel (int, default 10), retry (int), \
timeout (int, default 30), headers (dict), method (string), extract (JSONPath string)

### transform
Data transformation.
Config: select (list of fields), rename (dict), filter (expression string), \
compute (dict of name: expression), flatten (JSONPath), sort (string, e.g. "value desc"), \
dedupe (list of keys), limit (int)

### load
Output to SQLite, CSV, JSON, or stdout.
Config: target (string — "sqlite:///path.db", "csv:///path.csv", "json:///path.json", "stdout"), \
table (string), mode ("insert"|"upsert"|"replace"), key (string), batch_size (int)

### file
File I/O operations.
Config: action ("read"|"write"|"glob"), path (string), format ("json"|"csv")

### scrape
HTML scraping with CSS selectors.
Config: url (string), urls (list), select (dict of field: css_selector), parallel (int)

### guard
JIDOKA quality gates.
Config: schema (dict of field: type), required (list), expect_rows (string range, e.g. "1..1000"), \
expect_no_nulls (list), andon (bool)

### shell
Execute shell commands.
Config: command (string)

### parallel
Async step orchestration.
Config: tasks (list of task definitions)

### railway
Railway.app integration.
Config: action (string)

### netlify
Netlify integration.
Config: action (string)

### github
GitHub API integration.
Config: action (string)

## Expression Language (for filter/compute)

Safe expressions supporting: comparisons (>, <, ==, !=, >=, <=), arithmetic (+, -, *, /, %), \
boolean (and, or, not), string methods (.upper(), .lower(), .strip(), .startswith(), .endswith(), \
.replace(), .split()), ternary (x if condition else y), builtins (len, int, float, str, bool, \
abs, min, max, sum, round, sorted).

## Your Autonomous Behavior Rules

1. **Always validate before execute**: After creating a pipeline, call validate_pipeline before run_pipeline.
2. **Check metrics after execution**: Call get_metrics to verify the run succeeded and note performance.
3. **Self-heal on failure**: If a pipeline fails, check the error, fix the pipeline, and retry.
4. **Use guard steps**: Add guard steps between fetch/scrape and load for data validation.
5. **Enable checkpoint** for multi-step pipelines so failures can resume.
6. **Report clearly**: After completing a goal, summarize what was done with key metrics.

## Pipeline File Location

Tiger-generated pipelines are stored in: ~/.blitztigerclaw/tiger_pipelines/
Always use the full path returned by create_pipeline when running/validating.

## Important Notes

- Step definitions in YAML use single-key dicts: `- fetch:` not `- type: fetch`
- The `target` in load step uses URI format: `sqlite:///file.db`, `csv:///file.csv`
- Variable references use `${var_name}` syntax in config values
- Expressions in filter/compute are Python-like but sandboxed
"""


class TigerAgent:
    """Autonomous AI agent backed by Claude API with tool_use."""

    def __init__(
        self,
        api_key: str | None = None,
        model: str = DEFAULT_MODEL,
    ):
        self.model = model
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY") or _load_api_key()

        if not self.api_key:
            raise RuntimeError(
                "No API key found. Run 'blitztigerclaw tiger setup' or set ANTHROPIC_API_KEY."
            )

        try:
            import anthropic
        except ImportError:
            raise RuntimeError(
                "anthropic package not installed. Install with: pip install anthropic"
            )

        self._client = anthropic.Anthropic(api_key=self.api_key)
        self._conversation: list[dict] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def chat(self, message: str) -> str:
        """Interactive mode: user talks, Tiger executes via tool_use.

        Maintains conversation history for multi-turn interaction.
        Returns Tiger's final text response.
        """
        self._conversation.append({"role": "user", "content": message})
        response_text = self._call_claude(self._conversation)
        self._conversation.append({"role": "assistant", "content": response_text})
        return response_text

    def run_goal(self, goal: str) -> str:
        """One-shot autonomous execution: plan -> create -> validate -> execute -> report.

        Does NOT maintain conversation history (fresh context each time).
        Returns Tiger's final summary.
        """
        messages = [
            {
                "role": "user",
                "content": (
                    f"Autonomously accomplish this goal:\n\n{goal}\n\n"
                    "Steps: 1) Plan the pipeline 2) Create it with create_pipeline "
                    "3) Validate with validate_pipeline 4) Execute with run_pipeline "
                    "5) Check results with get_metrics 6) Report what you did and the results."
                ),
            }
        ]

        # Save goal
        _save_goal(goal)

        return self._call_claude(messages)

    def monitor(self, interval: int = 60, on_event: Any = None) -> None:
        """Daemon mode: watch metrics, process queue, self-heal.

        Runs in a loop checking for:
        - Pending queue items -> execute them
        - Failed pipelines -> attempt resume from checkpoint
        - New goals in tiger_goals.json -> process them

        Args:
            interval: Seconds between check cycles.
            on_event: Optional callback(event_type, details) for logging.
        """
        def _log(event: str, details: str = ""):
            if on_event:
                on_event(event, details)
            else:
                print(f"[Tiger] {event}: {details}" if details else f"[Tiger] {event}")

        _log("monitor_start", f"interval={interval}s")

        while True:
            try:
                # 1. Process queue
                _log("checking_queue")
                queue_result = TOOL_HANDLERS["check_queue"]({})
                queue_data = json.loads(queue_result)
                backlog_count = queue_data["summary"].get("backlog", 0)

                if backlog_count > 0:
                    _log("processing_queue", f"{backlog_count} items in backlog")
                    exec_result = TOOL_HANDLERS["execute_queue"]({"limit": 5})
                    exec_data = json.loads(exec_result)
                    _log("queue_processed", f"{exec_data['processed']} items executed")

                    # Check for failures and attempt self-heal
                    for result in exec_data.get("results", []):
                        if result["status"] == "failed":
                            _log("failure_detected", f"{result['pipeline']}: {result.get('error', '?')}")
                            self._attempt_self_heal(result, _log)

                # 2. Check for unprocessed goals
                goals = _load_pending_goals()
                for goal_entry in goals:
                    _log("processing_goal", goal_entry["goal"])
                    try:
                        self.run_goal(goal_entry["goal"])
                        _mark_goal_done(goal_entry["id"])
                        _log("goal_completed", goal_entry["goal"])
                    except Exception as e:
                        _log("goal_failed", f"{goal_entry['goal']}: {e}")

                _log("cycle_complete", f"sleeping {interval}s")
            except KeyboardInterrupt:
                _log("monitor_stop", "interrupted by user")
                break
            except Exception as e:
                _log("cycle_error", str(e))

            time.sleep(interval)

    # ------------------------------------------------------------------
    # Claude API with tool_use loop
    # ------------------------------------------------------------------

    def _call_claude(self, messages: list[dict]) -> str:
        """Call Claude API with tool_use. Handles the tool dispatch loop.

        Returns the final text response after all tool calls are resolved.
        """
        # Make a working copy so we don't mutate the caller's list
        msgs = list(messages)

        for _ in range(MAX_TOOL_ROUNDS):
            response = self._client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOL_SCHEMAS,
                messages=msgs,
            )

            # Collect text and tool_use blocks
            text_parts = []
            tool_uses = []

            for block in response.content:
                if block.type == "text":
                    text_parts.append(block.text)
                elif block.type == "tool_use":
                    tool_uses.append(block)

            # If no tool calls, we're done
            if not tool_uses:
                return "\n".join(text_parts)

            # Build the assistant message with all content blocks
            msgs.append({"role": "assistant", "content": response.content})

            # Execute each tool call and collect results
            tool_results = []
            for tool_use in tool_uses:
                result = self._handle_tool_call(tool_use.name, tool_use.input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use.id,
                    "content": result,
                })

            msgs.append({"role": "user", "content": tool_results})

        # Exhausted tool rounds
        return "\n".join(text_parts) if text_parts else "[Tiger] Max tool rounds reached."

    def _handle_tool_call(self, name: str, input: dict) -> str:
        """Dispatch a tool call to the appropriate handler."""
        handler = TOOL_HANDLERS.get(name)
        if handler is None:
            return json.dumps({"error": f"Unknown tool: {name}"})
        try:
            return handler(input)
        except Exception as e:
            return json.dumps({"error": f"Tool '{name}' failed: {e}"})

    # ------------------------------------------------------------------
    # Self-healing
    # ------------------------------------------------------------------

    def _attempt_self_heal(self, failure: dict, log_fn: Any) -> None:
        """Try to recover a failed pipeline via Claude reasoning."""
        pipeline_name = failure.get("pipeline", "unknown")
        error = failure.get("error", "unknown error")

        # Check for checkpoint
        checkpoint_result = TOOL_HANDLERS["check_checkpoint"]({"pipeline_name": pipeline_name})
        checkpoint_data = json.loads(checkpoint_result)

        if checkpoint_data.get("has_checkpoint"):
            log_fn("self_heal", f"Attempting resume from checkpoint for '{pipeline_name}'")
            # Find the pipeline file
            pipelines_result = TOOL_HANDLERS["list_pipelines"]({})
            pipelines_data = json.loads(pipelines_result)
            for p in pipelines_data.get("pipelines", []):
                if p["name"] == pipeline_name:
                    resume_result = TOOL_HANDLERS["resume_pipeline"]({"file": p["file"]})
                    resume_data = json.loads(resume_result)
                    if resume_data.get("status") == "completed":
                        log_fn("self_heal_success", f"Resumed '{pipeline_name}' successfully")
                    else:
                        log_fn("self_heal_failed", f"Resume failed: {resume_data.get('error')}")
                    return

        # No checkpoint — ask Claude to diagnose and fix
        log_fn("self_heal", f"Asking Claude to diagnose failure for '{pipeline_name}'")
        try:
            self.run_goal(
                f"The pipeline '{pipeline_name}' failed with error: {error}. "
                f"Diagnose the issue, fix the pipeline, and re-execute it."
            )
            log_fn("self_heal_success", f"Claude fixed and re-ran '{pipeline_name}'")
        except Exception as e:
            log_fn("self_heal_failed", str(e))


# ---------------------------------------------------------------------------
# API key management
# ---------------------------------------------------------------------------


def _load_api_key() -> str | None:
    """Load API key from persistent storage."""
    if TIGER_API_KEY_FILE.exists():
        return TIGER_API_KEY_FILE.read_text().strip()
    return None


def save_api_key(api_key: str) -> None:
    """Save API key to persistent storage."""
    TIGER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    TIGER_API_KEY_FILE.write_text(api_key)
    TIGER_API_KEY_FILE.chmod(0o600)


# ---------------------------------------------------------------------------
# Goal persistence
# ---------------------------------------------------------------------------


def _save_goal(goal: str) -> None:
    """Append a goal to the persistent goals file."""
    TIGER_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    goals = []
    if TIGER_GOALS_FILE.exists():
        try:
            goals = json.loads(TIGER_GOALS_FILE.read_text())
        except (json.JSONDecodeError, ValueError):
            goals = []

    goals.append({
        "id": f"goal_{int(time.time())}",
        "goal": goal,
        "status": "completed",
        "created_at": time.time(),
    })

    TIGER_GOALS_FILE.write_text(json.dumps(goals, indent=2))


def _load_pending_goals() -> list[dict]:
    """Load goals with status 'pending' from the goals file."""
    if not TIGER_GOALS_FILE.exists():
        return []
    try:
        goals = json.loads(TIGER_GOALS_FILE.read_text())
        return [g for g in goals if g.get("status") == "pending"]
    except (json.JSONDecodeError, ValueError):
        return []


def _mark_goal_done(goal_id: str) -> None:
    """Mark a goal as completed."""
    if not TIGER_GOALS_FILE.exists():
        return
    try:
        goals = json.loads(TIGER_GOALS_FILE.read_text())
        for g in goals:
            if g.get("id") == goal_id:
                g["status"] = "completed"
                break
        TIGER_GOALS_FILE.write_text(json.dumps(goals, indent=2))
    except (json.JSONDecodeError, ValueError):
        pass
