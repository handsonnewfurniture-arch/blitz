"""Tiger AI Agent tools — Claude tool_use definitions and handlers.

Each tool maps to a BlitzTigerClaw capability: pipeline CRUD, execution,
metrics, queue management, validation, and checkpoint operations.

Generated pipelines are stored in ~/.blitztigerclaw/tiger_pipelines/.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any

import yaml

TIGER_DIR = Path.home() / ".blitztigerclaw" / "tiger_pipelines"

# ---------------------------------------------------------------------------
# Tool schemas (Claude tool_use format)
# ---------------------------------------------------------------------------

TOOL_SCHEMAS: list[dict] = [
    {
        "name": "create_pipeline",
        "description": (
            "Create a new BlitzTigerClaw YAML pipeline file from a name and list of steps. "
            "Each step is a dict with a single key (the step type) mapping to its config. "
            "Optionally include description, vars, jit, checkpoint, and on_error settings."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Pipeline name (used as filename and identifier)",
                },
                "steps": {
                    "type": "array",
                    "description": (
                        "List of step objects. Each object has exactly one key "
                        "(the step type) mapping to its config dict."
                    ),
                    "items": {"type": "object"},
                },
                "description": {
                    "type": "string",
                    "description": "Optional pipeline description",
                },
                "vars": {
                    "type": "object",
                    "description": "Optional variables dict",
                },
                "jit": {
                    "type": "boolean",
                    "description": "Enable JIT change detection (default false)",
                },
                "checkpoint": {
                    "type": "boolean",
                    "description": "Enable checkpoint/resume (default false)",
                },
                "on_error": {
                    "type": "string",
                    "enum": ["stop", "skip"],
                    "description": "Error handling strategy (default stop)",
                },
            },
            "required": ["name", "steps"],
        },
    },
    {
        "name": "run_pipeline",
        "description": (
            "Execute a BlitzTigerClaw pipeline YAML file and return the execution summary "
            "(rows, duration, step details, errors)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file": {
                    "type": "string",
                    "description": "Path to the pipeline YAML file",
                },
                "variables": {
                    "type": "object",
                    "description": "Optional variable overrides",
                },
            },
            "required": ["file"],
        },
    },
    {
        "name": "list_pipelines",
        "description": "List all Tiger-generated pipeline YAML files in ~/.blitztigerclaw/tiger_pipelines/.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "get_metrics",
        "description": (
            "Get performance history for a pipeline (recent runs with rows, duration, status). "
            "Uses the KAIZEN metrics store."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pipeline_name": {
                    "type": "string",
                    "description": "Name of the pipeline to check metrics for",
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent runs to return (default 10)",
                },
            },
            "required": ["pipeline_name"],
        },
    },
    {
        "name": "get_bottlenecks",
        "description": (
            "Detect performance bottlenecks for a pipeline and get optimization suggestions. "
            "Analyzes step-level timing data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "pipeline_name": {
                    "type": "string",
                    "description": "Name of the pipeline to analyze",
                },
            },
            "required": ["pipeline_name"],
        },
    },
    {
        "name": "check_queue",
        "description": "View the KANBAN board: pipelines in backlog, in_progress, done, and failed states.",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "queue_pipeline",
        "description": "Add a pipeline YAML file to the KANBAN backlog queue for later execution.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file": {
                    "type": "string",
                    "description": "Path to the pipeline YAML file",
                },
                "variables": {
                    "type": "object",
                    "description": "Optional variable overrides",
                },
            },
            "required": ["file"],
        },
    },
    {
        "name": "execute_queue",
        "description": "Process pending items from the KANBAN backlog queue. Executes pipelines in order.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max items to process (0 = all, default 0)",
                },
            },
        },
    },
    {
        "name": "validate_pipeline",
        "description": (
            "Lint and validate a pipeline YAML file without executing it. "
            "Checks for unknown step types, missing configs, anti-patterns."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "file": {
                    "type": "string",
                    "description": "Path to the pipeline YAML file",
                },
            },
            "required": ["file"],
        },
    },
    {
        "name": "read_pipeline",
        "description": "Read and return the full YAML contents of a pipeline file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file": {
                    "type": "string",
                    "description": "Path to the pipeline YAML file",
                },
            },
            "required": ["file"],
        },
    },
    {
        "name": "list_step_types",
        "description": (
            "Show all available BlitzTigerClaw step types and their configuration options. "
            "Use this to understand what steps can be used in pipelines."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "check_checkpoint",
        "description": "Check if a pipeline has a saved checkpoint that can be resumed from.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pipeline_name": {
                    "type": "string",
                    "description": "Name of the pipeline to check",
                },
            },
            "required": ["pipeline_name"],
        },
    },
    {
        "name": "resume_pipeline",
        "description": "Resume a failed pipeline from its last checkpoint.",
        "input_schema": {
            "type": "object",
            "properties": {
                "file": {
                    "type": "string",
                    "description": "Path to the pipeline YAML file",
                },
                "variables": {
                    "type": "object",
                    "description": "Optional variable overrides",
                },
            },
            "required": ["file"],
        },
    },
]


# ---------------------------------------------------------------------------
# Handler functions — called by TigerAgent._handle_tool_call()
# ---------------------------------------------------------------------------


def _safe_filename(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)


def handle_create_pipeline(input: dict) -> str:
    """Write a YAML pipeline to tiger_pipelines/ and return the path."""
    name = input["name"]
    steps = input["steps"]

    pipeline: dict[str, Any] = {"name": name}
    if input.get("description"):
        pipeline["description"] = input["description"]
    if input.get("vars"):
        pipeline["vars"] = input["vars"]
    if input.get("jit"):
        pipeline["jit"] = True
    if input.get("checkpoint"):
        pipeline["checkpoint"] = True
    if input.get("on_error") and input["on_error"] != "stop":
        pipeline["on_error"] = input["on_error"]

    pipeline["steps"] = steps

    TIGER_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{_safe_filename(name)}.yaml"
    path = TIGER_DIR / filename
    path.write_text(yaml.dump(pipeline, default_flow_style=False, sort_keys=False))

    return json.dumps({
        "status": "created",
        "file": str(path),
        "name": name,
        "step_count": len(steps),
    })


def handle_run_pipeline(input: dict) -> str:
    """Execute a pipeline and return summary."""
    from blitztigerclaw.parser import parse_pipeline
    from blitztigerclaw.pipeline import Pipeline
    from blitztigerclaw.exceptions import BlitzError

    file_path = input["file"]
    variables = input.get("variables") or {}

    try:
        definition = parse_pipeline(file_path, variables or None)
    except BlitzError as e:
        return json.dumps({"status": "error", "error": f"Parse error: {e}"})

    try:
        pipeline = Pipeline(definition, verbose=False)
        context = asyncio.run(pipeline.run())
        summary = context.summary()
        return json.dumps({
            "status": "completed",
            "pipeline": definition.name,
            "total_rows": summary["total_rows"],
            "total_duration_ms": round(summary["total_duration_ms"], 1),
            "memory_peak_mb": round(summary.get("memory_peak_mb", 0), 1),
            "streaming_mode": summary.get("streaming_mode", False),
            "steps": summary["steps"],
            "jit_steps_skipped": summary.get("jit_steps_skipped", 0),
        })
    except BlitzError as e:
        return json.dumps({"status": "failed", "error": str(e), "pipeline": definition.name})
    except Exception as e:
        return json.dumps({"status": "failed", "error": str(e)})


def handle_list_pipelines(_input: dict) -> str:
    """List all Tiger-generated pipelines."""
    TIGER_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(TIGER_DIR.glob("*.yaml"))
    pipelines = []
    for f in files:
        try:
            raw = yaml.safe_load(f.read_text())
            pipelines.append({
                "file": str(f),
                "name": raw.get("name", f.stem),
                "steps": len(raw.get("steps", [])),
                "description": raw.get("description", ""),
            })
        except Exception:
            pipelines.append({"file": str(f), "name": f.stem, "steps": 0, "description": "parse error"})

    return json.dumps({"count": len(pipelines), "pipelines": pipelines})


def handle_get_metrics(input: dict) -> str:
    """Get performance history from KAIZEN metrics store."""
    from blitztigerclaw.tps.metrics import MetricsStore

    async def _get():
        store = MetricsStore()
        try:
            history = await store.get_history(
                input["pipeline_name"],
                limit=input.get("limit", 10),
            )
            return history
        finally:
            await store.close()

    history = asyncio.run(_get())
    if not history:
        return json.dumps({"status": "no_data", "message": f"No runs recorded for '{input['pipeline_name']}'."})
    return json.dumps({"status": "ok", "runs": history})


def handle_get_bottlenecks(input: dict) -> str:
    """Detect bottlenecks with suggestions."""
    from blitztigerclaw.tps.metrics import MetricsStore

    async def _get():
        store = MetricsStore()
        try:
            return await store.detect_bottlenecks(input["pipeline_name"])
        finally:
            await store.close()

    bottlenecks = asyncio.run(_get())
    if not bottlenecks:
        return json.dumps({"status": "no_data", "message": "No bottleneck data available."})
    return json.dumps({"status": "ok", "bottlenecks": bottlenecks})


def handle_check_queue(_input: dict) -> str:
    """View KANBAN board status."""
    from blitztigerclaw.tps.kanban import KanbanBoard

    kanban = KanbanBoard()
    board = kanban.get_board()
    summary = {state: len(items) for state, items in board.items()}
    return json.dumps({
        "summary": summary,
        "board": {
            state: [
                {
                    "id": item["id"],
                    "pipeline_name": item.get("pipeline_name", "?"),
                    "state": item["state"],
                    "created_at": item.get("created_at"),
                }
                for item in items
            ]
            for state, items in board.items()
        },
    })


def handle_queue_pipeline(input: dict) -> str:
    """Add pipeline to KANBAN backlog."""
    from blitztigerclaw.parser import parse_pipeline
    from blitztigerclaw.tps.kanban import KanbanBoard
    from blitztigerclaw.exceptions import BlitzError

    file_path = input["file"]
    variables = input.get("variables") or {}

    try:
        definition = parse_pipeline(file_path, variables or None)
    except BlitzError as e:
        return json.dumps({"status": "error", "error": f"Parse error: {e}"})

    kanban = KanbanBoard()
    item_id = kanban.add(
        pipeline_file=file_path,
        pipeline_name=definition.name,
        variables=variables,
    )
    return json.dumps({"status": "queued", "id": item_id, "pipeline": definition.name})


def handle_execute_queue(input: dict) -> str:
    """Process pending KANBAN queue items."""
    from blitztigerclaw.parser import parse_pipeline
    from blitztigerclaw.pipeline import Pipeline
    from blitztigerclaw.tps.kanban import KanbanBoard

    kanban = KanbanBoard()
    limit = input.get("limit", 0)
    processed = 0
    results = []

    while True:
        if limit > 0 and processed >= limit:
            break

        item = kanban.pull_next()
        if item is None:
            break

        try:
            overrides = item.get("variables", {})
            definition = parse_pipeline(item["pipeline_file"], overrides or None)
            pipeline = Pipeline(definition, verbose=False, kanban_id=item["id"])
            context = asyncio.run(pipeline.run())
            summary = context.summary()
            results.append({
                "id": item["id"],
                "pipeline": definition.name,
                "status": "completed",
                "total_rows": summary["total_rows"],
                "total_duration_ms": round(summary["total_duration_ms"], 1),
            })
        except Exception as e:
            results.append({
                "id": item["id"],
                "pipeline": item.get("pipeline_name", "?"),
                "status": "failed",
                "error": str(e),
            })

        processed += 1

    return json.dumps({"processed": processed, "results": results})


def handle_validate_pipeline(input: dict) -> str:
    """Lint a pipeline file."""
    from blitztigerclaw.tps.linter import PipelineLinter

    linter = PipelineLinter()
    try:
        results = linter.lint(input["file"])
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)})

    if not results:
        return json.dumps({"status": "valid", "issues": []})

    issues = [
        {
            "level": r.level,
            "step_index": r.step_index,
            "message": r.message,
            "tps_principle": r.tps_principle,
        }
        for r in results
    ]
    errors = sum(1 for r in results if r.level == "error")
    return json.dumps({
        "status": "invalid" if errors > 0 else "warnings",
        "errors": errors,
        "warnings": sum(1 for r in results if r.level == "warning"),
        "suggestions": sum(1 for r in results if r.level == "suggestion"),
        "issues": issues,
    })


def handle_read_pipeline(input: dict) -> str:
    """Read pipeline YAML contents."""
    file_path = input["file"]
    try:
        content = Path(file_path).read_text()
        return json.dumps({"status": "ok", "file": file_path, "content": content})
    except FileNotFoundError:
        return json.dumps({"status": "error", "error": f"File not found: {file_path}"})
    except Exception as e:
        return json.dumps({"status": "error", "error": str(e)})


def handle_list_step_types(_input: dict) -> str:
    """Show available step types and their configs."""
    from blitztigerclaw.steps import StepRegistry, discover

    discover()

    step_docs = {}
    for name, meta in StepRegistry.all_meta().items():
        step_docs[name] = {
            "description": meta.description,
            "config": dict(meta.config_docs),
            "streaming": (
                True if meta.streaming == "yes"
                else "partial" if meta.streaming == "conditional"
                else False
            ),
        }

    return json.dumps({
        "registered_types": StepRegistry.list_types(),
        "step_docs": step_docs,
    })


def handle_check_checkpoint(input: dict) -> str:
    """Check if a pipeline has a resumable checkpoint."""
    from blitztigerclaw.checkpoint import CheckpointManager

    mgr = CheckpointManager(input["pipeline_name"])
    if not mgr.exists:
        return json.dumps({"has_checkpoint": False})

    info = mgr.info
    return json.dumps({
        "has_checkpoint": True,
        "completed_step": info.get("completed_step"),
        "timestamp": info.get("timestamp"),
        "data_count": info.get("data_count"),
    })


def handle_resume_pipeline(input: dict) -> str:
    """Resume a failed pipeline from its last checkpoint."""
    from blitztigerclaw.parser import parse_pipeline
    from blitztigerclaw.pipeline import Pipeline
    from blitztigerclaw.exceptions import BlitzError

    file_path = input["file"]
    variables = input.get("variables") or {}

    try:
        definition = parse_pipeline(file_path, variables or None)
    except BlitzError as e:
        return json.dumps({"status": "error", "error": f"Parse error: {e}"})

    try:
        pipeline = Pipeline(definition, verbose=False, resume=True)
        context = asyncio.run(pipeline.run())
        summary = context.summary()
        return json.dumps({
            "status": "completed",
            "pipeline": definition.name,
            "resumed": True,
            "total_rows": summary["total_rows"],
            "total_duration_ms": round(summary["total_duration_ms"], 1),
            "steps": summary["steps"],
        })
    except BlitzError as e:
        return json.dumps({"status": "failed", "error": str(e), "pipeline": definition.name})
    except Exception as e:
        return json.dumps({"status": "failed", "error": str(e)})


# ---------------------------------------------------------------------------
# Dispatch map
# ---------------------------------------------------------------------------

TOOL_HANDLERS: dict[str, Any] = {
    "create_pipeline": handle_create_pipeline,
    "run_pipeline": handle_run_pipeline,
    "list_pipelines": handle_list_pipelines,
    "get_metrics": handle_get_metrics,
    "get_bottlenecks": handle_get_bottlenecks,
    "check_queue": handle_check_queue,
    "queue_pipeline": handle_queue_pipeline,
    "execute_queue": handle_execute_queue,
    "validate_pipeline": handle_validate_pipeline,
    "read_pipeline": handle_read_pipeline,
    "list_step_types": handle_list_step_types,
    "check_checkpoint": handle_check_checkpoint,
    "resume_pipeline": handle_resume_pipeline,
}
