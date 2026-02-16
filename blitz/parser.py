from __future__ import annotations

import os
import yaml
from pydantic import BaseModel, Field
from typing import Any

from blitz.exceptions import ParseError
from blitz.utils.url_expander import expand_vars


class StepDefinition(BaseModel):
    step_type: str
    config: dict[str, Any]


class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    vars: dict[str, Any] = Field(default_factory=dict)
    steps: list[StepDefinition]
    on_error: str = "stop"
    plugins: list[str] = Field(default_factory=list)
    jit: bool = False
    # v0.2.0: Checkpoint support
    checkpoint: bool = False


def parse_pipeline(
    file_path: str, overrides: dict[str, Any] | None = None
) -> PipelineDefinition:
    """Parse a YAML pipeline file into a PipelineDefinition."""

    if not os.path.exists(file_path):
        raise ParseError(f"Pipeline file not found: {file_path}")

    with open(file_path, "r") as f:
        try:
            raw = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ParseError(f"Invalid YAML: {e}")

    if not isinstance(raw, dict):
        raise ParseError("Pipeline YAML must be a mapping (dict) at the top level")

    if "name" not in raw:
        raise ParseError("Pipeline must have a 'name' field")

    if "steps" not in raw or not raw["steps"]:
        raise ParseError("Pipeline must have at least one step")

    # Merge overrides into vars
    variables = raw.get("vars", {})
    if overrides:
        variables.update(overrides)

    # Expand environment variables in vars
    for key, value in variables.items():
        if isinstance(value, str):
            variables[key] = os.path.expandvars(value)

    # Parse steps
    steps = []
    for i, step_raw in enumerate(raw["steps"]):
        if not isinstance(step_raw, dict) or len(step_raw) != 1:
            raise ParseError(
                f"Step {i + 1}: each step must be a single-key dict "
                f"(e.g., '- fetch: ...')"
            )
        step_type = list(step_raw.keys())[0]
        config = step_raw[step_type] or {}

        # Expand variables in string config values
        config = _expand_config(config, variables)
        steps.append(StepDefinition(step_type=step_type, config=config))

    # Load plugins if specified
    if raw.get("plugins"):
        _load_plugins(raw["plugins"], file_path)

    return PipelineDefinition(
        name=raw["name"],
        description=raw.get("description", ""),
        vars=variables,
        steps=steps,
        on_error=raw.get("on_error", "stop"),
        plugins=raw.get("plugins", []),
        jit=raw.get("jit", False),
        checkpoint=raw.get("checkpoint", False),
    )


def _expand_config(config: Any, variables: dict) -> Any:
    """Recursively expand variable references in config values."""
    if isinstance(config, str):
        return expand_vars(config, variables)
    if isinstance(config, dict):
        return {k: _expand_config(v, variables) for k, v in config.items()}
    if isinstance(config, list):
        return [_expand_config(item, variables) for item in config]
    return config


def _load_plugins(plugin_paths: list[str], pipeline_path: str):
    """Load plugin files that register custom step types."""
    import importlib.util

    base_dir = os.path.dirname(os.path.abspath(pipeline_path))
    for path in plugin_paths:
        full_path = os.path.join(base_dir, path) if not os.path.isabs(path) else path
        if not os.path.exists(full_path):
            raise ParseError(f"Plugin not found: {full_path}")
        spec = importlib.util.spec_from_file_location("blitz_plugin", full_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
