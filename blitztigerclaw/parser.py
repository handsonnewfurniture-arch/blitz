from __future__ import annotations

import os
import yaml
from pydantic import BaseModel, Field
from typing import Any

from blitztigerclaw.exceptions import ParseError
from blitztigerclaw.utils.url_expander import expand_vars


class StepDefinition(BaseModel):
    step_type: str
    config: dict[str, Any]


class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    vars: dict[str, Any] = Field(default_factory=dict)
    steps: list[StepDefinition] = Field(default_factory=list)
    on_error: str = "stop"
    plugins: list[str] = Field(default_factory=list)
    jit: bool = False
    # v0.2.0: Checkpoint support
    checkpoint: bool = False
    # v0.4.0: Explicit DAG definition (alternative to linear steps)
    graph: dict[str, Any] = Field(default_factory=dict)


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

    has_steps = "steps" in raw and raw["steps"]
    has_graph = "graph" in raw and raw["graph"]

    if not has_steps and not has_graph:
        raise ParseError("Pipeline must have 'steps' or 'graph'")

    # Merge overrides into vars
    variables = raw.get("vars", {})
    if overrides:
        variables.update(overrides)

    # Expand environment variables in vars
    for key, value in variables.items():
        if isinstance(value, str):
            variables[key] = os.path.expandvars(value)

    # Parse steps (linear mode)
    steps = []
    if has_steps:
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

    # Parse graph (DAG mode) — v0.4.0
    graph = {}
    if has_graph:
        graph = dict(raw["graph"])
        for node_id, node_def in graph.items():
            if "config" in node_def:
                node_def["config"] = _expand_config(node_def["config"], variables)

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
        graph=graph,
    )


def _expand_config(config: Any, variables: dict) -> Any:
    """Recursively expand variable references in config values.

    Also fixes YAML boolean key coercion: 'on', 'off', 'yes', 'no' are
    parsed as booleans by YAML. We convert them back to strings when
    used as dict keys.
    """
    if isinstance(config, str):
        return expand_vars(config, variables)
    if isinstance(config, dict):
        return {
            _fix_yaml_bool_key(k): _expand_config(v, variables)
            for k, v in config.items()
        }
    if isinstance(config, list):
        return [_expand_config(item, variables) for item in config]
    return config


def _fix_yaml_bool_key(key: Any) -> str:
    """Convert YAML boolean keys back to their string form.

    YAML 1.1 treats on/off/yes/no/true/false as booleans.
    When used as config keys they should remain strings.
    """
    if key is True:
        return "on"
    if key is False:
        return "off"
    return key


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
