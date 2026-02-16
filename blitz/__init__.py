"""Blitz — 100x efficiency data automation framework."""

__version__ = "0.1.0"

from blitz.parser import parse_pipeline
from blitz.pipeline import Pipeline
from blitz.context import Context


async def run(yaml_path: str, **variables) -> Context:
    """Run a pipeline from a YAML file. Returns Context with results."""
    definition = parse_pipeline(yaml_path, variables or None)
    pipeline = Pipeline(definition)
    return await pipeline.run()


def run_sync(yaml_path: str, **variables) -> Context:
    """Synchronous wrapper for run()."""
    import asyncio
    return asyncio.run(run(yaml_path, **variables))
