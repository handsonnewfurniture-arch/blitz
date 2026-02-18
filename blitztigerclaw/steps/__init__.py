from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, ClassVar

import importlib
import pkgutil


@dataclass(frozen=True)
class StepMeta:
    """Declarative metadata for a pipeline step.

    Each step class sets ``meta = StepMeta(...)`` so the framework can
    read optimizer hints, docs, fusion eligibility, and linting rules
    generically — no hardcoded step names anywhere else.
    """

    # -- Optimizer --
    default_strategy: str = "sync"  # sync|async|streaming|multiprocess|batched
    strategy_escalations: tuple[tuple[int, str], ...] = ()  # [(5000, "streaming"), ...]
    streaming_breakers: tuple[str, ...] = ()  # config keys that disable streaming
    streaming: str = "no"  # yes|no|conditional

    # -- Planner --
    fusable: bool = False  # can participate in operator fusion
    is_source: bool = False  # data source (no input dependency)

    # -- Docs --
    description: str = ""
    config_docs: dict[str, str] = field(default_factory=dict)

    # -- Linting --
    required_config: tuple[str, ...] = ()  # at least one must be present


class BaseStep(ABC):
    """Base class for all pipeline steps.

    v0.2.0: Added streaming interface (execute_stream / supports_streaming).
    v0.4.0: Added schema declarations (input_schema / output_schema) for
    typed DAG execution and projection pushdown.
    v0.5.0: Added ``meta`` class variable for self-describing steps.
    """

    meta: ClassVar[StepMeta] = StepMeta()

    def __init__(self, config: dict[str, Any], context: "Context"):
        self.config = config
        self.context = context

    @abstractmethod
    async def execute(self) -> list[dict[str, Any]]:
        """Run the step. Returns rows of data."""
        ...

    async def execute_async(self) -> list[dict[str, Any]]:
        return await self.execute()

    async def execute_pooled(self) -> list[dict[str, Any]]:
        return await self.execute()

    async def execute_stream(self) -> AsyncIterator[dict[str, Any]]:
        """Streaming execution: yields rows one at a time.

        Default implementation falls back to execute() and yields from result.
        Override in subclasses for true streaming behavior.
        """
        result = await self.execute()
        for item in result:
            yield item

    def supports_streaming(self) -> bool:
        """Whether this step supports streaming execution.

        Override to return True in steps that implement execute_stream natively.
        """
        return False

    def input_schema(self) -> "DataSchema | None":
        """Declare the schema this step expects as input.

        Override to enable schema-aware optimization (projection pushdown).
        Return None if the step accepts any schema (default).
        """
        return None

    def output_schema(self, input_schema: "DataSchema | None" = None) -> "DataSchema | None":
        """Declare the schema this step produces.

        Override to enable schema propagation through the DAG.
        Receives the input schema for steps that transform it.
        Return None if the output schema is unknown (default).
        """
        return None


class StepRegistry:
    """Registry mapping step type names to their implementation classes."""

    _registry: dict[str, type[BaseStep]] = {}

    @classmethod
    def register(cls, name: str):
        def decorator(step_class: type[BaseStep]):
            cls._registry[name] = step_class
            return step_class
        return decorator

    @classmethod
    def get(cls, name: str) -> type[BaseStep]:
        if name not in cls._registry:
            available = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Unknown step type: '{name}'. Available: [{available}]"
            )
        return cls._registry[name]

    @classmethod
    def list_types(cls) -> list[str]:
        return sorted(cls._registry.keys())

    @classmethod
    def get_meta(cls, name: str) -> StepMeta:
        """Return the StepMeta for a registered step type."""
        return cls.get(name).meta

    @classmethod
    def all_meta(cls) -> dict[str, StepMeta]:
        """Return {name: StepMeta} for every registered step."""
        return {name: klass.meta for name, klass in sorted(cls._registry.items())}

    @classmethod
    def validate_all(cls) -> list[str]:
        """Fitness check: ensure every step has description and config_docs.

        Returns a list of error strings (empty = all good).
        """
        errors: list[str] = []
        for name, klass in sorted(cls._registry.items()):
            m = klass.meta
            if not m.description:
                errors.append(f"{name}: missing description")
            if not m.config_docs:
                errors.append(f"{name}: missing config_docs")
        return errors


# ---------------------------------------------------------------------------
# Auto-discovery
# ---------------------------------------------------------------------------

_discovered = False


def discover() -> None:
    """Auto-import all step modules in this package to trigger registration.

    Safe to call multiple times (idempotent).
    """
    global _discovered
    if _discovered:
        return
    _discovered = True

    package = importlib.import_module(__name__)
    for info in pkgutil.iter_modules(package.__path__):
        if info.name.startswith("_"):
            continue
        importlib.import_module(f"{__name__}.{info.name}")
