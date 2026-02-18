from abc import ABC, abstractmethod
from typing import Any, AsyncIterator


class BaseStep(ABC):
    """Base class for all pipeline steps.

    v0.2.0: Added streaming interface (execute_stream / supports_streaming).
    v0.4.0: Added schema declarations (input_schema / output_schema) for
    typed DAG execution and projection pushdown.
    """

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
