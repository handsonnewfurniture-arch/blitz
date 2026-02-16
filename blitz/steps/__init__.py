from abc import ABC, abstractmethod
from typing import Any


class BaseStep(ABC):
    """Base class for all pipeline steps."""

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
