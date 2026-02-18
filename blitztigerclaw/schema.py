"""Typed data schema for BlitzTigerClaw v0.4.0.

Provides schema inference from data, propagation through DAG edges,
and field tracking for projection pushdown.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Field:
    """A single typed field in a data schema."""

    name: str
    dtype: str = "any"  # int, float, str, bool, list, dict, any
    nullable: bool = True


@dataclass
class DataSchema:
    """Schema describing the structure of data flowing through a DAG edge.

    Immutable operations return new DataSchema instances.
    """

    fields: dict[str, Field] = field(default_factory=dict)
    row_estimate: int | None = None

    # ------------------------------------------------------------------
    # Projections
    # ------------------------------------------------------------------

    def select(self, names: list[str]) -> DataSchema:
        """Keep only the named fields."""
        return DataSchema(
            fields={k: v for k, v in self.fields.items() if k in set(names)},
            row_estimate=self.row_estimate,
        )

    def drop(self, *names: str) -> DataSchema:
        """Remove the named fields."""
        remove = set(names)
        return DataSchema(
            fields={k: v for k, v in self.fields.items() if k not in remove},
            row_estimate=self.row_estimate,
        )

    def merge(self, other: DataSchema, prefix: str = "") -> DataSchema:
        """Merge another schema into this one (for joins)."""
        merged = dict(self.fields)
        for k, f in other.fields.items():
            key = f"{prefix}{k}" if prefix else k
            merged[key] = Field(name=key, dtype=f.dtype, nullable=f.nullable)
        return DataSchema(fields=merged)

    def add_field(self, name: str, dtype: str = "any") -> DataSchema:
        """Add a single field."""
        new = dict(self.fields)
        new[name] = Field(name=name, dtype=dtype)
        return DataSchema(fields=new, row_estimate=self.row_estimate)

    def rename(self, mapping: dict[str, str]) -> DataSchema:
        """Rename fields according to mapping."""
        new_fields = {}
        for k, f in self.fields.items():
            new_name = mapping.get(k, k)
            new_fields[new_name] = Field(name=new_name, dtype=f.dtype, nullable=f.nullable)
        return DataSchema(fields=new_fields, row_estimate=self.row_estimate)

    def with_estimate(self, rows: int) -> DataSchema:
        """Return a copy with updated row estimate."""
        return DataSchema(fields=dict(self.fields), row_estimate=rows)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def field_names(self) -> list[str]:
        return list(self.fields.keys())

    @property
    def width(self) -> int:
        return len(self.fields)

    @property
    def known(self) -> bool:
        """Whether the schema has any field information."""
        return len(self.fields) > 0

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def unknown(cls) -> DataSchema:
        """Schema with no field information."""
        return cls()

    @classmethod
    def infer(cls, data: list[dict[str, Any]], sample_size: int = 100) -> DataSchema:
        """Infer schema from data by sampling rows."""
        if not data:
            return cls()

        fields: dict[str, Field] = {}
        has_null: set[str] = set()

        for row in data[:sample_size]:
            for k, v in row.items():
                if v is None:
                    has_null.add(k)
                    if k not in fields:
                        fields[k] = Field(name=k, dtype="any", nullable=True)
                    continue

                if k not in fields:
                    # bool check must come before int (bool is subclass of int)
                    if isinstance(v, bool):
                        fields[k] = Field(name=k, dtype="bool")
                    elif isinstance(v, int):
                        fields[k] = Field(name=k, dtype="int")
                    elif isinstance(v, float):
                        fields[k] = Field(name=k, dtype="float")
                    elif isinstance(v, str):
                        fields[k] = Field(name=k, dtype="str")
                    elif isinstance(v, list):
                        fields[k] = Field(name=k, dtype="list")
                    elif isinstance(v, dict):
                        fields[k] = Field(name=k, dtype="dict")
                    else:
                        fields[k] = Field(name=k, dtype="any")

        # Mark nullable fields
        for k in has_null:
            if k in fields:
                f = fields[k]
                fields[k] = Field(name=k, dtype=f.dtype, nullable=True)

        return cls(fields=fields, row_estimate=len(data))

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        if not self.fields:
            return "DataSchema(unknown)"
        cols = ", ".join(f"{f.name}:{f.dtype}" for f in self.fields.values())
        est = f", ~{self.row_estimate} rows" if self.row_estimate else ""
        return f"DataSchema({cols}{est})"
