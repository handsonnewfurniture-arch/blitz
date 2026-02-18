"""Directed Acyclic Graph execution model for BlitzTigerClaw v0.4.0.

Replaces the linear step list with a typed graph of operations.
Nodes represent operations. Edges carry data between them.
Supports topological ordering and parallel execution grouping.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DagNode:
    """A single operation in the execution graph."""

    id: str
    step_type: str
    config: dict[str, Any] = field(default_factory=dict)
    # Planner annotations
    strategy: str = "sync"
    estimated_rows: int | None = None
    parallel_level: int = 0


@dataclass
class DagEdge:
    """Directed data-flow connection between two nodes."""

    source: str
    target: str
    port: str = "default"  # Named port for multi-input nodes (e.g. join)


class ExecutionDAG:
    """Directed acyclic graph of pipeline operations.

    Provides topological ordering, parallel group detection,
    and graph mutation for optimization passes.
    """

    __slots__ = ("nodes", "edges")

    def __init__(self):
        self.nodes: dict[str, DagNode] = {}
        self.edges: list[DagEdge] = []

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def add_node(self, node: DagNode) -> None:
        self.nodes[node.id] = node

    def add_edge(self, source: str, target: str, port: str = "default") -> None:
        self.edges.append(DagEdge(source=source, target=target, port=port))

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def predecessors(self, node_id: str) -> list[str]:
        """Node IDs that feed into this node."""
        return [e.source for e in self.edges if e.target == node_id]

    def successors(self, node_id: str) -> list[str]:
        """Node IDs that this node feeds into."""
        return [e.target for e in self.edges if e.source == node_id]

    def in_edges(self, node_id: str) -> list[DagEdge]:
        """All edges arriving at this node."""
        return [e for e in self.edges if e.target == node_id]

    def roots(self) -> list[str]:
        """Nodes with no incoming edges (data sources)."""
        targets = {e.target for e in self.edges}
        return [nid for nid in self.nodes if nid not in targets]

    def leaves(self) -> list[str]:
        """Nodes with no outgoing edges (sinks)."""
        sources = {e.source for e in self.edges}
        return [nid for nid in self.nodes if nid not in sources]

    # ------------------------------------------------------------------
    # Ordering
    # ------------------------------------------------------------------

    def topological_sort(self) -> list[str]:
        """Kahn's algorithm. Raises ValueError on cycle."""
        in_degree = {nid: 0 for nid in self.nodes}
        for edge in self.edges:
            in_degree[edge.target] += 1

        queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
        order: list[str] = []

        while queue:
            nid = queue.popleft()
            order.append(nid)
            for succ in self.successors(nid):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    queue.append(succ)

        if len(order) != len(self.nodes):
            raise ValueError("ExecutionDAG contains a cycle")
        return order

    def parallel_groups(self) -> list[list[str]]:
        """Group nodes into execution levels.

        Nodes in the same level have no dependencies on each other
        and can run concurrently.
        """
        order = self.topological_sort()
        level_of: dict[str, int] = {}

        for nid in order:
            preds = self.predecessors(nid)
            level_of[nid] = (max(level_of[p] for p in preds) + 1) if preds else 0

        by_level: dict[int, list[str]] = {}
        for nid, lvl in level_of.items():
            by_level.setdefault(lvl, []).append(nid)

        return [by_level[i] for i in sorted(by_level)]

    # ------------------------------------------------------------------
    # Mutation (used by optimization passes)
    # ------------------------------------------------------------------

    def remove_node(self, node_id: str) -> None:
        """Remove a node and all its edges."""
        self.nodes.pop(node_id, None)
        self.edges = [
            e for e in self.edges
            if e.source != node_id and e.target != node_id
        ]

    def redirect_edges(self, old_source: str, new_source: str) -> None:
        """Redirect all outgoing edges from old_source to new_source."""
        for edge in self.edges:
            if edge.source == old_source:
                edge.source = new_source

    def swap_adjacent(self, a_id: str, b_id: str) -> None:
        """Swap two adjacent nodes: (... -> A -> B -> ...) becomes (... -> B -> A -> ...).

        Precondition: there is an edge A -> B and A has one successor, B has one predecessor.
        """
        new_edges = []
        for e in self.edges:
            if e.source == a_id and e.target == b_id:
                # Direct link A->B becomes B->A
                new_edges.append(DagEdge(source=b_id, target=a_id, port=e.port))
            elif e.target == a_id:
                # Incoming to A -> redirect to B
                new_edges.append(DagEdge(source=e.source, target=b_id, port=e.port))
            elif e.source == b_id:
                # Outgoing from B -> redirect from A
                new_edges.append(DagEdge(source=a_id, target=e.target, port=e.port))
            else:
                new_edges.append(e)
        self.edges = new_edges

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.nodes)

    def __repr__(self) -> str:
        return f"ExecutionDAG({len(self.nodes)} nodes, {len(self.edges)} edges)"

    def describe(self) -> str:
        """Human-readable DAG description."""
        lines = [repr(self)]
        for group in self.parallel_groups():
            parallel = len(group) > 1
            for nid in group:
                node = self.nodes[nid]
                preds = self.predecessors(nid)
                tag = " [parallel]" if parallel else ""
                pred_str = f" <- {', '.join(preds)}" if preds else " (root)"
                lines.append(
                    f"  {nid}: {node.step_type} [{node.strategy}]{tag}{pred_str}"
                )
        return "\n".join(lines)
