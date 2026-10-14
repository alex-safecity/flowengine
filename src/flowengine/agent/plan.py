"""Dry-run / explain-plan — let an agent inspect a flow before executing it.

``explain(config)`` answers the questions an agent asks before trusting a
generated worker: *what will run, in what order, where does it branch, can it
loop, what components does it need, what data does it consume and produce?* It
reuses :class:`~flowengine.core.graph.GraphExecutor`'s ordering/cycle analysis so
the plan matches real execution semantics.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from flowengine.agent.meta import ComponentMeta
from flowengine.agent.semantic import build_meta_map
from flowengine.config.registry import ComponentRegistry
from flowengine.config.schema import FlowConfig, GraphNodeConfig
from flowengine.core.graph import GraphExecutor


class Branch(BaseModel):
    """A port-conditioned routing edge in a graph flow."""

    source: str
    port: str
    target: str


class DataEdge(BaseModel):
    """A data wire: where a node's bound input comes from (0.7.0 bindings)."""

    source: str = Field(description="'<node>.<key>[.path]', 'flow.<key>', or 'literal'/'expr'")
    target: str = Field(description="'<node>.<input key>'")
    kind: str = Field(description="reference | literal | expr")


class FlowPlan(BaseModel):
    """A machine-readable execution plan for a flow."""

    flow_type: str
    execution_order: list[str] = Field(default_factory=list)
    branches: list[Branch] = Field(default_factory=list)
    data_edges: list[DataEdge] = Field(default_factory=list)
    possible_cycles: bool = False
    max_iterations: int | None = None
    required_components: list[str] = Field(default_factory=list)
    context_inputs: list[str] = Field(default_factory=list)
    context_outputs: list[str] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def explain(
    config: FlowConfig,
    registry: ComponentRegistry | None = None,
) -> FlowPlan:
    """Produce a :class:`FlowPlan` describing how ``config`` would execute."""
    flow = config.flow
    required = sorted({c.type for c in config.components})
    metas = build_meta_map(config, registry=registry)

    if flow.type == "graph":
        return _explain_graph(config, required, metas)
    return _explain_steps(config, required, metas)


def _explain_steps(
    config: FlowConfig, required: list[str], metas: dict[str, ComponentMeta]
) -> FlowPlan:
    steps = config.flow.steps or []
    order = [s.component for s in steps]
    return FlowPlan(
        flow_type=config.flow.type,
        execution_order=order,
        branches=[],
        possible_cycles=False,
        max_iterations=None,
        required_components=required,
        context_inputs=_context_inputs(config, order, metas),
        context_outputs=_context_outputs(config, metas),
    )


def _explain_graph(
    config: FlowConfig, required: list[str], metas: dict[str, ComponentMeta]
) -> FlowPlan:
    nodes = config.flow.nodes or []
    edges = config.flow.edges or []
    executor = GraphExecutor(nodes, edges, {}, config.flow.settings)

    has_cycles = executor._has_cycles
    if has_cycles:
        order = _cyclic_order(executor, nodes)
    else:
        order = executor._topological_sort()

    branches = [
        Branch(source=e.source, port=e.port, target=e.target)
        for e in edges
        if e.port is not None
    ]
    node_order_components = [
        next(n.component for n in nodes if n.id == nid) for nid in order
    ]
    return FlowPlan(
        flow_type="graph",
        execution_order=order,
        branches=branches,
        data_edges=_data_edges(nodes),
        possible_cycles=has_cycles,
        max_iterations=config.flow.settings.max_iterations if has_cycles else None,
        required_components=required,
        context_inputs=_context_inputs(config, node_order_components, metas),
        context_outputs=_context_outputs(config, metas),
    )


def _data_edges(nodes: list[GraphNodeConfig]) -> list[DataEdge]:
    """The resolved data-dependency graph next to the control graph."""
    from flowengine.config.schema import BindingExpr, BindingLiteral

    edges: list[DataEdge] = []
    for node in nodes:
        if node.bind is None:
            continue
        for key, src in node.bind.inputs.items():
            target = f"{node.id}.{key}"
            if isinstance(src, BindingLiteral):
                edges.append(DataEdge(source="literal", target=target, kind="literal"))
            elif isinstance(src, BindingExpr):
                edges.append(DataEdge(source=f"expr: {src.expr}", target=target, kind="expr"))
            else:
                edges.append(DataEdge(source=src, target=target, kind="reference"))
    return edges


def _cyclic_order(executor: GraphExecutor, nodes: list) -> list[str]:
    """Best-effort linear order for a cyclic graph (BFS over non-back edges)."""
    from collections import deque

    roots = executor._find_roots_for_cyclic()
    seen: set[str] = set()
    order: list[str] = []
    queue: deque[str] = deque(roots)
    while queue:
        nid = queue.popleft()
        if nid in seen:
            continue
        seen.add(nid)
        order.append(nid)
        for edge in executor._forward.get(nid, []):
            if (edge.source, edge.target) in executor._back_edges:
                continue
            queue.append(edge.target)
    # Append any nodes not reached (defensive).
    for n in nodes:
        if n.id not in seen:
            order.append(n.id)
    return order


def _context_inputs(
    config: FlowConfig, ordered_components: list[str], metas: dict[str, ComponentMeta]
) -> list[str]:
    """Declared flow inputs, else the union of consumed keys lacking a producer."""
    if config.inputs:
        return list(config.inputs.keys())
    produced: set[str] = set()
    needed: list[str] = []
    for comp_name in ordered_components:
        meta = metas.get(comp_name)
        if meta is None:
            continue
        for key in meta.input_keys:
            if key not in produced and key not in needed:
                needed.append(key)
        produced.update(meta.output_keys)
    return needed


def _context_outputs(
    config: FlowConfig, metas: dict[str, ComponentMeta]
) -> list[str]:
    """Declared flow outputs, else the union of all produced keys."""
    if config.outputs:
        return list(config.outputs.keys())
    produced: list[str] = []
    for meta in metas.values():
        for key in meta.output_keys:
            if key not in produced:
                produced.append(key)
    return produced
