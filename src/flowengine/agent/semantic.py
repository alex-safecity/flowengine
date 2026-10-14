"""Semantic validation — checks that go beyond schema/structure.

Schema validation (Pydantic) answers "is this well-formed YAML for a flow?".
Semantic validation answers the questions an agent actually cares about:

* Are the ports used by edges declared by their source component?
* Does every declared output get produced by some component?
* Does every consumed key have a producer or a flow input?
* Are there unreachable nodes?
* Do cyclic graphs have an exit path (not just an iteration cap)?
* Is there at least one terminal output path?
* Which components require human approval before running?

Every check degrades gracefully: when a component exposes no
:class:`~flowengine.agent.meta.ComponentMeta`, data-flow checks that would need
it are skipped (or downgraded to warnings) rather than producing false errors.
This matters because agent-generated flows often use symbolic component types
(``web_search``) that are not importable at validation time.
"""

from __future__ import annotations

from flowengine.agent.bindings import check_bindings
from flowengine.agent.issues import FlowIssue, IssueCode
from flowengine.agent.meta import ComponentMeta
from flowengine.config.registry import ComponentRegistry, load_component_class
from flowengine.config.schema import FlowConfig
from flowengine.core.graph import ERROR_PORT as IMPLICIT_ERROR_PORT
from flowengine.errors import ConfigurationError


def resolve_component_meta(
    type_path: str,
    registry: ComponentRegistry | None = None,
) -> ComponentMeta | None:
    """Best-effort resolution of a component type to its ``ComponentMeta``.

    Tries, in order: a registered class (by name or path), then dynamic import of
    the type path. Returns ``None`` when the component cannot be resolved or
    declares no metadata — a normal, non-error condition for symbolic types.
    """
    component_class = None
    if registry is not None:
        component_class = registry.get_class(type_path)
    if component_class is None:
        try:
            component_class = load_component_class(type_path)
        except ConfigurationError:
            return None

    # Prefer the class-level attribute (no instantiation needed).
    meta = getattr(component_class, "meta", None)
    if isinstance(meta, ComponentMeta):
        return meta

    # Fall back to instance get_meta() for components that derive it dynamically.
    try:
        instance = component_class("__meta_probe__")
        probed = instance.get_meta()
        if isinstance(probed, ComponentMeta):
            return probed
    except Exception:
        return None
    return None


def build_meta_map(
    config: FlowConfig,
    registry: ComponentRegistry | None = None,
    metas: dict[str, ComponentMeta] | None = None,
) -> dict[str, ComponentMeta]:
    """Map each *component name* in the flow to its ``ComponentMeta`` (if any).

    Args:
        config: The flow configuration.
        registry: Optional registry for resolving component classes.
        metas: Optional explicit override keyed by component name OR type path.
            Lets callers (e.g. NeuroCore) inject metadata without importing.
    """
    result: dict[str, ComponentMeta] = {}
    metas = metas or {}
    for comp in config.components:
        if comp.name in metas:
            result[comp.name] = metas[comp.name]
        elif comp.type in metas:
            result[comp.name] = metas[comp.type]
        else:
            resolved = resolve_component_meta(comp.type, registry)
            if resolved is not None:
                result[comp.name] = resolved
    return result


def _graph_adjacency(
    config: FlowConfig,
) -> tuple[dict[str, list], dict[str, list], dict[str, str]]:
    """Return (outgoing, incoming, node->component) for a graph flow."""
    nodes = config.flow.nodes or []
    edges = config.flow.edges or []
    outgoing: dict[str, list] = {n.id: [] for n in nodes}
    incoming: dict[str, list] = {n.id: [] for n in nodes}
    node_component = {n.id: n.component for n in nodes}
    for edge in edges:
        if edge.source in outgoing:
            outgoing[edge.source].append(edge)
        if edge.target in incoming:
            incoming[edge.target].append(edge)
    return outgoing, incoming, node_component


def _find_cycle_nodes(outgoing: dict[str, list]) -> set[str]:
    """Return the set of nodes that participate in at least one cycle."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = dict.fromkeys(outgoing, WHITE)
    in_cycle: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> None:
        color[node] = GRAY
        stack.append(node)
        for edge in outgoing.get(node, []):
            tgt = edge.target
            if color.get(tgt) == GRAY:
                # Back edge: everything from tgt up the stack is in the cycle.
                if tgt in stack:
                    idx = stack.index(tgt)
                    in_cycle.update(stack[idx:])
            elif color.get(tgt) == WHITE:
                visit(tgt)
        stack.pop()
        color[node] = BLACK

    for n in outgoing:
        if color[n] == WHITE:
            visit(n)
    return in_cycle


def _check_ports(
    config: FlowConfig, metas: dict[str, ComponentMeta]
) -> list[FlowIssue]:
    """Edges may only use ports declared by their source component's meta."""
    issues: list[FlowIssue] = []
    if config.flow.type != "graph":
        return issues
    _, _, node_component = _graph_adjacency(config)
    for i, edge in enumerate(config.flow.edges or []):
        if edge.port is None or edge.port == IMPLICIT_ERROR_PORT:
            continue  # every node has an implicit error port (a guard BLOCK takes it)
        comp_name = node_component.get(edge.source)
        meta = metas.get(comp_name) if comp_name else None
        if meta is None or not meta.ports:
            continue  # can't verify — skip
        if edge.port not in meta.port_names:
            suggestion = None
            # Offer the closest declared port name.
            close = _closest(edge.port, meta.port_names)
            if close:
                suggestion = f"Did you mean port '{close}'?"
            issues.append(
                FlowIssue(
                    code=IssueCode.UNDECLARED_PORT,
                    severity="error",
                    path=f"flow.edges[{i}].port",
                    message=(
                        f"Edge from '{edge.source}' uses port '{edge.port}', "
                        f"which component '{meta.name}' does not declare. "
                        f"Declared ports: {meta.port_names}."
                    ),
                    why="The graph executor only routes edges whose port the source activates.",
                    suggestion=suggestion,
                )
            )
    return issues


def _check_reachability(config: FlowConfig) -> list[FlowIssue]:
    """Flag graph nodes with no structural path from a root node."""
    issues: list[FlowIssue] = []
    if config.flow.type != "graph":
        return issues
    outgoing, incoming, _ = _graph_adjacency(config)
    if not outgoing:
        return issues
    roots = [n for n in outgoing if not incoming.get(n)]
    if not roots:
        return issues  # pure cycle / no clear entry — handled by cycle checks
    reachable: set[str] = set()
    queue = list(roots)
    while queue:
        node = queue.pop()
        if node in reachable:
            continue
        reachable.add(node)
        for edge in outgoing.get(node, []):
            queue.append(edge.target)
    for node in outgoing:
        if node not in reachable:
            issues.append(
                FlowIssue(
                    code=IssueCode.UNREACHABLE_NODE,
                    severity="warning",
                    path=f"flow.nodes ({node})",
                    message=f"Node '{node}' is not reachable from any entry node.",
                    why="Unreachable nodes never execute and usually indicate a missing edge.",
                )
            )
    return issues


def _check_cycles_and_terminals(config: FlowConfig) -> list[FlowIssue]:
    """Cyclic graphs need an exit; the flow needs at least one terminal."""
    issues: list[FlowIssue] = []
    if config.flow.type != "graph":
        return issues
    outgoing, _, _ = _graph_adjacency(config)
    if not outgoing:
        return issues
    leaves = [n for n in outgoing if not outgoing.get(n)]
    cycle_nodes = _find_cycle_nodes(outgoing)

    # A cyclic flow with no leaf node anywhere can only stop by hitting
    # max_iterations — usually an oversight in agent-generated flows.
    if cycle_nodes and not leaves:
        issues.append(
            FlowIssue(
                code=IssueCode.CYCLE_WITHOUT_EXIT,
                severity="warning",
                path="flow.edges",
                message=(
                    "The graph is cyclic and has no terminal node; it can only "
                    "stop by reaching max_iterations."
                ),
                why="Agent loops should have an explicit exit edge (e.g. a 'done' port).",
                suggestion="Add a port-routed edge from the loop to a terminal node.",
            )
        )

    if not leaves and not cycle_nodes:
        issues.append(
            FlowIssue(
                code=IssueCode.NO_TERMINAL_OUTPUT,
                severity="warning",
                path="flow.edges",
                message="The graph has no terminal node (every node has an outgoing edge).",
                why="A flow needs at least one terminal path to produce final outputs.",
            )
        )
    return issues


def _check_contract(
    config: FlowConfig, metas: dict[str, ComponentMeta]
) -> list[FlowIssue]:
    """Validate the declared inputs/outputs contract against component metadata."""
    issues: list[FlowIssue] = []

    # Which keys does the flow produce, per available metadata?
    produced: set[str] = set()
    for meta in metas.values():
        produced.update(meta.output_keys)
    have_any_meta = bool(metas)

    # Declared outputs should be produced by some component (when we can tell).
    if config.outputs and have_any_meta:
        for key in config.outputs:
            if key not in produced:
                issues.append(
                    FlowIssue(
                        code=IssueCode.OUTPUT_NOT_PRODUCED,
                        severity="warning",
                        path=f"outputs.{key}",
                        message=(
                            f"Declared output '{key}' is not produced by any "
                            "component (per available metadata)."
                        ),
                        why="A flow should fulfil its declared output contract.",
                    )
                )

    # Each consumed key should have a producer or be a flow input.
    available = set(config.inputs.keys()) | produced
    for comp in config.components:
        meta = metas.get(comp.name)
        if meta is None:
            continue
        for key in meta.input_keys:
            if key not in available:
                issues.append(
                    FlowIssue(
                        code=IssueCode.MISSING_INPUT_PRODUCER,
                        severity="warning",
                        path=f"components ({comp.name})",
                        message=(
                            f"Component '{comp.name}' consumes '{key}', but no "
                            "flow input or upstream component produces it."
                        ),
                        why="A consumed key with no producer is read as missing/None at runtime.",
                        suggestion=f"Declare '{key}' under top-level inputs, or add a producing component.",
                    )
                )
    return issues


def _check_approvals(
    config: FlowConfig, metas: dict[str, ComponentMeta]
) -> list[FlowIssue]:
    """Surface components that need approval / carry elevated risk."""
    issues: list[FlowIssue] = []
    for comp in config.components:
        meta = metas.get(comp.name)
        if meta is None:
            continue
        if meta.requires_approval or meta.risk_level in ("high", "critical"):
            issues.append(
                FlowIssue(
                    code=IssueCode.APPROVAL_REQUIRED,
                    severity="warning",
                    path=f"components ({comp.name})",
                    message=(
                        f"Component '{comp.name}' ({meta.name}) is "
                        f"risk_level={meta.risk_level}"
                        + (", requires_approval" if meta.requires_approval else "")
                        + f". Effects: {meta.effects or 'unspecified'}."
                    ),
                    why="High-risk components should be approved before autonomous execution.",
                )
            )
    return issues


def _closest(word: str, candidates: list[str]) -> str | None:
    """Return the closest candidate by edit distance, if reasonably close."""
    if not candidates:
        return None
    import difflib

    matches = difflib.get_close_matches(word, candidates, n=1, cutoff=0.6)
    return matches[0] if matches else None


def validate_semantics(
    config: FlowConfig,
    registry: ComponentRegistry | None = None,
    metas: dict[str, ComponentMeta] | None = None,
) -> list[FlowIssue]:
    """Run all semantic checks against a (schema-valid) flow configuration.

    Args:
        config: A validated :class:`FlowConfig`.
        registry: Optional component registry for metadata resolution.
        metas: Optional explicit metadata overrides (by component name or type).

    Returns:
        A list of :class:`FlowIssue` (errors and warnings). Empty means clean.
    """
    meta_map = build_meta_map(config, registry, metas)
    issues: list[FlowIssue] = []
    issues.extend(_check_ports(config, meta_map))
    issues.extend(_check_reachability(config))
    issues.extend(_check_cycles_and_terminals(config))
    issues.extend(_check_contract(config, meta_map))
    issues.extend(_check_approvals(config, meta_map))
    issues.extend(check_bindings(config, meta_map))
    return issues
