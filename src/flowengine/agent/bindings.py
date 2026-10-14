"""Semantic checks for data bindings (``bind:`` blocks on graph nodes).

Codes (design/07 §4):

| Code | Severity | Meaning |
|---|---|---|
| ``UNKNOWN_BINDING_NODE`` | error | reference head is not a node id or ``flow`` |
| ``UNDECLARED_BINDING_KEY`` | warning | key not in the source component's declared outputs |
| ``BINDING_NOT_UPSTREAM`` | error (warning inside a cycle) | source is not an ancestor on the control graph |
| ``BINDING_FOREIGN_TARGET`` | error | output target outside the node's own namespace |
| ``UNBOUND_INPUT`` | warning | consumed key has no binding and no raw producer upstream |
| ``BINDING_TYPE_MISMATCH`` | warning | typed input and output specs disagree |
| ``BINDING_EXPR_INVALID`` | error | expression fails the safe-AST allow-list |

Every check degrades when metadata is missing, like the rest of the semantic layer.
"""

from __future__ import annotations

import ast
from typing import Any

from flowengine.agent.issues import FlowIssue, IssueCode, JsonPatchOp, RepairSuggestion
from flowengine.agent.meta import ComponentMeta
from flowengine.config.schema import (
    BindingExpr,
    BindingLiteral,
    FlowConfig,
    GraphNodeConfig,
)
from flowengine.core.binding import FLOW_NAMESPACE, flow_is_namespaced
from flowengine.eval.safe_ast import SafeASTValidator

_COMPATIBLE = {("integer", "number")}


def _closest(word: str, candidates: list[str]) -> str | None:
    if not candidates:
        return None
    import difflib

    matches = difflib.get_close_matches(word, candidates, n=1, cutoff=0.6)
    return matches[0] if matches else None


def _back_edges(nodes: list[GraphNodeConfig], edges: list[Any]) -> set[tuple[str, str]]:
    """Edges that close a cycle (DFS back edges), as the executor identifies them."""
    forward: dict[str, list[str]] = {n.id: [] for n in nodes}
    for e in edges:
        if e.source in forward:
            forward[e.source].append(e.target)
    white, gray, black = 0, 1, 2
    color = dict.fromkeys(forward, white)
    back: set[tuple[str, str]] = set()

    def visit(node: str) -> None:
        color[node] = gray
        for tgt in forward.get(node, []):
            if color.get(tgt) == gray:
                back.add((node, tgt))
            elif color.get(tgt) == white:
                visit(tgt)
        color[node] = black

    for nid in forward:
        if color[nid] == white:
            visit(nid)
    return back


def _ancestors(
    nodes: list[GraphNodeConfig],
    edges: list[Any],
    *,
    skip: frozenset[tuple[str, str]] | set[tuple[str, str]] = frozenset(),
) -> dict[str, set[str]]:
    """Node id -> every node that can reach it on the control graph (minus ``skip`` edges)."""
    incoming: dict[str, set[str]] = {n.id: set() for n in nodes}
    for e in edges:
        if e.target in incoming and (e.source, e.target) not in skip:
            incoming[e.target].add(e.source)
    result: dict[str, set[str]] = {}
    for nid in incoming:
        seen: set[str] = set()
        stack = list(incoming[nid])
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(incoming.get(cur, ()))
        result[nid] = seen
    return result


def _node_pointer(index: int, *rest: str) -> str:
    return "/flow/nodes/" + "/".join([str(index), *rest])


def check_bindings(config: FlowConfig, metas: dict[str, ComponentMeta]) -> list[FlowIssue]:
    """Run the binding checks over a graph flow (no-op for other flow types)."""
    issues: list[FlowIssue] = []
    if config.flow.type != "graph":
        return issues
    nodes = config.flow.nodes or []
    edges = config.flow.edges or []
    node_ids = [n.id for n in nodes]
    node_by_id = {n.id: n for n in nodes}
    ancestors = _ancestors(nodes, edges)  # through cycles
    forward_ancestors = _ancestors(nodes, edges, skip=_back_edges(nodes, edges))
    validator = SafeASTValidator()
    namespaced = flow_is_namespaced(nodes)

    def meta_of(node: GraphNodeConfig) -> ComponentMeta | None:
        return metas.get(node.component)

    for index, node in enumerate(nodes):
        if node.bind is None:
            continue
        for key, src in node.bind.inputs.items():
            path = f"flow.nodes[{index}].bind.inputs.{key}"
            if isinstance(src, BindingLiteral):
                continue
            if isinstance(src, BindingExpr):
                issues.extend(_check_expr(src.expr, path, index, key, validator))
                continue
            parts = src.split(".")
            if len(parts) < 2 or not all(parts):
                issues.append(
                    FlowIssue(
                        code=IssueCode.UNKNOWN_BINDING_NODE,
                        severity="error",
                        path=path,
                        message=f"Binding '{src}' is not of the form <node_id>.<key>.",
                        why="A reference names the node whose namespace holds the value.",
                    )
                )
                continue
            head, ref_key = parts[0], parts[1]
            if head == FLOW_NAMESPACE:
                continue
            if head not in node_by_id:
                close = _closest(head, node_ids)
                repair = None
                if close:
                    repair = RepairSuggestion(
                        explanation=f"Node '{head}' does not exist; '{close}' is the closest id.",
                        yaml_patch=[
                            JsonPatchOp(
                                op="replace",
                                path=_node_pointer(index, "bind", "inputs", key),
                                value=".".join([close, *parts[1:]]),
                            )
                        ],
                        confidence=0.6,
                    )
                issues.append(
                    FlowIssue(
                        code=IssueCode.UNKNOWN_BINDING_NODE,
                        severity="error",
                        path=path,
                        message=f"Binding '{src}' references unknown node '{head}'.",
                        why="The value would never be available at runtime.",
                        suggestion=f"Did you mean '{close}'?" if close else None,
                        repair=repair,
                    )
                )
                continue
            if head == node.id:
                continue  # a node may read its own namespace (loop state)
            if head not in forward_ancestors.get(node.id, set()):
                via_cycle = head in ancestors.get(node.id, set())
                issues.append(
                    FlowIssue(
                        code=IssueCode.BINDING_NOT_UPSTREAM,
                        severity="warning" if via_cycle else "error",
                        path=path,
                        message=(
                            f"Binding '{src}': node '{head}' only reaches '{node.id}' "
                            "through a cycle; its namespace is empty on the first pass."
                            if via_cycle
                            else f"Binding '{src}': node '{head}' is not upstream of "
                            f"'{node.id}' on the control graph."
                        ),
                        why="A namespace is only filled once its node has run.",
                        suggestion=(
                            "Give the expression an 'or' default for the first pass."
                            if via_cycle
                            else f"Add a control edge from '{head}' to '{node.id}' or bind from an ancestor."
                        ),
                    )
                )
            src_meta = meta_of(node_by_id[head])
            if src_meta is not None and src_meta.outputs and ref_key not in src_meta.outputs:
                close = _closest(ref_key, src_meta.output_keys)
                issues.append(
                    FlowIssue(
                        code=IssueCode.UNDECLARED_BINDING_KEY,
                        severity="warning",
                        path=path,
                        message=(
                            f"Binding '{src}': component '{src_meta.name}' does not declare "
                            f"output '{ref_key}'. Declared: {src_meta.output_keys}."
                        ),
                        why="Dynamic outputs are allowed, but an undeclared key is usually a typo.",
                        suggestion=f"Did you mean '{head}.{close}'?" if close else None,
                        repair=RepairSuggestion(
                            explanation=f"'{close}' is the closest declared output of '{head}'.",
                            yaml_patch=[
                                JsonPatchOp(
                                    op="replace",
                                    path=_node_pointer(index, "bind", "inputs", key),
                                    value=".".join([head, close, *parts[2:]]),
                                )
                            ],
                            confidence=0.5,
                        )
                        if close
                        else None,
                    )
                )
            elif src_meta is not None and ref_key in src_meta.outputs:
                own_meta = meta_of(node)
                if own_meta is not None and key in own_meta.inputs:
                    want, have = own_meta.inputs[key].type, src_meta.outputs[ref_key].type
                    if (
                        "any" not in (want, have)
                        and want != have
                        and (want, have) not in _COMPATIBLE
                        and (have, want) not in _COMPATIBLE
                    ):
                        issues.append(
                            FlowIssue(
                                code=IssueCode.BINDING_TYPE_MISMATCH,
                                severity="warning",
                                path=path,
                                message=(
                                    f"Binding '{src}' has type '{have}' but '{node.id}' "
                                    f"expects '{want}' for '{key}'."
                                ),
                                why="Mismatched types usually fail inside the component.",
                            )
                        )

        for key, target in node.bind.outputs.items():
            head = target.split(".")[0]
            if head != node.id:
                issues.append(
                    FlowIssue(
                        code=IssueCode.BINDING_FOREIGN_TARGET,
                        severity="error",
                        path=f"flow.nodes[{index}].bind.outputs.{key}",
                        message=(
                            f"Output target '{target}' is outside the namespace of '{node.id}'."
                        ),
                        why="A node writes only into its own namespace; consumers bind from it.",
                        repair=RepairSuggestion(
                            explanation="Keep the output inside the node's own namespace.",
                            yaml_patch=[
                                JsonPatchOp(
                                    op="replace",
                                    path=_node_pointer(index, "bind", "outputs", key),
                                    value=f"{node.id}.{key}",
                                )
                            ],
                            confidence=0.8,
                        ),
                    )
                )

    if namespaced:
        issues.extend(_check_unbound_inputs(config, metas, ancestors))
    return issues


def _check_expr(
    expr: str, path: str, index: int, key: str, validator: SafeASTValidator
) -> list[FlowIssue]:
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        return [
            FlowIssue(
                code=IssueCode.BINDING_EXPR_INVALID,
                severity="error",
                path=path,
                message=f"Binding expression '{expr}' has invalid syntax: {exc.msg}.",
                why="The expression is parsed before the flow runs.",
            )
        ]
    if not validator.validate(tree):
        return [
            FlowIssue(
                code=IssueCode.BINDING_EXPR_INVALID,
                severity="error",
                path=path,
                message=f"Binding expression '{expr}' is not allowed: {validator.errors}.",
                why="Only the safe-AST subset (no calls, no imports) may run inside a flow.",
                suggestion="Use attribute access, arithmetic, comparisons and 'or' defaults.",
            )
        ]
    return []


def _check_unbound_inputs(
    config: FlowConfig, metas: dict[str, ComponentMeta], ancestors: dict[str, set[str]]
) -> list[FlowIssue]:
    """In namespaced mode, a consumed key without a binding is only served by raw
    keys of unbound ancestors (or flow inputs)."""
    issues: list[FlowIssue] = []
    nodes = config.flow.nodes or []
    node_by_id = {n.id: n for n in nodes}
    flow_inputs = set(config.inputs.keys())
    for index, node in enumerate(nodes):
        meta = metas.get(node.component)
        if meta is None or not meta.inputs or node.bind is None:
            continue  # unbound nodes read raw keys; MISSING_INPUT_PRODUCER covers them
        bound = set(node.bind.inputs)
        raw_available: set[str] = set(flow_inputs)
        for anc in ancestors.get(node.id, set()):
            anc_node = node_by_id.get(anc)
            anc_meta = metas.get(anc_node.component) if anc_node else None
            if anc_node is not None and anc_node.bind is None and anc_meta is not None:
                raw_available.update(anc_meta.output_keys)
        for key, spec in meta.inputs.items():
            if key in bound or key in raw_available or not spec.required:
                continue
            producers = [
                a
                for a in ancestors.get(node.id, set())
                if (m := metas.get(node_by_id[a].component)) is not None and key in m.outputs
            ]
            repair = None
            if producers:
                repair = RepairSuggestion(
                    explanation=f"Bind '{key}' from upstream node '{producers[0]}'.",
                    yaml_patch=[
                        JsonPatchOp(
                            op="add",
                            path=_node_pointer(index, "bind", "inputs", key),
                            value=f"{producers[0]}.{key}",
                        )
                    ],
                    confidence=0.5,
                )
            issues.append(
                FlowIssue(
                    code=IssueCode.UNBOUND_INPUT,
                    severity="warning",
                    path=f"flow.nodes[{index}].bind.inputs.{key}",
                    message=(
                        f"Node '{node.id}' consumes '{key}' but has no binding for it and no "
                        "unbound upstream node produces it as a raw key."
                    ),
                    why="In a namespaced flow, data reaches a node through bindings.",
                    suggestion=(
                        f"Bind '{key}' from '{producers[0]}.{key}'." if producers else None
                    ),
                    repair=repair,
                )
            )
    return issues


__all__ = ["check_bindings"]
