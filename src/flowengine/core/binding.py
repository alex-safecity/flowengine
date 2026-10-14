"""Data bindings: per-node namespaces for graph flows.

A graph flow whose nodes carry ``bind`` blocks runs in **namespaced mode**: every node
executes against a :class:`ScopedContext` and what it writes lands in
``context.data.<node_id>`` (its *namespace*); bound inputs are resolved from other
namespaces — ``<node_id>.<key>[.<path>...]`` references, literals or expressions — and
placed in front of the component under the key it reads. Skills and components are
untouched: they still ``context.get`` / ``context.set`` flat keys.

A flow with no ``bind`` on any node runs exactly as before (legacy mode): the executor
does not wrap anything.

Namespaced mode in one picture::

    context.data.flow       = { limit: 100 }           # flow inputs (raw keys at start)
    context.data.n_extract  = { incident_id: "…" }     # what node n_extract wrote
    context.data.n_lookup   = { rows: [...] }          # what node n_lookup wrote
    context.data.resume_data, context.metadata, context.input — shared, unchanged

Rules:

- **Reads** by a component fall through: its own overlay first (bound inputs, what it
  wrote on earlier visits — its namespace is loaded into the overlay at the start of each
  visit, under the keys the component uses — and what it wrote so far), then the parent
  data (``resume_data``, raw keys of unbound nodes).
- **Writes** go to the overlay only, then are merged into the node's namespace; an
  explicit ``bind.outputs`` entry may rename the key within that namespace. A key the
  component deleted from its overlay is removed from the namespace.
- An **unbound** node in a namespaced flow is also mirrored to raw keys, so consumers that
  predate bindings keep working; the compiler warns (``UNBOUND_INPUT``).
- ``metadata`` is the parent's object (suspension, errors and timings propagate for free);
  the active output port is copied back after the call.
- A reference that cannot be resolved raises :class:`BindingError` (an execution error
  routed through the node's ``on_error`` policy); expressions may use ``or`` for defaults.
"""

from __future__ import annotations

import ast
import inspect
from collections.abc import Awaitable
from typing import Any

from flowengine.config.schema import (
    BindingExpr,
    BindingLiteral,
    BindingSource,
    GraphNodeConfig,
    NodeBinding,
)
from flowengine.core.component import BaseComponent
from flowengine.core.context import DotDict, FlowContext
from flowengine.errors import FlowExecutionError
from flowengine.eval.safe_ast import SafeASTValidator

FLOW_NAMESPACE = "flow"


class BindingError(FlowExecutionError):
    """A binding could not be resolved or an output target is invalid."""


def flow_is_namespaced(nodes: list[GraphNodeConfig]) -> bool:
    """True when any node carries a ``bind`` block (namespaced mode)."""
    return any(n.bind is not None for n in nodes)


def parse_reference(ref: str) -> tuple[str, list[str]]:
    """Split ``head.key.path...`` into ``(head, [key, path...])``."""
    parts = ref.split(".")
    if len(parts) < 2 or not all(parts):
        raise BindingError(f"Invalid binding reference '{ref}': expected <node_id>.<key>")
    return parts[0], parts[1:]


def walk_path(value: Any, path: list[str], ref: str) -> Any:
    """Follow ``path`` through dicts, DotDicts, lists and attributes."""
    for seg in path:
        if isinstance(value, DotDict):
            value = value._data
        if isinstance(value, dict):
            if seg not in value:
                raise BindingError(f"Binding reference '{ref}': key '{seg}' not found")
            value = value[seg]
        elif isinstance(value, (list, tuple)):
            try:
                value = value[int(seg)]
            except (ValueError, IndexError) as exc:
                raise BindingError(f"Binding reference '{ref}': bad index '{seg}'") from exc
        elif hasattr(value, seg):
            value = getattr(value, seg)
        else:
            raise BindingError(f"Binding reference '{ref}': cannot descend into '{seg}'")
    return value


class ScopedData(DotDict):
    """A DotDict overlay over a parent DotDict: reads fall through, writes stay local."""

    def __init__(self, parent: DotDict) -> None:
        super().__init__({})
        object.__setattr__(self, "_parent", parent)

    def __getattr__(self, key: str) -> Any:
        if key.startswith("_"):
            return object.__getattribute__(self, key)
        if key in self._data:
            value = self._data[key]
            return DotDict(value) if isinstance(value, dict) else value
        return getattr(self._parent, key)

    def __contains__(self, key: str) -> bool:
        return key in self._data or key in self._parent

    def __delattr__(self, key: str) -> None:
        # A delete reaches through to the parent for shared keys (``resume_data``)
        if key.startswith("_"):
            object.__delattr__(self, key)
        elif key in self._data:
            del self._data[key]
        elif key in self._parent:
            delattr(self._parent, key)

    def get(self, key: str, default: Any = None) -> Any:
        value = getattr(self, key, None)
        return default if value is None else value

    def keys(self) -> list[str]:
        seen = list(self._data)
        seen.extend(k for k in self._parent.keys() if k not in self._data)  # noqa: SIM118
        return seen

    def values(self) -> list[Any]:
        return [getattr(self, k) for k in self.keys()]

    def items(self) -> list[tuple[str, Any]]:
        return [(k, getattr(self, k)) for k in self.keys()]

    def to_dict(self) -> dict[str, Any]:
        merged: dict[str, Any] = dict(self._parent.to_dict())
        merged.update(self._data)
        return merged

    def overlay(self) -> dict[str, Any]:
        """What this scope wrote (and its bound inputs)."""
        return dict(self._data)

    def __repr__(self) -> str:
        return f"ScopedData({self._data} over {self._parent!r})"


class ScopedContext(FlowContext):
    """A FlowContext for one node: overlay data, shared metadata and input."""

    node_id: str
    parent: FlowContext

    def __init__(self, parent: FlowContext, node_id: str) -> None:
        super().__init__(
            data=ScopedData(parent.data), metadata=parent.metadata, input=parent.input
        )
        self.node_id = node_id
        self.parent = parent

    def overlay(self) -> dict[str, Any]:
        data = self.data
        assert isinstance(data, ScopedData)
        return data.overlay()


class BindingEvaluator:
    """Evaluate ``{ expr: ... }`` bindings over the namespaces (safe AST, no calls)."""

    def __init__(self) -> None:
        self._validator = SafeASTValidator()

    def evaluate(self, expr: str, parent: FlowContext) -> Any:
        try:
            tree = ast.parse(expr, mode="eval")
        except SyntaxError as exc:
            raise BindingError(f"Binding expression '{expr}': invalid syntax: {exc}") from exc
        if not self._validator.validate(tree):
            raise BindingError(
                f"Binding expression '{expr}' is not allowed: {self._validator.errors}"
            )
        namespace: dict[str, Any] = {
            k: (DotDict(v) if isinstance(v, dict) else v) for k, v in parent.data.items()
        }
        namespace.update({"context": parent, "True": True, "False": False, "None": None})
        try:
            return eval(compile(tree, "<binding>", "eval"), {"__builtins__": {}}, namespace)
        except Exception as exc:
            raise BindingError(f"Binding expression '{expr}' failed: {exc}") from exc


def resolve_source(
    src: BindingSource, parent: FlowContext, evaluator: BindingEvaluator | None = None
) -> Any:
    """Resolve one bound input against the parent context."""
    if isinstance(src, BindingLiteral):
        return src.value
    if isinstance(src, BindingExpr):
        return (evaluator or BindingEvaluator()).evaluate(src.expr, parent)
    head, path = parse_reference(src)
    if head not in parent.data:
        raise BindingError(
            f"Binding reference '{src}': namespace '{head}' is not available "
            f"(node not executed or no flow inputs)"
        )
    return walk_path(getattr(parent.data, head), path, src)


def set_nested(data: DotDict, target: str, value: Any) -> None:
    """``set_nested(data, "n_lookup.rows", v)`` → ``data.n_lookup.rows = v``."""
    head, path = parse_reference(target)
    raw = data._data
    ns = raw.setdefault(head, {})
    if not isinstance(ns, dict):
        raise BindingError(f"Output target '{target}': '{head}' is not a namespace")
    for seg in path[:-1]:
        ns = ns.setdefault(seg, {})
    ns[path[-1]] = value._data if isinstance(value, DotDict) else value


class BoundComponent(BaseComponent):
    """Executor-side wrapper applying a node's bindings around an inner component.

    Stateless; the executor keeps one per node. Works on both executor paths: when the
    inner ``process`` returns a coroutine, so does this one.
    """

    def __init__(
        self,
        inner: BaseComponent,
        node_id: str,
        binding: NodeBinding | None,
        *,
        mirror_raw: bool = False,
        evaluator: BindingEvaluator | None = None,
    ) -> None:
        super().__init__(inner.name)
        self.inner = inner
        self.node_id = node_id
        self.binding = binding or NodeBinding()
        self.mirror_raw = mirror_raw  # unbound node in a namespaced flow
        self._evaluator = evaluator or BindingEvaluator()
        self._config = inner.config
        self._initialized = inner.is_initialized

    # Delegate metadata and lifecycle to the inner component --------------------
    def get_meta(self) -> Any:
        return self.inner.get_meta()

    def init(self, config: dict[str, Any]) -> None:
        self.inner.init(config)
        self._config = self.inner.config
        self._initialized = True

    def validate_config(self) -> list[str]:
        return self.inner.validate_config()

    def health_check(self) -> bool:
        return self.inner.health_check()

    def setup(self, context: FlowContext) -> None:
        self.inner.setup(self._scope(context))

    def teardown(self, context: FlowContext) -> None:
        self.inner.teardown(self._scope(context))

    # The binding itself ----------------------------------------------------------
    def process(self, context: FlowContext) -> Any:
        scope = self._scope(context)
        preloaded = self._preload_own_namespace(context, scope)
        bound_inputs: dict[str, Any] = {}
        for key, src in self.binding.inputs.items():
            value = resolve_source(src, context, self._evaluator)
            bound_inputs[key] = value
            scope.set(key, value)
        result = self.inner.process(scope)
        if inspect.isawaitable(result):
            return self._finish_async(context, scope, bound_inputs, preloaded, result)
        self._merge(context, scope, bound_inputs, preloaded, result)
        return context

    def _preload_own_namespace(self, parent: FlowContext, scope: ScopedContext) -> set[str]:
        """Expose what the node wrote on earlier visits (loop state, agent history)."""
        own = parent.data._data.get(self.node_id)
        if not isinstance(own, dict):
            return set()
        loaded: set[str] = set()
        renamed_leaves: set[str] = set()
        for key, target in self.binding.outputs.items():
            _, path = parse_reference(target)
            renamed_leaves.add(path[0])
            try:
                scope.set(key, walk_path(own, path, target))
                loaded.add(key)
            except BindingError:
                continue  # not produced yet
        for key, value in own.items():
            if key not in renamed_leaves and key not in self.binding.outputs:
                scope.set(key, value)
                loaded.add(key)
        return loaded

    async def _finish_async(
        self,
        parent: FlowContext,
        scope: ScopedContext,
        bound_inputs: dict[str, Any],
        preloaded: set[str],
        pending: Awaitable[FlowContext],
    ) -> FlowContext:
        result = await pending
        self._merge(parent, scope, bound_inputs, preloaded, result)
        return parent

    def _scope(self, context: FlowContext) -> ScopedContext:
        # setup/process/teardown receive the same parent; one scope per call is
        # enough because the overlay is merged at the end of process
        if isinstance(context, ScopedContext) and context.node_id == self.node_id:
            return context
        return ScopedContext(context, self.node_id)

    def _merge(
        self,
        parent: FlowContext,
        scope: ScopedContext,
        bound_inputs: dict[str, Any],
        preloaded: set[str],
        result: FlowContext,
    ) -> None:
        written = scope.overlay() if result is scope else _overlay_of(result)
        for key in preloaded - set(written):
            target = self.binding.outputs.get(key, f"{self.node_id}.{key}")
            _, path = parse_reference(target)
            parent.data._data.get(self.node_id, {}).pop(path[0], None)
            if self.mirror_raw:
                parent.delete(key)
        for key, value in written.items():
            if key in bound_inputs and value is bound_inputs[key]:
                continue  # an untouched bound input is not an output
            target = self.binding.outputs.get(key, f"{self.node_id}.{key}")
            head, _ = parse_reference(target)
            if head != self.node_id:
                raise BindingError(
                    f"Node '{self.node_id}' may not write into namespace '{head}' "
                    f"(output target '{target}')"
                )
            set_nested(parent.data, target, value)
            if self.mirror_raw:
                parent.set(key, value)
        port = scope.get_active_port()
        if port is not None:
            parent.set_port(port)


def _overlay_of(result: FlowContext) -> dict[str, Any]:
    """A component that returned a different context: take its data as the overlay."""
    data = result.data
    return data.overlay() if isinstance(data, ScopedData) else data.to_dict()


def bind_components(
    nodes: list[GraphNodeConfig],
    components: dict[str, BaseComponent],
    evaluator: BindingEvaluator | None = None,
) -> dict[str, BaseComponent] | None:
    """Wrap every node's component for namespaced mode; ``None`` in legacy mode."""
    if not flow_is_namespaced(nodes):
        return None
    evaluator = evaluator or BindingEvaluator()
    bound: dict[str, BaseComponent] = {}
    for node in nodes:
        inner = components.get(node.component)
        if inner is None:
            continue  # the executor reports the missing component itself
        bound[node.id] = BoundComponent(
            inner, node.id, node.bind, mirror_raw=node.bind is None, evaluator=evaluator
        )
    return bound


def seed_flow_namespace(context: FlowContext, nodes: list[GraphNodeConfig]) -> None:
    """On a fresh namespaced run, snapshot the raw initial keys as ``flow`` inputs."""
    if FLOW_NAMESPACE in context.data:
        return
    node_ids = {n.id for n in nodes}
    context.data._data[FLOW_NAMESPACE] = {
        k: v for k, v in context.data._data.items() if k not in node_ids and k != "resume_data"
    }


__all__ = [
    "BindingError",
    "BindingEvaluator",
    "BoundComponent",
    "ScopedContext",
    "ScopedData",
    "bind_components",
    "flow_is_namespaced",
    "resolve_source",
    "seed_flow_namespace",
    "set_nested",
]
