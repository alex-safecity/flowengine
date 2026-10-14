"""Tests for data bindings / per-node namespaces (design/07, flowengine 0.7.0)."""

import pytest

from flowengine import BaseComponent, FlowContext
from flowengine.config.schema import (
    BindingExpr,
    BindingLiteral,
    FlowSettings,
    GraphEdgeConfig,
    GraphNodeConfig,
    NodeBinding,
)
from flowengine.core.binding import (
    BindingError,
    BoundComponent,
    ScopedContext,
    ScopedData,
    resolve_source,
)
from flowengine.core.context import DotDict
from flowengine.core.graph import GraphExecutor
from flowengine.errors import ComponentError

# ── components (untouched by bindings: flat get/set) ─────────────────────────

class Emit(BaseComponent):
    """Writes config['outputs'] (a dict) and records its name."""

    def process(self, context: FlowContext) -> FlowContext:
        for k, v in self.config.get("outputs", {}).items():
            context.set(k, v)
        context.set("order", [*context.get("order", []), self.name])
        return context


class Echo(BaseComponent):
    """Reads config['reads'] keys and writes them back as echo_<key>."""

    def process(self, context: FlowContext) -> FlowContext:
        for k in self.config.get("reads", []):
            context.set(f"echo_{k}", context.get(k))
        if self.config.get("port"):
            self.set_output_port(context, self.config["port"])
        return context


class Count(BaseComponent):
    def process(self, context: FlowContext) -> FlowContext:
        n = context.get("n", 0) + 1
        context.set("n", n)
        self.set_output_port(context, "again" if n < self.config.get("times", 2) else "done")
        return context


class Gate(BaseComponent):
    def process(self, context: FlowContext) -> FlowContext:
        data = context.get("resume_data")
        choice = getattr(data, "choice", None) if data is not None else None
        if choice is not None:
            context.set("choice", choice)
            context.delete("resume_data")
            self.set_output_port(context, "chosen")
            return context
        context.suspend(self.name, reason="waiting")
        return context


class AsyncEcho(Echo):
    async def process(self, context: FlowContext) -> FlowContext:  # type: ignore[override]
        return super().process(context)


def _inst(cls, name, **config):
    c = cls(name)
    c.init(config)
    return c


def _executor(nodes, edges, instances, settings=None):
    return GraphExecutor(
        nodes=[GraphNodeConfig(**n) for n in nodes],
        edges=[GraphEdgeConfig(**e) for e in edges],
        components=instances,
        settings=FlowSettings(**(settings or {"timeout_seconds": 60, "max_iterations": 10})),
    )


# ── ScopedContext ────────────────────────────────────────────────────────────

class TestScopedContext:
    def test_reads_fall_through_and_writes_stay_local(self):
        parent = FlowContext()
        parent.set("shared", 1)
        parent.set("n_a", {"x": 2})
        scope = ScopedContext(parent, "n_b")

        assert scope.get("shared") == 1
        assert scope.has("shared") and "shared" in scope.data
        assert scope.get("n_a").x == 2
        scope.set("mine", 3)
        assert scope.get("mine") == 3
        assert parent.get("mine") is None
        assert scope.overlay() == {"mine": 3}
        assert isinstance(scope.data, ScopedData)

    def test_metadata_and_input_are_shared(self):
        parent = FlowContext(input={"q": 1})
        scope = ScopedContext(parent, "n_b")
        scope.suspend("n_b", "why")
        assert parent.metadata.suspended and parent.metadata.suspended_at_node == "n_b"
        assert scope.input == {"q": 1}

    def test_delete_reaches_shared_keys(self):
        parent = FlowContext()
        parent.set("resume_data", {"choice": "x"})
        scope = ScopedContext(parent, "n_b")
        scope.delete("resume_data")
        assert not parent.has("resume_data")

    def test_keys_and_to_dict_merge(self):
        parent = FlowContext()
        parent.set("a", 1)
        scope = ScopedContext(parent, "n")
        scope.set("b", 2)
        assert set(scope.data.keys()) == {"a", "b"}
        assert scope.data.to_dict() == {"a": 1, "b": 2}


# ── resolve_source ───────────────────────────────────────────────────────────

class TestResolve:
    def _parent(self):
        p = FlowContext()
        p.set("flow", {"limit": 100})
        p.set("n_x", {"data": {"items": [{"id": "i0"}, {"id": "i1"}]}, "count": 2})
        return p

    def test_literal(self):
        assert resolve_source(BindingLiteral(value=[1, 2]), self._parent()) == [1, 2]

    def test_reference_key_and_path(self):
        p = self._parent()
        assert resolve_source("n_x.count", p) == 2
        assert resolve_source("flow.limit", p) == 100
        assert resolve_source("n_x.data.items.1.id", p) == "i1"

    def test_expression_sees_namespaces(self):
        p = self._parent()
        assert resolve_source(BindingExpr(expr="n_x.count + flow.limit"), p) == 102
        assert resolve_source(BindingExpr(expr="n_x.missing or 'dflt'"), p) == "dflt"

    def test_expression_allowlist(self):
        with pytest.raises(BindingError, match="not allowed"):
            resolve_source(BindingExpr(expr="__import__('os')"), self._parent())

    def test_missing_namespace_and_key(self):
        p = self._parent()
        with pytest.raises(BindingError, match="namespace 'n_nope'"):
            resolve_source("n_nope.k", p)
        with pytest.raises(BindingError, match="key 'nope'"):
            resolve_source("n_x.nope", p)
        with pytest.raises(BindingError, match="Invalid binding reference"):
            resolve_source("n_x", p)


# ── BoundComponent alone ─────────────────────────────────────────────────────

class TestBoundComponent:
    def test_inputs_in_outputs_namespaced(self):
        parent = FlowContext()
        parent.set("n_src", {"text": "hi"})
        bound = BoundComponent(
            _inst(Echo, "echo", reads=["text", "lim"]),
            "n_echo",
            NodeBinding(inputs={"text": "n_src.text", "lim": {"value": 5}}),
        )
        bound.process(parent)
        assert parent.get("n_echo").to_dict() == {"echo_text": "hi", "echo_lim": 5}
        assert parent.get("echo_text") is None  # nothing raw

    def test_output_rename_within_own_namespace(self):
        parent = FlowContext()
        parent.set("n_src", {"text": "hi"})
        bound = BoundComponent(
            _inst(Echo, "echo", reads=["text"]),
            "n_echo",
            NodeBinding(inputs={"text": "n_src.text"}, outputs={"echo_text": "n_echo.out"}),
        )
        bound.process(parent)
        assert parent.get("n_echo").to_dict() == {"out": "hi"}

    def test_foreign_output_target_is_rejected(self):
        parent = FlowContext()
        parent.set("n_src", {"text": "hi"})
        bound = BoundComponent(
            _inst(Echo, "echo", reads=["text"]),
            "n_echo",
            NodeBinding(inputs={"text": "n_src.text"}, outputs={"echo_text": "n_src.out"}),
        )
        with pytest.raises(BindingError, match="may not write into namespace 'n_src'"):
            bound.process(parent)

    def test_unbound_node_mirrors_raw_keys(self):
        parent = FlowContext()
        bound = BoundComponent(_inst(Emit, "emit", outputs={"k": 1}), "n_emit", None, mirror_raw=True)
        bound.process(parent)
        assert parent.get("n_emit").k == 1 and parent.get("k") == 1

    def test_port_propagates(self):
        parent = FlowContext()
        bound = BoundComponent(_inst(Echo, "e", port="yes"), "n_e", NodeBinding())
        bound.process(parent)
        assert parent.get_active_port() == "yes"

    def test_own_namespace_is_visible_on_the_next_visit(self):
        parent = FlowContext()
        bound = BoundComponent(_inst(Count, "c", times=3), "n_c", NodeBinding())
        for _ in range(3):
            bound.process(parent)
        assert parent.get("n_c").n == 3 and parent.get("n") is None

    def test_renamed_output_round_trips_to_the_next_visit(self):
        parent = FlowContext()
        bound = BoundComponent(
            _inst(Count, "c", times=3), "n_c", NodeBinding(outputs={"n": "n_c.count"})
        )
        bound.process(parent)
        bound.process(parent)
        assert parent.get("n_c").to_dict() == {"count": 2}

    def test_deleted_key_leaves_the_namespace(self):
        class Consume(BaseComponent):
            def process(self, context):
                context.delete("token")
                context.set("done", True)
                return context

        parent = FlowContext()
        parent.set("n_x", {"token": "t", "keep": 1})
        BoundComponent(_inst(Consume, "x"), "n_x", NodeBinding()).process(parent)
        assert parent.get("n_x").to_dict() == {"keep": 1, "done": True}

    def test_modified_bound_input_is_an_output(self):
        class Upper(BaseComponent):
            def process(self, context):
                context.set("text", context.get("text").upper())
                return context

        parent = FlowContext()
        parent.set("n_src", {"text": "hi"})
        bound = BoundComponent(_inst(Upper, "u"), "n_u", NodeBinding(inputs={"text": "n_src.text"}))
        bound.process(parent)
        assert parent.get("n_u").text == "HI"
        assert parent.get("n_src").text == "hi"


# ── GraphExecutor in namespaced mode ─────────────────────────────────────────

NODES = [
    {"id": "n_a", "component": "a"},
    {"id": "n_b", "component": "b", "bind": {"inputs": {"text": "n_a.text"}}},
    {"id": "n_c", "component": "c", "bind": {"inputs": {"text": "n_a.text"}}},
]
EDGES = [{"source": "n_a", "target": "n_b"}, {"source": "n_a", "target": "n_c"}]


def _two_echo():
    return {
        "a": _inst(Emit, "a", outputs={"text": "hello"}),
        "b": _inst(Echo, "b", reads=["text"]),
        "c": _inst(Echo, "c", reads=["text"]),
    }


class TestNamespacedExecution:
    def test_same_key_two_nodes_no_collision(self):
        ctx = _executor(NODES, EDGES, _two_echo()).execute(FlowContext())
        assert ctx.get("n_b").echo_text == "hello" and ctx.get("n_c").echo_text == "hello"
        assert ctx.get("n_a").text == "hello" and ctx.get("text") == "hello"  # unbound: mirrored

    def test_flow_namespace_seeded_from_initial_keys(self):
        ctx = FlowContext()
        ctx.set("limit", 7)
        nodes = [{"id": "n_a", "component": "a", "bind": {"inputs": {"lim": "flow.limit"}}}]
        inst = {"a": _inst(Echo, "a", reads=["lim"])}
        out = _executor(nodes, [], inst).execute(ctx)
        assert out.get("flow").limit == 7 and out.get("n_a").echo_lim == 7

    def test_legacy_flow_is_not_wrapped(self):
        nodes = [{"id": "n_a", "component": "a"}, {"id": "n_b", "component": "b"}]
        ex = _executor(nodes, [{"source": "n_a", "target": "n_b"}], _two_echo())
        assert ex._bound is None
        ctx = ex.execute(FlowContext())
        assert ctx.data.to_dict() == {"text": "hello", "order": ["a"], "echo_text": "hello"}

    def test_unresolvable_binding_follows_on_error(self):
        nodes = [
            {"id": "n_a", "component": "a"},
            {"id": "n_b", "component": "b", "bind": {"inputs": {"text": "n_a.nope"}}},
        ]
        with pytest.raises(ComponentError, match="key 'nope'"):
            _executor(nodes, [{"source": "n_a", "target": "n_b"}], _two_echo()).execute(FlowContext())
        nodes[1]["on_error"] = "skip"
        ctx = _executor(
            nodes, [{"source": "n_a", "target": "n_b"}], _two_echo(),
            settings={"timeout_seconds": 60, "fail_fast": False},
        ).execute(FlowContext())
        assert "b" in ctx.metadata.skipped_components

    def test_ports_cycles_and_max_visits(self):
        nodes = [
            {"id": "n_count", "component": "count", "max_visits": 5, "bind": {}},
            {"id": "n_done", "component": "done", "bind": {"inputs": {"n": "n_count.n"}}},
        ]
        edges = [
            {"source": "n_count", "target": "n_count", "port": "again"},
            {"source": "n_count", "target": "n_done", "port": "done"},
        ]
        inst = {"count": _inst(Count, "count", times=3), "done": _inst(Echo, "done", reads=["n"])}
        ctx = _executor(nodes, edges, inst).execute(FlowContext())
        assert ctx.get("n_count").n == 3 and ctx.get("n_done").echo_n == 3
        assert ctx.metadata.node_visit_counts["n_count"] == 3

    def test_suspend_and_resume_with_resume_data(self):
        nodes = [
            {"id": "n_a", "component": "a"},
            {"id": "n_gate", "component": "gate", "bind": {}},
            {"id": "n_b", "component": "b", "bind": {"inputs": {"text": "n_gate.choice"}}},
        ]
        edges = [
            {"source": "n_a", "target": "n_gate"},
            {"source": "n_gate", "target": "n_b", "port": "chosen"},
        ]
        inst = {**_two_echo(), "gate": _inst(Gate, "gate")}
        first = _executor(nodes, edges, inst).execute(FlowContext())
        assert first.metadata.suspended_at_node == "n_gate"
        restored = FlowContext.from_dict(first.to_dict())
        restored.set("resume_data", {"choice": "fine-1"})
        out = _executor(nodes, edges, inst).execute(restored)
        assert not out.metadata.suspended
        assert out.get("n_gate").choice == "fine-1" and out.get("n_b").echo_text == "fine-1"
        assert not out.has("resume_data")
        assert out.get("flow").to_dict() == {}  # seeded once, before the first node

    @pytest.mark.asyncio
    async def test_async_components_are_bound_too(self):
        inst = {
            "a": _inst(Emit, "a", outputs={"text": "hello"}),
            "b": _inst(AsyncEcho, "b", reads=["text"]),
            "c": _inst(AsyncEcho, "c", reads=["text"]),
        }
        ctx = await _executor(NODES, EDGES, inst).execute_async(FlowContext())
        assert ctx.get("n_b").echo_text == "hello" and ctx.get("n_c").echo_text == "hello"

    def test_hooks_still_see_component_names(self):
        seen = []

        class Hook:
            def on_node_start(self, node_id, component, context):
                seen.append((node_id, component))

        ex = GraphExecutor(
            nodes=[GraphNodeConfig(**n) for n in NODES],
            edges=[GraphEdgeConfig(**e) for e in EDGES],
            components=_two_echo(),
            settings=FlowSettings(timeout_seconds=60),
            hooks=[Hook()],
        )
        ex.execute(FlowContext())
        assert seen == [("n_a", "a"), ("n_b", "b"), ("n_c", "c")]


class TestSchema:
    def test_binding_models_parse_yaml_shapes(self):
        node = GraphNodeConfig(
            id="n_lookup",
            component="lookup",
            bind={
                "inputs": {
                    "sql_params": "n_extract.data.incident_id",
                    "sql": {"value": "SELECT 1"},
                    "limit": {"expr": "flow.limit or 100"},
                },
                "outputs": {"rows": "n_lookup.rows"},
            },
            provenance={"sql_params": "system_of_record"},
        )
        assert isinstance(node.bind.inputs["sql"], BindingLiteral)
        assert isinstance(node.bind.inputs["limit"], BindingExpr)
        assert node.bind.inputs["sql_params"] == "n_extract.data.incident_id"
        assert node.provenance == {"sql_params": "system_of_record"}

    def test_round_trip(self):
        node = GraphNodeConfig(id="n", component="c", bind={"inputs": {"k": {"value": 1}}})
        again = GraphNodeConfig(**node.model_dump())
        assert again == node
        assert isinstance(DotDict({"a": 1}).a, int)
