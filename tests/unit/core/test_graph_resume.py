"""Tests for GraphExecutor resume semantics (0.7.0).

A suspended graph resumes at the node that suspended: completed nodes are
skipped and propagate the port they set when they ran (``metadata.node_ports``),
the cyclic executor re-enqueues the suspended node and whatever was still queued
(``metadata.pending_nodes``) — never the roots — and a suspended execution is not
counted as a visit. The restored context round-trips through ``to_dict``.
"""

import pytest

from flowengine import BaseComponent, FlowContext
from flowengine.config.schema import FlowSettings, GraphEdgeConfig, GraphNodeConfig
from flowengine.core.graph import GraphExecutor
from flowengine.errors import FlowExecutionError


class Append(BaseComponent):
    def process(self, context: FlowContext) -> FlowContext:
        context.set("order", [*context.get("order", []), self.name])
        return context


class Route(BaseComponent):
    """Sets the configured port and records itself."""

    def process(self, context: FlowContext) -> FlowContext:
        self.set_output_port(context, self.config.get("port", "yes"))
        context.set("order", [*context.get("order", []), self.name])
        return context


class Count(BaseComponent):
    """Loops ``times`` times on port 'again', then exits on 'done'."""

    def process(self, context: FlowContext) -> FlowContext:
        n = context.get("n", 0) + 1
        context.set("n", n)
        context.set("order", [*context.get("order", []), self.name])
        self.set_output_port(context, "again" if n < self.config.get("times", 2) else "done")
        return context


class Gate(BaseComponent):
    """Suspends until resume_data carries a choice, then exits on 'chosen'."""

    def process(self, context: FlowContext) -> FlowContext:
        context.set("order", [*context.get("order", []), self.name])
        data = context.get("resume_data")
        choice = getattr(data, "choice", None) if data is not None else None
        if choice is not None:
            context.set("choice", choice)
            context.delete("resume_data")
            self.set_output_port(context, "chosen")
            return context
        context.suspend(self.name, reason="waiting")
        return context


class AsyncAppend(BaseComponent):
    async def process(self, context: FlowContext) -> FlowContext:
        context.set("order", [*context.get("order", []), self.name])
        return context


def _executor(nodes, edges, instances, settings=None) -> GraphExecutor:
    for inst in instances.values():
        inst.init(getattr(inst, "config", {}) or {})
    return GraphExecutor(
        nodes=[GraphNodeConfig(**n) for n in nodes],
        edges=[GraphEdgeConfig(**e) for e in edges],
        components=instances,
        settings=FlowSettings(**(settings or {"timeout_seconds": 60, "max_iterations": 10})),
    )


def _instances(**kinds):
    out = {}
    for name, (cls, cfg) in kinds.items():
        inst = cls(name)
        inst.init(cfg)
        out[name] = inst
    return out


def _round_trip(context: FlowContext) -> FlowContext:
    """What a run store does between the two processes."""
    return FlowContext.from_dict(context.to_dict())


# ── acyclic: port behind a completed node ────────────────────────────────────

ACYCLIC_NODES = [
    {"id": "n_router", "component": "router"},
    {"id": "n_gate", "component": "gate"},
    {"id": "n_mark", "component": "mark"},
    {"id": "n_other", "component": "other"},
]
ACYCLIC_EDGES = [
    {"source": "n_router", "target": "n_gate", "port": "yes"},
    {"source": "n_router", "target": "n_other", "port": "no"},
    {"source": "n_gate", "target": "n_mark", "port": "chosen"},
]


def _acyclic():
    return _executor(
        ACYCLIC_NODES,
        ACYCLIC_EDGES,
        _instances(
            router=(Route, {"port": "yes"}), gate=(Gate, {}), mark=(Append, {}), other=(Append, {})
        ),
    )


class TestDagResume:
    def test_suspension_records_node_id_and_ports(self):
        result = _acyclic().execute(FlowContext())
        assert result.metadata.suspended is True
        assert result.metadata.suspended_at_node == "n_gate"  # node id, not "gate"
        assert result.metadata.completed_nodes == ["n_router"]
        assert result.metadata.node_ports == {"n_router": "yes"}

    def test_resume_reaches_the_node_behind_the_port(self):
        first = _acyclic().execute(FlowContext())
        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "fine-1"})

        result = _acyclic().execute(restored)  # resume node taken from the context

        assert result.metadata.suspended is False
        assert result.get("choice") == "fine-1"
        assert result.get("order") == ["router", "gate", "gate", "mark"]
        assert "gate" not in result.metadata.skipped_components
        assert result.metadata.node_ports == {"n_router": "yes", "n_gate": "chosen", "n_mark": None}

    def test_explicit_resume_node_wins(self):
        first = _acyclic().execute(FlowContext())
        restored = _round_trip(first)
        restored.metadata.suspended_at_node = None  # a caller that cleared it (pre-0.7 style)
        restored.set("resume_data", {"choice": "x"})

        result = _acyclic().execute(restored, resume_node="n_gate")
        assert result.get("order") == ["router", "gate", "gate", "mark"]

    def test_unknown_resume_node_is_rejected(self):
        with pytest.raises(FlowExecutionError, match="unknown node"):
            _acyclic().execute(FlowContext(), resume_node="n_nope")

    def test_pre_07_component_name_is_mapped_to_its_node(self):
        first = _acyclic().execute(FlowContext())
        restored = _round_trip(first)
        restored.metadata.suspended_at_node = "gate"  # what 0.6 contexts carry
        restored.set("resume_data", {"choice": "x"})
        result = _acyclic().execute(restored)
        assert result.get("order")[-1] == "mark"

    def test_router_does_not_rerun_on_resume(self):
        first = _acyclic().execute(FlowContext())
        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "x"})
        result = _acyclic().execute(restored)
        assert result.get("order").count("router") == 1

    @pytest.mark.asyncio
    async def test_async_twin(self):
        inst = _instances(
            router=(Route, {"port": "yes"}),
            gate=(Gate, {}),
            mark=(AsyncAppend, {}),
            other=(AsyncAppend, {}),
        )
        ex = _executor(ACYCLIC_NODES, ACYCLIC_EDGES, inst)
        first = await ex.execute_async(FlowContext())
        assert first.metadata.suspended_at_node == "n_gate"
        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "x"})
        result = await _executor(ACYCLIC_NODES, ACYCLIC_EDGES, inst).execute_async(restored)
        assert result.get("order") == ["router", "gate", "gate", "mark"]


# ── cyclic: re-entry at the suspended node ───────────────────────────────────

CYCLIC_NODES = [
    {"id": "n_router", "component": "router"},
    {"id": "n_counter", "component": "counter", "max_visits": 5},
    {"id": "n_gate", "component": "gate"},
    {"id": "n_mark", "component": "mark"},
    {"id": "n_other", "component": "other"},
]
CYCLIC_EDGES = [
    {"source": "n_router", "target": "n_counter", "port": "yes"},
    {"source": "n_router", "target": "n_other", "port": "no"},
    {"source": "n_counter", "target": "n_counter", "port": "again"},
    {"source": "n_counter", "target": "n_gate", "port": "done"},
    {"source": "n_gate", "target": "n_mark", "port": "chosen"},
]


def _cyclic():
    return _executor(
        CYCLIC_NODES,
        CYCLIC_EDGES,
        _instances(
            router=(Route, {"port": "yes"}),
            counter=(Count, {"times": 2}),
            gate=(Gate, {}),
            mark=(Append, {}),
            other=(Append, {}),
        ),
    )


class TestCyclicResume:
    def test_suspension_is_not_a_visit(self):
        result = _cyclic().execute(FlowContext())
        assert result.metadata.suspended_at_node == "n_gate"
        assert result.metadata.node_visit_counts == {"n_router": 1, "n_counter": 2}
        assert result.metadata.pending_nodes == []
        assert result.get("n") == 2

    def test_resume_continues_at_the_gate_only(self):
        first = _cyclic().execute(FlowContext())
        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "fine-1"})

        result = _cyclic().execute(restored)

        assert result.metadata.suspended is False
        assert result.get("choice") == "fine-1"
        assert result.get("n") == 2, "the counter must not run again"
        assert result.metadata.node_visit_counts == {
            "n_router": 1, "n_counter": 2, "n_gate": 1, "n_mark": 1
        }
        assert result.get("order") == ["router", "counter", "counter", "gate", "gate", "mark"]
        assert result.metadata.iteration_count == first.metadata.iteration_count

    def test_gate_with_max_visits_one_can_resume(self):
        nodes = [dict(n, max_visits=1) if n["id"] == "n_gate" else n for n in CYCLIC_NODES]
        inst = _instances(
            router=(Route, {"port": "yes"}),
            counter=(Count, {"times": 2}),
            gate=(Gate, {}),
            mark=(Append, {}),
            other=(Append, {}),
        )
        first = _executor(nodes, CYCLIC_EDGES, inst).execute(FlowContext())
        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "x"})
        result = _executor(nodes, CYCLIC_EDGES, inst).execute(restored)
        assert result.get("order")[-1] == "mark"

    def test_pending_fan_out_survives_suspension(self):
        # router --yes--> gate and router --yes--> side; side is queued behind
        # the gate when it suspends and must still run after the resume.
        nodes = [
            {"id": "n_router", "component": "router"},
            {"id": "n_gate", "component": "gate"},
            {"id": "n_side", "component": "side"},
            {"id": "n_loop", "component": "loop"},
            {"id": "n_mark", "component": "mark"},
        ]
        edges = [
            {"source": "n_router", "target": "n_gate", "port": "yes"},
            {"source": "n_router", "target": "n_side", "port": "yes"},
            {"source": "n_gate", "target": "n_mark", "port": "chosen"},
            {"source": "n_side", "target": "n_loop"},
            {"source": "n_loop", "target": "n_loop", "port": "again"},
        ]
        inst = _instances(
            router=(Route, {"port": "yes"}),
            gate=(Gate, {}),
            side=(Append, {}),
            loop=(Count, {"times": 2}),
            mark=(Append, {}),
        )
        first = _executor(nodes, edges, inst).execute(FlowContext())
        assert first.metadata.suspended_at_node == "n_gate"
        assert first.metadata.pending_nodes == ["n_side"]

        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "x"})
        result = _executor(nodes, edges, inst).execute(restored)
        assert result.metadata.suspended is False
        assert result.metadata.pending_nodes == []
        assert result.get("order") == ["router", "gate", "gate", "side", "mark", "loop", "loop"]

    def test_resume_at_back_edge_target_does_not_double_count_iterations(self):
        # gate inside the loop: counter --again--> gate --chosen--> counter
        nodes = [
            {"id": "n_counter", "component": "counter"},
            {"id": "n_gate", "component": "gate"},
            {"id": "n_mark", "component": "mark"},
        ]
        edges = [
            {"source": "n_counter", "target": "n_gate", "port": "again"},
            {"source": "n_gate", "target": "n_counter", "port": "chosen"},
            {"source": "n_counter", "target": "n_mark", "port": "done"},
        ]
        inst = _instances(counter=(Count, {"times": 3}), gate=(Gate, {}), mark=(Append, {}))
        settings = {"timeout_seconds": 60, "max_iterations": 10}
        ctx = FlowContext()
        for choice in ("a", "b"):
            ctx = _executor(nodes, edges, inst, settings).execute(ctx)
            assert ctx.metadata.suspended_at_node == "n_gate"
            ctx = _round_trip(ctx)
            ctx.set("resume_data", {"choice": choice})
        result = _executor(nodes, edges, inst, settings).execute(ctx)
        assert result.metadata.suspended is False
        assert result.get("n") == 3
        assert result.metadata.node_visit_counts == {"n_counter": 3, "n_gate": 2, "n_mark": 1}
        assert result.metadata.iteration_count == 2  # two re-entries of the loop

    @pytest.mark.asyncio
    async def test_async_twin(self):
        inst = _instances(
            router=(Route, {"port": "yes"}),
            counter=(Count, {"times": 2}),
            gate=(Gate, {}),
            mark=(AsyncAppend, {}),
            other=(AsyncAppend, {}),
        )
        first = await _executor(CYCLIC_NODES, CYCLIC_EDGES, inst).execute_async(FlowContext())
        assert first.metadata.node_visit_counts == {"n_router": 1, "n_counter": 2}
        restored = _round_trip(first)
        restored.set("resume_data", {"choice": "x"})
        result = await _executor(CYCLIC_NODES, CYCLIC_EDGES, inst).execute_async(restored)
        assert result.get("n") == 2
        assert result.get("order") == ["router", "counter", "counter", "gate", "gate", "mark"]


class TestSerialisation:
    def test_new_fields_round_trip(self):
        ctx = FlowContext()
        ctx.metadata.node_ports = {"n_a": "yes", "n_b": None}
        ctx.metadata.pending_nodes = ["n_c"]
        back = FlowContext.from_dict(ctx.to_dict())
        assert back.metadata.node_ports == {"n_a": "yes", "n_b": None}
        assert back.metadata.pending_nodes == ["n_c"]

    def test_pre_07_dict_loads_with_defaults(self):
        d = FlowContext().to_dict()
        d["metadata"].pop("node_ports")
        d["metadata"].pop("pending_nodes")
        back = FlowContext.from_dict(d)
        assert back.metadata.node_ports == {} and back.metadata.pending_nodes == []


class TestErrorPort:
    """The implicit, exclusive error port a runtime guard routes on."""

    def _executor(self, inst, extra_edges=()):
        from flowengine.core.graph import ERROR_PORT

        nodes = [
            {"id": "n_g", "component": "g"},
            {"id": "n_x", "component": "x"},
            {"id": "n_fb", "component": "fb"},
        ]
        edges = [{"source": "n_g", "target": "n_x"}, *extra_edges]
        return _executor(nodes, edges, inst), ERROR_PORT

    def test_error_port_takes_only_error_edges(self):
        class Fail(BaseComponent):
            def process(self, context):
                self.set_output_port(context, "error")
                return context

        inst = _instances(g=(Fail, {}), x=(Append, {}), fb=(Append, {}))
        ex, port = self._executor(inst, [{"source": "n_g", "target": "n_fb", "port": "error"}])
        ctx = ex.execute(FlowContext())
        assert ctx.get("order") == ["fb"]
        assert ctx.metadata.node_ports["n_g"] == "error"
        assert "x" in ctx.metadata.skipped_components

    def test_error_port_without_an_error_edge_halts_downstream(self):
        class Fail(BaseComponent):
            def process(self, context):
                self.set_output_port(context, "error")
                return context

        inst = _instances(g=(Fail, {}), x=(Append, {}), fb=(Append, {}))
        # fb hangs off x (not an error edge): nothing downstream of the block runs
        ex, _ = self._executor(inst, [{"source": "n_x", "target": "n_fb"}])
        ctx = ex.execute(FlowContext())
        assert ctx.get("order") is None
        assert {"x", "fb"} <= set(ctx.metadata.skipped_components)

    def test_current_node_is_visible_to_components(self):
        seen = []

        class Peek(BaseComponent):
            def process(self, context):
                seen.append(context.current_node)
                return context

        inst = _instances(g=(Peek, {}), x=(Peek, {}), fb=(Append, {}))
        ex, _ = self._executor(inst)
        ex.execute(FlowContext())
        assert seen == ["n_g", "n_x"]
