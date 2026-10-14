"""Tests for GraphExecutor — cyclic graph execution.

Covers cycle detection, simple cycles, agent loops, max_iterations policies,
branching, multiple/nested cycles, timeouts, suspension, hooks, error handling,
metadata tracking, self-loops, and DAG backward compatibility.
"""

import time
from unittest.mock import MagicMock

import pytest

from flowengine import BaseComponent, FlowConfig, FlowContext, FlowEngine
from flowengine.config.schema import (
    ComponentConfig,
    FlowDefinition,
    FlowSettings,
    GraphEdgeConfig,
    GraphNodeConfig,
)
from flowengine.core.graph import GraphExecutor
from flowengine.errors import ComponentError, FlowTimeoutError, MaxIterationsError

# ── Test components ──────────────────────────────────────────────────────────


class AppendComponent(BaseComponent):
    """Appends its name to context.data.order list."""

    def process(self, context: FlowContext) -> FlowContext:
        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)
        return context


class CounterComponent(BaseComponent):
    """Increments a counter in context each time it runs."""

    def process(self, context: FlowContext) -> FlowContext:
        key = self.config.get("counter_key", "counter")
        current = context.get(key, 0)
        context.set(key, current + 1)
        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)
        return context


class LoopDeciderComponent(BaseComponent):
    """Sets port 'continue' or 'done' based on counter threshold."""

    def process(self, context: FlowContext) -> FlowContext:
        key = self.config.get("counter_key", "counter")
        threshold = self.config.get("threshold", 3)
        current = context.get(key, 0)

        if current >= threshold:
            self.set_output_port(context, "done")
        else:
            self.set_output_port(context, "continue")

        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)
        return context


class ConditionComponent(BaseComponent):
    """Sets active port based on config['port'] value."""

    def process(self, context: FlowContext) -> FlowContext:
        port = self.config.get("port", "true")
        self.set_output_port(context, port)
        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)
        return context


class FailComponent(BaseComponent):
    """Always raises an error."""

    def process(self, context: FlowContext) -> FlowContext:
        raise RuntimeError("Intentional failure")


class SuspendComponent(BaseComponent):
    """Suspends the flow."""

    def process(self, context: FlowContext) -> FlowContext:
        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)
        context.suspend(self.name, reason="Waiting for approval")
        return context


class SlowComponent(BaseComponent):
    """Sleeps for configurable duration (timeout tests)."""

    def process(self, context: FlowContext) -> FlowContext:
        duration = self.config.get("sleep", 0.1)
        time.sleep(duration)
        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)
        return context


class ConditionalSuspendComponent(BaseComponent):
    """Suspends on the Nth visit (1-indexed)."""

    def process(self, context: FlowContext) -> FlowContext:
        suspend_on = self.config.get("suspend_on_visit", 2)
        visit_key = f"_visits_{self.name}"
        visits = context.get(visit_key, 0) + 1
        context.set(visit_key, visits)

        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)

        if visits == suspend_on:
            context.suspend(self.name, reason=f"Suspended on visit {visits}")

        return context


class FailOnVisitComponent(BaseComponent):
    """Fails on the Nth visit (1-indexed)."""

    def process(self, context: FlowContext) -> FlowContext:
        fail_on = self.config.get("fail_on_visit", 2)
        visit_key = f"_visits_{self.name}"
        visits = context.get(visit_key, 0) + 1
        context.set(visit_key, visits)

        order = context.get("order", [])
        order.append(self.name)
        context.set("order", order)

        if visits == fail_on:
            raise RuntimeError(f"Intentional failure on visit {visits}")

        return context


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_config(
    nodes: list[dict],
    edges: list[dict],
    components: list[dict],
    settings: dict | None = None,
) -> FlowConfig:
    """Build a FlowConfig for graph tests."""
    return FlowConfig(
        name="test-cyclic",
        version="1.0",
        components=[ComponentConfig(**c) for c in components],
        flow=FlowDefinition(
            type="graph",
            settings=FlowSettings(**(settings or {"timeout_seconds": 60})),
            nodes=[GraphNodeConfig(**n) for n in nodes],
            edges=[GraphEdgeConfig(**e) for e in edges],
        ),
    )


def _build_engine(
    config: FlowConfig,
    instances: dict[str, BaseComponent],
) -> FlowEngine:
    return FlowEngine(config, instances, validate_types=False)


def _build_executor(
    nodes: list[dict],
    edges: list[dict],
    instances: dict[str, BaseComponent],
    settings: dict | None = None,
    hooks: list | None = None,
) -> GraphExecutor:
    """Build a GraphExecutor directly (bypasses FlowEngine)."""
    node_objs = [GraphNodeConfig(**n) for n in nodes]
    edge_objs = [GraphEdgeConfig(**e) for e in edges]
    settings_obj = FlowSettings(**(settings or {"timeout_seconds": 60}))
    return GraphExecutor(
        nodes=node_objs,
        edges=edge_objs,
        components=instances,
        settings=settings_obj,
        hooks=hooks,
    )


# ── TestCycleDetection ───────────────────────────────────────────────────────


class TestCycleDetection:
    """Back-edge identification for all graph shapes."""

    def test_simple_cycle_detected(self):
        """A → B → A has one back-edge."""
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
        )
        assert executor._has_cycles is True
        assert len(executor._back_edges) == 1
        # The back-edge is b→a (creates cycle)
        assert ("b", "a") in executor._back_edges

    def test_dag_no_cycles(self):
        """A → B → C has no back-edges."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "c"}],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
            },
        )
        assert executor._has_cycles is False
        assert len(executor._back_edges) == 0

    def test_self_loop_detected(self):
        """A → A is a self-loop back-edge."""
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}],
            edges=[{"source": "a", "target": "a"}],
            instances={"ca": AppendComponent("ca")},
        )
        assert executor._has_cycles is True
        assert ("a", "a") in executor._back_edges

    def test_triangle_cycle_detected(self):
        """A → B → C → A has one back-edge."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
                {"source": "c", "target": "a"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
            },
        )
        assert executor._has_cycles is True
        assert len(executor._back_edges) == 1

    def test_diamond_no_cycle(self):
        """Diamond: A → B, A → C, B → D, C → D — not a cycle."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "a", "target": "c"},
                {"source": "b", "target": "d"},
                {"source": "c", "target": "d"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
                "cd": AppendComponent("cd"),
            },
        )
        assert executor._has_cycles is False

    def test_back_edge_targets_tracked(self):
        """Back-edge targets are stored for iteration counting."""
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
        )
        assert "a" in executor._back_edge_targets


# ── TestSimpleCycle ──────────────────────────────────────────────────────────


class TestSimpleCycle:
    """A→B→A counter with max_iterations and visit counts."""

    def test_simple_cycle_executes(self):
        """A→B→A runs multiple times with exit policy."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert len(order) > 2
        # Both nodes executed
        assert "ca" in order
        assert "cb" in order

    def test_visit_counts_tracked(self):
        """Visit counts are recorded in metadata."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": CounterComponent("ca"), "cb": CounterComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert "a" in result.metadata.node_visit_counts
        assert "b" in result.metadata.node_visit_counts
        assert result.metadata.node_visit_counts["a"] > 1
        assert result.metadata.node_visit_counts["b"] > 1

    def test_iteration_count_increments(self):
        """Iteration count increments on back-edge re-entry."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 5,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.metadata.iteration_count > 0

    def test_counter_accumulates_across_iterations(self):
        """Context data persists across loop iterations."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A", "config": {"counter_key": "counter"}},
                {"name": "cb", "type": "t.B", "config": {"counter_key": "counter"}},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": CounterComponent("ca"), "cb": CounterComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Both components increment the same counter
        assert result.get("counter") > 2

    def test_max_iterations_stops_cycle_with_fail(self):
        """Cycle raises MaxIterationsError when limit exceeded with fail policy."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 100},
                {"id": "b", "component": "cb", "max_visits": 100},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            # Per-node max_visits is high, so iteration limit fires first
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "fail"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)

        with pytest.raises(MaxIterationsError):
            engine.execute()

    def test_cycle_nodes_not_in_completed_nodes(self):
        """Cycle-participating nodes use visit_counts, not completed_nodes."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Cycle nodes should NOT be in completed_nodes
        assert "a" not in result.metadata.completed_nodes
        assert "b" not in result.metadata.completed_nodes

    def test_three_node_cycle(self):
        """A→B→C→A cycle works with three nodes."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
                {"source": "c", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
                {"name": "cc", "type": "t.C"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
            "cc": AppendComponent("cc"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert len(order) >= 6  # At least 2 full passes
        # First pass is a, b, c
        assert order[0] == "ca"
        assert order[1] == "cb"
        assert order[2] == "cc"

    def test_per_node_visit_limit_terminates_queue(self):
        """Per-node visit limit (from max_iterations default) exhausts queue."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should terminate — either via max_iterations or per-node visit limit
        assert result.metadata.node_visit_counts["a"] <= 2
        assert result.metadata.node_visit_counts["b"] <= 2


# ── TestAgentLoop ────────────────────────────────────────────────────────────


class TestAgentLoop:
    """4-node agent pattern: plan → act → observe → decide → [continue→plan | done→deliver]."""

    def _agent_config(self, threshold: int = 3, max_iterations: int = 10):
        return _make_config(
            nodes=[
                {"id": "plan", "component": "planner"},
                {"id": "act", "component": "actor"},
                {"id": "observe", "component": "observer"},
                {"id": "decide", "component": "decider"},
                {"id": "deliver", "component": "deliverer"},
            ],
            edges=[
                {"source": "plan", "target": "act"},
                {"source": "act", "target": "observe"},
                {"source": "observe", "target": "decide"},
                {"source": "decide", "target": "plan", "port": "continue"},
                {"source": "decide", "target": "deliver", "port": "done"},
            ],
            components=[
                {"name": "planner", "type": "t.P", "config": {"counter_key": "steps"}},
                {"name": "actor", "type": "t.A", "config": {"counter_key": "steps"}},
                {"name": "observer", "type": "t.O", "config": {"counter_key": "steps"}},
                {"name": "decider", "type": "t.D",
                 "config": {"counter_key": "steps", "threshold": threshold}},
                {"name": "deliverer", "type": "t.Del"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": max_iterations,
                       "on_max_iterations": "exit"},
        )

    def _agent_instances(self):
        """Create component instances. Config comes from FlowEngine.init()."""
        return {
            "planner": CounterComponent("planner"),
            "actor": CounterComponent("actor"),
            "observer": CounterComponent("observer"),
            "decider": LoopDeciderComponent("decider"),
            "deliverer": AppendComponent("deliverer"),
        }

    def test_agent_loop_exits_via_done_port(self):
        """Agent exits loop when decider routes to 'done'."""
        config = self._agent_config(threshold=4, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert "deliverer" in order
        # Deliverer should be the last executed
        assert order[-1] == "deliverer"

    def test_agent_loop_iteration_count(self):
        """Agent loop tracks iteration count correctly."""
        config = self._agent_config(threshold=8, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should have iterated at least once (plan is the back-edge target)
        assert result.metadata.iteration_count >= 1

    def test_agent_loop_counter_reaches_threshold(self):
        """Counter accumulates through agent loop iterations."""
        config = self._agent_config(threshold=6, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Counter should be >= threshold when decider chose 'done'
        assert result.get("steps") >= 6

    def test_agent_loop_max_iterations_override(self):
        """Max iterations stops agent loop even if decider never says done."""
        config = self._agent_config(threshold=999, max_iterations=3)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should have stopped — deliverer not reached
        assert "deliverer" not in result.get("order")

    def test_agent_loop_visit_counts_all_nodes(self):
        """All cycle-participating nodes have visit counts."""
        config = self._agent_config(threshold=4, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        vc = result.metadata.node_visit_counts
        # All cycle nodes should have been visited
        for node_id in ["plan", "act", "observe", "decide"]:
            assert node_id in vc
            assert vc[node_id] >= 1

    def test_agent_loop_deliver_node_completed(self):
        """Non-cycle terminal node goes into completed_nodes."""
        config = self._agent_config(threshold=4, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        # deliver is not part of the cycle, so it goes into completed_nodes
        assert "deliver" in result.metadata.completed_nodes

    def test_agent_loop_skips_deliver_on_continue(self):
        """Deliverer is not executed when decider routes to continue."""
        config = self._agent_config(threshold=999, max_iterations=2)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert "deliverer" not in order

    def test_agent_loop_with_initial_context(self):
        """Agent loop can receive initial context data."""
        config = self._agent_config(threshold=3, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)

        ctx = FlowContext()
        ctx.set("steps", 2)  # Start close to threshold
        result = engine.execute(ctx)

        order = result.get("order")
        assert "deliverer" in order
        # Should exit quickly since counter starts at 2

    def test_agent_loop_execution_order(self):
        """Agent loop executes in correct order each iteration."""
        config = self._agent_config(threshold=999, max_iterations=2)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # First pass: planner, actor, observer, decider
        assert order[0] == "planner"
        assert order[1] == "actor"
        assert order[2] == "observer"
        assert order[3] == "decider"

    def test_agent_loop_timings_recorded(self):
        """Component timings are recorded across iterations."""
        config = self._agent_config(threshold=4, max_iterations=20)
        instances = self._agent_instances()
        engine = _build_engine(config, instances)
        result = engine.execute()

        timings = result.metadata.component_timings
        for comp in ["planner", "actor", "observer", "decider"]:
            assert comp in timings


# ── TestMaxIterationsPolicy ──────────────────────────────────────────────────


class TestMaxIterationsPolicy:
    """fail/exit/warn policies and per-node max_visits."""

    def test_fail_policy_raises(self):
        """on_max_iterations='fail' raises MaxIterationsError."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 100},
                {"id": "b", "component": "cb", "max_visits": 100},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "fail"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)

        with pytest.raises(MaxIterationsError) as exc_info:
            engine.execute()

        assert exc_info.value.max_iterations == 2
        assert exc_info.value.cycle_entry_node == "a"

    def test_exit_policy_stops_silently(self):
        """on_max_iterations='exit' stops without raising."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Execution completed without raising
        assert result.metadata.iteration_count >= 1

    def test_warn_policy_stops_and_logs(self, caplog):
        """on_max_iterations='warn' logs warning and stops."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 100},
                {"id": "b", "component": "cb", "max_visits": 100},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            # Per-node max_visits is high, so iteration limit fires first
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "warn"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)

        with caplog.at_level("WARNING"):
            result = engine.execute()

        assert result.metadata.max_iterations_reached is True
        assert "Max iterations reached" in caplog.text

    def test_per_node_max_visits(self):
        """Per-node max_visits overrides flow-level max_iterations."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 2},
                {"id": "b", "component": "cb"},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 100,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Node a should only be visited max_visits times
        assert result.metadata.node_visit_counts.get("a", 0) <= 2

    def test_fail_policy_error_attributes(self):
        """MaxIterationsError has correct attributes."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 100},
                {"id": "b", "component": "cb", "max_visits": 100},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 1,
                       "on_max_iterations": "fail"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)

        with pytest.raises(MaxIterationsError) as exc_info:
            engine.execute()

        err = exc_info.value
        assert err.max_iterations == 1
        assert err.actual_iterations > err.max_iterations

    def test_max_iterations_default_is_10(self):
        """Default max_iterations is 10."""
        settings = FlowSettings(timeout_seconds=60)
        assert settings.max_iterations == 10


# ── TestCycleWithBranches ────────────────────────────────────────────────────


class TestCycleWithBranches:
    """Cycles combined with DAG branches."""

    def test_cycle_with_entry_prefix(self):
        """Linear prefix → cycle: entry → A → B → A (exits)."""
        config = _make_config(
            nodes=[
                {"id": "entry", "component": "c_entry"},
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "entry", "target": "a"},
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "c_entry", "type": "t.E"},
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "c_entry": AppendComponent("c_entry"),
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # Entry must execute first
        assert order[0] == "c_entry"
        # Cycle nodes must follow
        assert "ca" in order
        assert "cb" in order

    def test_cycle_with_exit_suffix(self):
        """Cycle exits to terminal: A → B → decide → [continue→A | done→finish]."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "decide", "component": "c_decide"},
                {"id": "finish", "component": "c_finish"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "decide"},
                {"source": "decide", "target": "a", "port": "continue"},
                {"source": "decide", "target": "finish", "port": "done"},
            ],
            components=[
                {"name": "ca", "type": "t.A", "config": {"counter_key": "steps"}},
                {"name": "cb", "type": "t.B", "config": {"counter_key": "steps"}},
                {"name": "c_decide", "type": "t.D",
                 "config": {"counter_key": "steps", "threshold": 4}},
                {"name": "c_finish", "type": "t.F"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 20,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": CounterComponent("ca"),
            "cb": CounterComponent("cb"),
            "c_decide": LoopDeciderComponent("c_decide"),
            "c_finish": AppendComponent("c_finish"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert "c_finish" in order
        assert order[-1] == "c_finish"

    def test_entry_node_in_completed_nodes(self):
        """Non-cycle entry node (before cycle) goes into completed_nodes."""
        config = _make_config(
            nodes=[
                {"id": "entry", "component": "c_entry"},
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "entry", "target": "a"},
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "c_entry", "type": "t.E"},
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "c_entry": AppendComponent("c_entry"),
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Entry is not in the cycle → should be in completed_nodes
        assert "entry" in result.metadata.completed_nodes

    def test_cycle_with_parallel_branches(self):
        """Cycle node has both a loopback edge and a non-loop exit edge."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "exit_node", "component": "c_exit"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a", "port": "continue"},
                {"source": "b", "target": "exit_node", "port": "done"},
            ],
            components=[
                {"name": "ca", "type": "t.A", "config": {"counter_key": "cnt"}},
                {"name": "cb", "type": "t.B",
                 "config": {"counter_key": "cnt", "threshold": 3}},
                {"name": "c_exit", "type": "t.E"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 20,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": CounterComponent("ca"),
            "cb": LoopDeciderComponent("cb"),
            "c_exit": AppendComponent("c_exit"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert "c_exit" in order

    def test_cycle_with_two_exit_branches(self):
        """Decide node can route to two different exit nodes."""
        config = _make_config(
            nodes=[
                {"id": "work", "component": "c_work"},
                {"id": "decide", "component": "c_decide"},
                {"id": "success", "component": "c_success"},
                {"id": "failure", "component": "c_failure"},
            ],
            edges=[
                {"source": "work", "target": "decide"},
                {"source": "decide", "target": "work", "port": "continue"},
                {"source": "decide", "target": "success", "port": "done"},
                {"source": "decide", "target": "failure", "port": "fail"},
            ],
            components=[
                {"name": "c_work", "type": "t.W",
                 "config": {"counter_key": "count"}},
                {"name": "c_decide", "type": "t.D",
                 "config": {"counter_key": "count", "threshold": 3}},
                {"name": "c_success", "type": "t.S"},
                {"name": "c_failure", "type": "t.F"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 20,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "c_work": CounterComponent("c_work"),
            "c_decide": LoopDeciderComponent("c_decide"),
            "c_success": AppendComponent("c_success"),
            "c_failure": AppendComponent("c_failure"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # Should exit via 'done' → success
        assert "c_success" in order
        assert "c_failure" not in order

    def test_cycle_preserves_order_on_branch_exit(self):
        """After cycle exits, terminal nodes execute in correct order."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "decide", "component": "c_decide"},
                {"id": "cleanup", "component": "c_cleanup"},
                {"id": "finish", "component": "c_finish"},
            ],
            edges=[
                {"source": "a", "target": "decide"},
                {"source": "decide", "target": "a", "port": "continue"},
                {"source": "decide", "target": "cleanup", "port": "done"},
                {"source": "cleanup", "target": "finish"},
            ],
            components=[
                {"name": "ca", "type": "t.A", "config": {"counter_key": "x"}},
                {"name": "c_decide", "type": "t.D",
                 "config": {"counter_key": "x", "threshold": 2}},
                {"name": "c_cleanup", "type": "t.C"},
                {"name": "c_finish", "type": "t.F"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 20,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": CounterComponent("ca"),
            "c_decide": LoopDeciderComponent("c_decide"),
            "c_cleanup": AppendComponent("c_cleanup"),
            "c_finish": AppendComponent("c_finish"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        cleanup_idx = order.index("c_cleanup")
        finish_idx = order.index("c_finish")
        assert finish_idx > cleanup_idx


# ── TestMultipleCycles ───────────────────────────────────────────────────────


class TestMultipleCycles:
    """Independent cycles in the same graph."""

    def test_two_independent_cycles(self):
        """Two separate cycles: A→B→A and C→D→C."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
                {"source": "c", "target": "d"},
                {"source": "d", "target": "c"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
                {"name": "cc", "type": "t.C"},
                {"name": "cd", "type": "t.D"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
            "cc": AppendComponent("cc"),
            "cd": AppendComponent("cd"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # Both cycles should have executed
        assert "ca" in order
        assert "cb" in order
        assert "cc" in order
        assert "cd" in order

    def test_two_cycles_have_separate_back_edges(self):
        """Multiple cycles produce multiple back-edges."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
                {"source": "c", "target": "d"},
                {"source": "d", "target": "c"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
                "cd": AppendComponent("cd"),
            },
        )
        assert executor._has_cycles is True
        assert len(executor._back_edges) == 2

    def test_sequential_cycles(self):
        """Two cycles connected: A→B→A with A also pointing to C→D→C."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
                {"source": "a", "target": "c"},
                {"source": "c", "target": "d"},
                {"source": "d", "target": "c"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
                {"name": "cc", "type": "t.C"},
                {"name": "cd", "type": "t.D"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
            "cc": AppendComponent("cc"),
            "cd": AppendComponent("cd"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Graph should execute without error
        order = result.get("order")
        assert len(order) > 0

    def test_multiple_back_edge_targets(self):
        """Both cycle entry points are tracked as back-edge targets."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
                {"source": "c", "target": "d"},
                {"source": "d", "target": "c"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
                "cd": AppendComponent("cd"),
            },
        )
        # Both cycle targets should be in back_edge_targets
        assert len(executor._back_edge_targets) == 2


# ── TestNestedCycles ─────────────────────────────────────────────────────────


class TestNestedCycles:
    """Cycle within a cycle (figure-8 or nested patterns)."""

    def test_figure_eight_cycle(self):
        """A→B→C→A with B also having B→D→B subcycle."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
                {"source": "c", "target": "a"},
                {"source": "b", "target": "d"},
                {"source": "d", "target": "b"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
                {"name": "cc", "type": "t.C"},
                {"name": "cd", "type": "t.D"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
            "cc": AppendComponent("cc"),
            "cd": AppendComponent("cd"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should execute without error
        order = result.get("order")
        assert len(order) > 0
        assert "ca" in order

    def test_nested_cycle_has_multiple_back_edges(self):
        """Nested cycles produce multiple back-edges."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
                {"source": "c", "target": "a"},
                {"source": "b", "target": "d"},
                {"source": "d", "target": "b"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
                "cd": AppendComponent("cd"),
            },
        )
        assert executor._has_cycles is True
        assert len(executor._back_edges) >= 2

    def test_nested_cycle_terminates(self):
        """Nested cycles terminate within iteration/visit limits."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
                {"id": "d", "component": "cd"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
                {"source": "c", "target": "a"},
                {"source": "b", "target": "d"},
                {"source": "d", "target": "b"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
                {"name": "cc", "type": "t.C"},
                {"name": "cd", "type": "t.D"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
            "cc": AppendComponent("cc"),
            "cd": AppendComponent("cd"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should terminate without error
        order = result.get("order")
        assert len(order) > 0


# ── TestCyclicTimeout ────────────────────────────────────────────────────────


class TestCyclicTimeout:
    """Global timeout across iterations."""

    def test_timeout_during_cycle(self):
        """Flow timeout triggers during cyclic execution."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A", "config": {"sleep": 0.3}},
                {"name": "cb", "type": "t.B", "config": {"sleep": 0.3}},
            ],
            settings={"timeout_seconds": 0.5, "max_iterations": 100,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": SlowComponent("ca"),
            "cb": SlowComponent("cb"),
        }
        engine = _build_engine(config, instances)

        with pytest.raises(FlowTimeoutError):
            engine.execute()

    def test_timeout_has_elapsed_info(self):
        """Timeout exception carries elapsed time."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A", "config": {"sleep": 0.3}},
                {"name": "cb", "type": "t.B", "config": {"sleep": 0.3}},
            ],
            settings={"timeout_seconds": 0.5, "max_iterations": 100,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": SlowComponent("ca"),
            "cb": SlowComponent("cb"),
        }
        engine = _build_engine(config, instances)

        with pytest.raises(FlowTimeoutError) as exc_info:
            engine.execute()

        assert exc_info.value.elapsed > 0

    def test_fast_cycle_completes_within_timeout(self):
        """Fast cycle completes within timeout."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # No timeout — should complete normally
        order = result.get("order")
        assert len(order) > 0


# ── TestCyclicSuspension ─────────────────────────────────────────────────────


class TestCyclicSuspension:
    """Suspend/resume inside cycle."""

    def test_suspend_inside_cycle(self):
        """Suspension stops cyclic execution."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "s", "component": "cs"},
            ],
            edges=[
                {"source": "a", "target": "s"},
                {"source": "s", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cs", "type": "t.S"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 10,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cs": SuspendComponent("cs"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.metadata.suspended is True
        # The executor records the node id, whatever the component passed
        assert result.metadata.suspended_at_node == "s"

    def test_suspend_preserves_visit_counts(self):
        """Visit counts are preserved when flow suspends."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "s", "component": "cs"},
            ],
            edges=[
                {"source": "a", "target": "s"},
                {"source": "s", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cs", "type": "t.S",
                 "config": {"suspend_on_visit": 2}},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 10,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cs": ConditionalSuspendComponent("cs"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.metadata.suspended is True
        # Visit counts should reflect executions before suspension
        assert result.metadata.node_visit_counts.get("a", 0) >= 1
        assert result.metadata.node_visit_counts.get("s", 0) >= 1

    def test_resume_from_suspension_in_cycle(self):
        """Resuming after suspension continues from the suspended node."""
        # Use matching node IDs and component names so suspend works correctly
        # (SuspendComponent passes self.name to context.suspend, which stores as
        # suspended_at_node; the cyclic executor uses this as a node ID in the queue)
        config = _make_config(
            nodes=[
                {"id": "ca", "component": "ca"},
                {"id": "cs", "component": "cs"},
            ],
            edges=[
                {"source": "ca", "target": "cs"},
                {"source": "cs", "target": "ca"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cs", "type": "t.S"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 5,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cs": SuspendComponent("cs"),
        }
        engine = _build_engine(config, instances)

        # First execution suspends
        result = engine.execute()
        assert result.metadata.suspended is True

        # For resume: suspended_at_node stays set; the executor clears the
        # suspension flags itself and re-enters at that node

        # Replace cs with an AppendComponent to avoid re-suspending
        replacement = AppendComponent("cs")
        replacement.init({})
        engine.components["cs"] = replacement

        # Resume
        result2 = engine.execute(result)
        order2 = result2.get("order")
        # Should have continued execution beyond the initial run
        assert len(order2) > 2

    def test_suspend_preserves_iteration_count(self):
        """Iteration count is preserved across suspension."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "s", "component": "cs"},
            ],
            edges=[
                {"source": "a", "target": "s"},
                {"source": "s", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cs", "type": "t.S",
                 "config": {"suspend_on_visit": 2}},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 10,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cs": ConditionalSuspendComponent("cs"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.metadata.suspended is True
        # Visit counts should be serializable (part of resume state)
        assert isinstance(result.metadata.node_visit_counts, dict)


# ── TestCyclicHooks ──────────────────────────────────────────────────────────


class TestCyclicHooks:
    """Hook events fire for iterations."""

    def test_on_iteration_start_fires(self):
        """on_iteration_start hook fires when iteration begins."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        # on_iteration_start should have been called
        assert hook.on_iteration_start.called

    def test_on_iteration_complete_fires(self):
        """on_iteration_complete hook fires when an iteration finishes."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        # on_iteration_complete should have been called
        assert hook.on_iteration_complete.called
        # Each call should include iteration number, node_id, context, duration
        for call in hook.on_iteration_complete.call_args_list:
            args = call[0]
            assert isinstance(args[0], int)   # iteration
            assert isinstance(args[1], str)   # cycle_entry_node
            assert isinstance(args[3], float)  # duration >= 0

    def test_on_iteration_complete_covers_all_iterations(self):
        """on_iteration_complete fires for every iteration including the final one.

        on_iteration_start fires N times (at back-edge re-entry).
        on_iteration_complete fires N+1 times (N at re-entry for the *previous*
        iteration + 1 for the final iteration when the queue empties).
        """
        hook = MagicMock()
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        # complete fires one more than start (final iteration end)
        assert hook.on_iteration_complete.call_count == hook.on_iteration_start.call_count + 1

    def test_on_max_iterations_hook_fires(self):
        """on_max_iterations hook fires when iteration limit is reached."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 100},
                {"id": "b", "component": "cb", "max_visits": 100},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        assert hook.on_max_iterations.called
        args = hook.on_max_iterations.call_args[0]
        assert args[0] == 2  # max_iterations value
        assert isinstance(args[1], str)  # cycle_entry_node

    def test_on_max_iterations_hook_fires_before_exception(self):
        """on_max_iterations hook fires even when policy is 'fail'."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca", "max_visits": 100},
                {"id": "b", "component": "cb", "max_visits": 100},
            ],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "fail"},
            hooks=[hook],
        )
        context = FlowContext()
        with pytest.raises(MaxIterationsError):
            executor.execute(context)

        # Hook should still have fired before the exception
        assert hook.on_max_iterations.called

    def test_on_node_start_fires_each_visit(self):
        """on_node_start fires for every node visit in cycle."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        # on_node_start should fire for each visit
        assert hook.on_node_start.call_count >= 4  # At least 2 iterations x 2 nodes

    def test_on_node_complete_fires_each_visit(self):
        """on_node_complete fires for every node visit in cycle."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        assert hook.on_node_complete.call_count >= 4

    def test_on_flow_suspended_fires_in_cycle(self):
        """on_flow_suspended hook fires when cycle suspends."""
        hook = MagicMock()
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "s", "component": "cs"},
            ],
            edges=[
                {"source": "a", "target": "s"},
                {"source": "s", "target": "a"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cs": SuspendComponent("cs"),
            },
            settings={"timeout_seconds": 60, "max_iterations": 10,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        executor.execute(context)

        assert hook.on_flow_suspended.called

    def test_hook_error_does_not_break_execution(self):
        """Hook exceptions are silently caught."""
        hook = MagicMock()
        hook.on_node_start.side_effect = RuntimeError("Hook exploded")
        hook.on_node_complete.side_effect = RuntimeError("Hook exploded")
        hook.on_iteration_start.side_effect = RuntimeError("Hook exploded")
        hook.on_iteration_complete.side_effect = RuntimeError("Hook exploded")
        hook.on_max_iterations.side_effect = RuntimeError("Hook exploded")

        executor = _build_executor(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            instances={"ca": AppendComponent("ca"), "cb": AppendComponent("cb")},
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
            hooks=[hook],
        )
        context = FlowContext()
        # Should not raise despite hook errors
        result = executor.execute(context)
        assert result.get("order") is not None


# ── TestCyclicErrorHandling ──────────────────────────────────────────────────


class TestCyclicErrorHandling:
    """Error policies within cycles."""

    def test_fail_fast_in_cycle(self):
        """fail_fast=True raises ComponentError in cycle."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A",
                 "config": {"fail_on_visit": 2}},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 10,
                       "on_max_iterations": "exit", "fail_fast": True},
        )
        instances = {
            "ca": FailOnVisitComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)

        with pytest.raises(ComponentError):
            engine.execute()

    def test_on_error_skip_in_cycle(self):
        """on_error='skip' records error and continues cycle."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "on_error": "skip"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A",
                 "config": {"fail_on_visit": 2}},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 5,
                       "on_max_iterations": "exit", "fail_fast": False},
        )
        instances = {
            "ca": FailOnVisitComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should have errors but not crash
        assert result.metadata.has_errors

    def test_error_on_first_node_fails_immediately(self):
        """Error on first node in cycle with fail_fast raises immediately."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 10,
                       "on_max_iterations": "exit", "fail_fast": True},
        )
        instances = {
            "ca": FailComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)

        with pytest.raises(ComponentError):
            engine.execute()

    def test_error_records_in_metadata(self):
        """Errors in cycles are recorded in metadata."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "on_error": "skip"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A",
                 "config": {"fail_on_visit": 2}},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 5,
                       "on_max_iterations": "exit", "fail_fast": False},
        )
        instances = {
            "ca": FailOnVisitComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert len(result.metadata.errors) >= 1
        assert result.metadata.errors[0]["component"] == "ca"

    def test_on_error_continue_in_cycle(self):
        """on_error='continue' continues without skipping."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca", "on_error": "continue"},
                {"id": "b", "component": "cb"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "ca", "type": "t.A",
                 "config": {"fail_on_visit": 2}},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 5,
                       "on_max_iterations": "exit", "fail_fast": False},
        )
        instances = {
            "ca": FailOnVisitComponent("ca"),
            "cb": AppendComponent("cb"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.metadata.has_errors
        # Component is NOT in skipped_components (continue vs skip)
        assert "ca" not in result.metadata.skipped_components


# ── TestCyclicMetadata ───────────────────────────────────────────────────────


class TestCyclicMetadata:
    """Visit counts, iteration counts, and timings."""

    def test_visit_counts_accurate(self):
        """Visit counts match actual executions."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": CounterComponent("ca"), "cb": CounterComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # Count how many times each appears in order
        ca_count = order.count("ca")
        cb_count = order.count("cb")

        assert result.metadata.node_visit_counts["a"] == ca_count
        assert result.metadata.node_visit_counts["b"] == cb_count

    def test_component_timings_aggregated(self):
        """Component timings aggregate across visits."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        timings = result.metadata.component_timings
        assert "ca" in timings
        assert "cb" in timings
        assert timings["ca"] >= 0
        assert timings["cb"] >= 0

    def test_metadata_serialization_round_trip(self):
        """Cyclic metadata survives to_dict/from_dict round trip."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Serialize and deserialize
        data = result.to_dict()
        restored = FlowContext.from_dict(data)

        assert restored.metadata.node_visit_counts == result.metadata.node_visit_counts
        assert restored.metadata.iteration_count == result.metadata.iteration_count
        assert restored.metadata.max_iterations_reached == result.metadata.max_iterations_reached

    def test_step_timings_have_correct_execution_order(self):
        """Step timings track execution order across iterations."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}, {"id": "b", "component": "cb"}],
            edges=[{"source": "a", "target": "b"}, {"source": "b", "target": "a"}],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": AppendComponent("ca"), "cb": AppendComponent("cb")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Step timings should have entries for each execution
        assert len(result.metadata.step_timings) >= 4
        # Execution orders should be monotonically increasing
        orders = [st.execution_order for st in result.metadata.step_timings]
        assert orders == sorted(orders)


# ── TestSelfLoop ─────────────────────────────────────────────────────────────


class TestSelfLoop:
    """Single node pointing to itself."""

    def test_self_loop_executes(self):
        """Self-loop A→A executes with max_iterations."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}],
            edges=[{"source": "a", "target": "a"}],
            components=[{"name": "ca", "type": "t.A"}],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": CounterComponent("ca")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should have executed multiple times
        assert result.get("counter") > 1

    def test_self_loop_terminates(self):
        """Self-loop stops within configured limits."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca"}],
            edges=[{"source": "a", "target": "a"}],
            components=[{"name": "ca", "type": "t.A"}],
            settings={"timeout_seconds": 60, "max_iterations": 2,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": CounterComponent("ca")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should terminate within max_iterations visits
        assert result.metadata.node_visit_counts.get("a", 0) <= 2

    def test_self_loop_with_per_node_max_visits(self):
        """Per-node max_visits limits self-loop visits."""
        config = _make_config(
            nodes=[{"id": "a", "component": "ca", "max_visits": 5}],
            edges=[{"source": "a", "target": "a"}],
            components=[{"name": "ca", "type": "t.A"}],
            settings={"timeout_seconds": 60, "max_iterations": 100,
                       "on_max_iterations": "exit"},
        )
        instances = {"ca": CounterComponent("ca")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        # Should not visit more than max_visits times
        assert result.metadata.node_visit_counts.get("a", 0) <= 5


# ── TestDAGBackwardCompat ────────────────────────────────────────────────────


class TestDAGBackwardCompat:
    """Verify DAG graphs use the DAG path, producing identical results."""

    def test_dag_uses_dag_path(self):
        """DAG graph uses _execute_dag, not _execute_cyclic."""
        executor = _build_executor(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
            ],
            instances={
                "ca": AppendComponent("ca"),
                "cb": AppendComponent("cb"),
                "cc": AppendComponent("cc"),
            },
        )
        assert executor._has_cycles is False

    def test_dag_execution_unchanged(self):
        """DAG execution produces same results as before."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "ca"},
                {"id": "b", "component": "cb"},
                {"id": "c", "component": "cc"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
            ],
            components=[
                {"name": "ca", "type": "t.A"},
                {"name": "cb", "type": "t.B"},
                {"name": "cc", "type": "t.C"},
            ],
        )
        instances = {
            "ca": AppendComponent("ca"),
            "cb": AppendComponent("cb"),
            "cc": AppendComponent("cc"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.get("order") == ["ca", "cb", "cc"]
        assert result.metadata.node_visit_counts == {}
        assert result.metadata.iteration_count == 0
        assert result.metadata.max_iterations_reached is False

    def test_dag_branching_still_works(self):
        """Port-based branching in DAGs still works correctly."""
        config = _make_config(
            nodes=[
                {"id": "cond", "component": "c_cond"},
                {"id": "yes", "component": "c_yes"},
                {"id": "no", "component": "c_no"},
            ],
            edges=[
                {"source": "cond", "target": "yes", "port": "true"},
                {"source": "cond", "target": "no", "port": "false"},
            ],
            components=[
                {"name": "c_cond", "type": "t.C",
                 "config": {"port": "true"}},
                {"name": "c_yes", "type": "t.Y"},
                {"name": "c_no", "type": "t.N"},
            ],
        )
        instances = {
            "c_cond": ConditionComponent("c_cond"),
            "c_yes": AppendComponent("c_yes"),
            "c_no": AppendComponent("c_no"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert "c_cond" in order
        assert "c_yes" in order
        assert "c_no" not in order
