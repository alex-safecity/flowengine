"""Tests for GraphExecutor — DAG-based flow execution."""

import pytest

from flowengine import (
    BaseComponent,
    FlowConfig,
    FlowContext,
    FlowEngine,
)
from flowengine.core.graph import GraphExecutor
from flowengine.config.schema import (
    ComponentConfig,
    FlowDefinition,
    FlowSettings,
    GraphEdgeConfig,
    GraphNodeConfig,
)
from flowengine.errors import ComponentError, ConfigurationError


# ── Test components ──────────────────────────────────────────────────────────


class AppendComponent(BaseComponent):
    """Appends its name to context.data.order list."""

    def process(self, context: FlowContext) -> FlowContext:
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


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_config(
    nodes: list[dict],
    edges: list[dict],
    components: list[dict],
    settings: dict | None = None,
) -> FlowConfig:
    """Build a FlowConfig for graph tests."""
    return FlowConfig(
        name="test-graph",
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
    config: FlowConfig, instances: dict[str, BaseComponent]
) -> FlowEngine:
    return FlowEngine(config, instances, validate_types=False)


# ── Tests ────────────────────────────────────────────────────────────────────


class TestGraphLinear:
    """Linear graph: A → B → C"""

    def test_linear_execution_order(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_b"},
                {"id": "c", "component": "comp_c"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
            ],
            components=[
                {"name": "comp_a", "type": "test.AppendComponent"},
                {"name": "comp_b", "type": "test.AppendComponent"},
                {"name": "comp_c", "type": "test.AppendComponent"},
            ],
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_b": AppendComponent("comp_b"),
            "comp_c": AppendComponent("comp_c"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.get("order") == ["comp_a", "comp_b", "comp_c"]

    def test_single_node_graph(self):
        config = _make_config(
            nodes=[{"id": "only", "component": "comp_only"}],
            edges=[],
            components=[
                {"name": "comp_only", "type": "test.AppendComponent"},
            ],
        )
        instances = {"comp_only": AppendComponent("comp_only")}
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.get("order") == ["comp_only"]


class TestGraphBranching:
    """Branching with port-based routing."""

    def _branching_config(self, port: str):
        return _make_config(
            nodes=[
                {"id": "cond", "component": "condition"},
                {"id": "on_true", "component": "branch_true"},
                {"id": "on_false", "component": "branch_false"},
            ],
            edges=[
                {"source": "cond", "target": "on_true", "port": "true"},
                {"source": "cond", "target": "on_false", "port": "false"},
            ],
            components=[
                {"name": "condition", "type": "test.Cond", "config": {"port": port}},
                {"name": "branch_true", "type": "test.Append"},
                {"name": "branch_false", "type": "test.Append"},
            ],
        )

    def _branching_instances(self, port: str):
        return {
            "condition": ConditionComponent("condition"),
            "branch_true": AppendComponent("branch_true"),
            "branch_false": AppendComponent("branch_false"),
        }

    def test_routes_to_true_branch(self):
        config = self._branching_config("true")
        engine = _build_engine(config, self._branching_instances("true"))
        result = engine.execute()

        assert "condition" in result.get("order")
        assert "branch_true" in result.get("order")
        assert "branch_false" not in result.get("order")

    def test_routes_to_false_branch(self):
        config = self._branching_config("false")
        engine = _build_engine(config, self._branching_instances("false"))
        result = engine.execute()

        assert "condition" in result.get("order")
        assert "branch_false" in result.get("order")
        assert "branch_true" not in result.get("order")

    def test_unreachable_node_skipped(self):
        config = self._branching_config("true")
        engine = _build_engine(config, self._branching_instances("true"))
        result = engine.execute()

        assert "branch_false" in result.metadata.skipped_components


class TestGraphDiamond:
    """Diamond pattern: A → B, A → C, B → D, C → D"""

    def test_diamond_executes_all(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_b"},
                {"id": "c", "component": "comp_c"},
                {"id": "d", "component": "comp_d"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "a", "target": "c"},
                {"source": "b", "target": "d"},
                {"source": "c", "target": "d"},
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_b", "type": "t.B"},
                {"name": "comp_c", "type": "t.C"},
                {"name": "comp_d", "type": "t.D"},
            ],
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_b": AppendComponent("comp_b"),
            "comp_c": AppendComponent("comp_c"),
            "comp_d": AppendComponent("comp_d"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # A must be first, D must be last
        assert order[0] == "comp_a"
        assert order[-1] == "comp_d"
        # B and C both executed (order between them may vary)
        assert "comp_b" in order
        assert "comp_c" in order


class TestGraphMultipleRoots:
    """Multiple roots (parallel entry points)."""

    def test_multiple_roots_both_execute(self):
        config = _make_config(
            nodes=[
                {"id": "r1", "component": "root1"},
                {"id": "r2", "component": "root2"},
                {"id": "join", "component": "joiner"},
            ],
            edges=[
                {"source": "r1", "target": "join"},
                {"source": "r2", "target": "join"},
            ],
            components=[
                {"name": "root1", "type": "t.R1"},
                {"name": "root2", "type": "t.R2"},
                {"name": "joiner", "type": "t.J"},
            ],
        )
        instances = {
            "root1": AppendComponent("root1"),
            "root2": AppendComponent("root2"),
            "joiner": AppendComponent("joiner"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        assert "root1" in order
        assert "root2" in order
        assert "joiner" in order
        # joiner must come after both roots
        assert order.index("joiner") > order.index("root1")
        assert order.index("joiner") > order.index("root2")


class TestGraphDefaultEdges:
    """Edges with port=None always activate."""

    def test_unconditional_edges_always_activate(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_b"},
            ],
            edges=[
                {"source": "a", "target": "b"},  # port=None (unconditional)
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_b", "type": "t.B"},
            ],
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_b": AppendComponent("comp_b"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.get("order") == ["comp_a", "comp_b"]


class TestGraphValidation:
    """Schema and cycle validation."""

    def test_empty_nodes_raises(self):
        with pytest.raises(Exception):
            FlowDefinition(type="graph", nodes=[], edges=[])

    def test_cycle_executes_with_max_iterations(self):
        """Previously rejected as 'Cycle detected', now executes with loop limit."""
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_b"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "a"},
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_b", "type": "t.B"},
            ],
            settings={"timeout_seconds": 60, "max_iterations": 3,
                       "on_max_iterations": "exit"},
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_b": AppendComponent("comp_b"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        order = result.get("order")
        # Should have run more than one pass
        assert len(order) > 2
        assert result.metadata.iteration_count <= 3

    def test_edge_references_missing_node(self):
        with pytest.raises(Exception, match="not found in nodes"):
            _make_config(
                nodes=[{"id": "a", "component": "comp_a"}],
                edges=[{"source": "a", "target": "ghost"}],
                components=[{"name": "comp_a", "type": "t.A"}],
            )

    def test_node_references_missing_component(self):
        with pytest.raises(Exception, match="undefined component"):
            _make_config(
                nodes=[{"id": "a", "component": "nonexistent"}],
                edges=[],
                components=[{"name": "comp_a", "type": "t.A"}],
            )

    def test_duplicate_node_ids(self):
        with pytest.raises(Exception, match="Duplicate node IDs"):
            _make_config(
                nodes=[
                    {"id": "a", "component": "comp_a"},
                    {"id": "a", "component": "comp_a"},
                ],
                edges=[],
                components=[{"name": "comp_a", "type": "t.A"}],
            )


class TestGraphErrorHandling:
    """Error handling in graph execution."""

    def test_fail_fast_stops_on_error(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_fail"},
                {"id": "c", "component": "comp_c"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_fail", "type": "t.F"},
                {"name": "comp_c", "type": "t.C"},
            ],
            settings={"fail_fast": True, "timeout_seconds": 60},
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_fail": FailComponent("comp_fail"),
            "comp_c": AppendComponent("comp_c"),
        }
        engine = _build_engine(config, instances)
        with pytest.raises(ComponentError):
            engine.execute()

    def test_on_error_skip_continues(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_fail", "on_error": "skip"},
                {"id": "c", "component": "comp_c"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_fail", "type": "t.F"},
                {"name": "comp_c", "type": "t.C"},
            ],
            settings={"fail_fast": False, "timeout_seconds": 60},
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_fail": FailComponent("comp_fail"),
            "comp_c": AppendComponent("comp_c"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert "comp_fail" in result.metadata.skipped_components
        # comp_c still runs because edges from b to c are unconditional
        # and c is reachable via the activation from b's outgoing edges
        # (the error skip doesn't activate downstream in current impl,
        #  but c won't be reachable since b errored and didn't activate edges)
        assert result.metadata.has_errors


class TestGraphSuspension:
    """Suspension (pause/resume) in graph execution."""

    def test_suspend_stops_execution(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "s", "component": "comp_suspend"},
                {"id": "c", "component": "comp_c"},
            ],
            edges=[
                {"source": "a", "target": "s"},
                {"source": "s", "target": "c"},
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_suspend", "type": "t.S"},
                {"name": "comp_c", "type": "t.C"},
            ],
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_suspend": SuspendComponent("comp_suspend"),
            "comp_c": AppendComponent("comp_c"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        assert result.metadata.suspended is True
        # The executor records the node id, whatever the component passed
        assert result.metadata.suspended_at_node == "s"
        assert "comp_c" not in result.get("order")


class TestGraphCompletedNodes:
    """Resume support — skipping already-completed nodes."""

    def test_skips_completed_nodes(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_b"},
                {"id": "c", "component": "comp_c"},
            ],
            edges=[
                {"source": "a", "target": "b"},
                {"source": "b", "target": "c"},
            ],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_b", "type": "t.B"},
                {"name": "comp_c", "type": "t.C"},
            ],
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_b": AppendComponent("comp_b"),
            "comp_c": AppendComponent("comp_c"),
        }
        engine = _build_engine(config, instances)

        # Simulate resume: a and b already completed
        context = FlowContext()
        context.metadata.completed_nodes = ["a", "b"]

        result = engine.execute(context)
        order = result.get("order")
        # Only c should execute (a is root so it's activated but skipped as completed)
        assert "comp_a" not in order
        assert "comp_b" not in order
        assert "comp_c" in order


class TestGraphTimings:
    """Timing and metadata tracking."""

    def test_records_timings_for_all_nodes(self):
        config = _make_config(
            nodes=[
                {"id": "a", "component": "comp_a"},
                {"id": "b", "component": "comp_b"},
            ],
            edges=[{"source": "a", "target": "b"}],
            components=[
                {"name": "comp_a", "type": "t.A"},
                {"name": "comp_b", "type": "t.B"},
            ],
        )
        instances = {
            "comp_a": AppendComponent("comp_a"),
            "comp_b": AppendComponent("comp_b"),
        }
        engine = _build_engine(config, instances)
        result = engine.execute()

        timings = result.metadata.component_timings
        assert "comp_a" in timings
        assert "comp_b" in timings
