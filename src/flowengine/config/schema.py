"""FlowEngine configuration schema models.

This module defines Pydantic models for validating flow configurations.
"""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from flowengine.agent.meta import IOFieldSpec


class ComponentConfig(BaseModel):
    """Configuration for a single component.

    Attributes:
        name: Unique component name within the flow
        type: Component class path (e.g., "myapp.components.MyComponent")
        config: Component-specific configuration dictionary
    """

    name: str = Field(..., description="Unique component name")
    type: str = Field(..., description="Component class path")
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Component-specific configuration",
    )


class FlowSettings(BaseModel):
    """Flow execution settings.

    Attributes:
        fail_fast: If True, stop on first error. Default True.
        timeout_seconds: Maximum flow execution time. Default 300.
        timeout_mode: How to enforce timeouts:
            - "cooperative": Components must call check_deadline() (default)
            - "hard_async": Use asyncio.wait_for for async component execution
            - "hard_process": Run steps in separate processes with hard kill
        require_deadline_check: If True, raise error when long-running components
            don't call check_deadline() (only applies to cooperative mode).
            Default False (only warns).
        on_condition_error: How to handle condition evaluation errors:
            - "fail": Raise ConditionEvaluationError (default)
            - "skip": Skip the step and record the error
            - "warn": Log a warning and skip the step
        max_iterations: Maximum number of loop iterations for cyclic graphs.
            Default 10. Range: 1-1000.
        on_max_iterations: Policy when max_iterations is reached:
            - "fail": Raise MaxIterationsError (default)
            - "exit": Silently stop execution
            - "warn": Log a warning and stop execution
        convergence_check: Reserved for v0.3.1. Enable convergence detection.
        convergence_keys: Reserved for v0.3.1. Context keys to check for convergence.
    """

    fail_fast: bool = Field(
        default=True,
        description="Stop on first error",
    )
    timeout_seconds: float = Field(
        default=300.0,
        description="Maximum flow execution time",
        gt=0,
    )
    timeout_mode: Literal["cooperative", "hard_async", "hard_process"] = Field(
        default="cooperative",
        description=(
            "Timeout enforcement mode: 'cooperative' (components call check_deadline), "
            "'hard_async' (asyncio.wait_for), 'hard_process' (process isolation)"
        ),
    )
    require_deadline_check: bool = Field(
        default=False,
        description=(
            "If True, raise error when long-running components don't call "
            "check_deadline() in cooperative mode. Default False (only warns)."
        ),
    )
    on_condition_error: Literal["fail", "skip", "warn"] = Field(
        default="fail",
        description="How to handle condition evaluation errors",
    )
    max_iterations: int = Field(
        default=10,
        description="Maximum loop iterations for cyclic graphs",
        ge=1,
        le=1000,
    )
    on_max_iterations: Literal["fail", "exit", "warn"] = Field(
        default="fail",
        description="Policy when max_iterations is reached in cyclic graphs",
    )
    convergence_check: bool = Field(
        default=False,
        description="Reserved for v0.3.1: enable convergence detection",
    )
    convergence_keys: list[str] = Field(
        default_factory=list,
        description="Reserved for v0.3.1: context keys to check for convergence",
    )
    executor: Optional[Literal["graph"]] = Field(
        default=None,
        description=(
            "Executor path a graph flow commits to. 'graph' = the GraphExecutor "
            "(ports, conditions, cycles, bindings, resume); runtimes may require it."
        ),
    )


class StepConfig(BaseModel):
    """Configuration for a single execution step.

    Attributes:
        component: Name of component to execute
        description: Human-readable step description
        condition: Python expression for conditional execution
        on_error: How to handle errors (fail/skip/continue)
    """

    component: str = Field(..., description="Component name to execute")
    description: Optional[str] = Field(
        default=None,
        description="Human-readable step description",
    )
    condition: Optional[str] = Field(
        default=None,
        description="Python expression for conditional execution",
    )
    on_error: Literal["fail", "skip", "continue"] = Field(
        default="fail",
        description="Error handling behavior",
    )


class BindingLiteral(BaseModel):
    """A bound input given as a literal value: ``{ value: ... }``."""

    value: Any = Field(..., description="Literal value for the input")


class BindingExpr(BaseModel):
    """A bound input computed by a safe-AST expression: ``{ expr: "..." }``.

    The expression sees the flow inputs as ``flow`` and every node namespace by
    node id (``n_lookup.rows``), plus ``context``; the same allow-list as edge
    conditions applies (no calls).
    """

    expr: str = Field(..., description="Expression over the node namespaces")


BindingSource = str | BindingLiteral | BindingExpr
"""A bound input: a reference string ``<node_id>.<key>[.<path>...]`` (``flow``
for flow inputs), a literal, or an expression."""


class NodeBinding(BaseModel):
    """Data bindings of a graph node (the *data wires* of a canvas).

    Attributes:
        inputs: Key the component reads -> where its value comes from.
        outputs: Key the component writes -> ``<own_node_id>.<key>`` target.
            Every key a node writes lands in its own namespace by default.
    """

    inputs: dict[str, BindingSource] = Field(
        default_factory=dict, description="Consumed key -> reference, literal or expr"
    )
    outputs: dict[str, str] = Field(
        default_factory=dict, description="Produced key -> <node_id>.<key> target"
    )


class GraphNodeConfig(BaseModel):
    """A node in a graph flow.

    Attributes:
        id: Unique node ID within the graph
        component: References a component name
        description: Human-readable node description
        on_error: How to handle errors (fail/skip/continue)
        max_visits: Maximum times this node can execute in cyclic graphs.
            None means use the flow-level max_iterations setting.
        bind: Data bindings. A flow with a ``bind`` on any node runs in
            namespaced mode: every node writes into ``context.data.<node_id>``
            (see :mod:`flowengine.core.binding`).
        provenance: Provenance class per bound input (``citizen``, ``model``,
            ``document``, ``system_of_record``, ``identity.verified``, ...).
            Carried for policy; the engine does not interpret it.
    """

    id: str = Field(..., description="Unique node ID within the graph")
    component: str = Field(..., description="Component name to execute")
    description: Optional[str] = Field(
        default=None,
        description="Human-readable node description",
    )
    on_error: Literal["fail", "skip", "continue"] = Field(
        default="fail",
        description="Error handling behavior",
    )
    max_visits: Optional[int] = Field(
        default=None,
        description="Max executions in cyclic graphs (None = use flow max_iterations)",
    )
    bind: Optional[NodeBinding] = Field(
        default=None, description="Data bindings (inputs/outputs) of this node"
    )
    provenance: Optional[dict[str, str]] = Field(
        default=None, description="Provenance class per input key (policy metadata)"
    )


class GraphEdgeConfig(BaseModel):
    """An edge connecting two nodes in a graph flow.

    An edge activates when BOTH hold:
    - its port matches the source node's active output port (or ``port`` is None), and
    - its ``condition`` evaluates True against the context (or ``condition`` is None).

    Attributes:
        source: Source node ID
        target: Target node ID
        port: Output port name (e.g. "true", "false"). None = unconditional edge.
        condition: Optional Python expression gating the edge, evaluated against
            ``context`` (e.g. ``context.get('score') > 0.5``). None = no condition.
    """

    source: str = Field(..., description="Source node ID")
    target: str = Field(..., description="Target node ID")
    port: Optional[str] = Field(
        default=None,
        description="Output port name (e.g. 'true', 'false'). None = unconditional.",
    )
    condition: Optional[str] = Field(
        default=None,
        description="Python expression gating this edge, evaluated against `context`.",
    )


class FlowDefinition(BaseModel):
    """Flow structure definition.

    Attributes:
        type: Flow execution type that determines how steps are processed:
            - "sequential": (default) Runs all steps in order. Conditions guard
              individual steps - if a step's condition is False, it's skipped
              and the next step runs. All matching steps execute.
            - "conditional": First-match branching (like switch/case). Stops
              after the first step whose condition evaluates to True. Only one
              step executes. Defaults on_condition_error to "skip".
            - "graph": DAG-based execution with topological ordering and
              port-based routing.
        settings: Execution settings
        steps: Ordered list of execution steps (for sequential/conditional)
        nodes: List of graph nodes (for graph type)
        edges: List of graph edges (for graph type)
    """

    type: Literal["sequential", "conditional", "graph"] = Field(
        default="sequential",
        description=(
            "Flow execution type: 'sequential' runs all matching steps, "
            "'conditional' stops after first match, "
            "'graph' executes DAG with port-based routing"
        ),
    )
    settings: FlowSettings = Field(
        default_factory=FlowSettings,
        description="Execution settings",
    )
    steps: Optional[list[StepConfig]] = Field(
        default=None,
        description="Ordered list of execution steps (sequential/conditional)",
    )
    nodes: Optional[list[GraphNodeConfig]] = Field(
        default=None,
        description="List of graph nodes (graph type)",
    )
    edges: Optional[list[GraphEdgeConfig]] = Field(
        default=None,
        description="List of graph edges (graph type)",
    )

    @model_validator(mode="after")
    def validate_flow_definition(self) -> "FlowDefinition":
        if self.type in ("sequential", "conditional"):
            if not self.steps or len(self.steps) == 0:
                raise ValueError("sequential/conditional flows require 'steps'")
        elif self.type == "graph":
            if not self.nodes or len(self.nodes) == 0:
                raise ValueError("graph flows require 'nodes'")
            if not self.edges:
                self.edges = []
            # Validate unique node IDs
            node_ids = [n.id for n in self.nodes]
            if len(node_ids) != len(set(node_ids)):
                duplicates = [nid for nid in node_ids if node_ids.count(nid) > 1]
                raise ValueError(f"Duplicate node IDs: {set(duplicates)}")
            # Validate all edge source/target reference valid node IDs
            node_id_set = set(node_ids)
            for edge in self.edges:
                if edge.source not in node_id_set:
                    raise ValueError(
                        f"Edge source '{edge.source}' not found in nodes"
                    )
                if edge.target not in node_id_set:
                    raise ValueError(
                        f"Edge target '{edge.target}' not found in nodes"
                    )
        return self


class FlowConfig(BaseModel):
    """Complete flow configuration.

    This is the root model for a flow configuration file.

    Attributes:
        name: Human-readable flow name
        version: Configuration version string
        description: Optional flow description
        components: List of component definitions
        flow: Flow definition with steps

    Example YAML:
        ```yaml
        name: "My Flow"
        version: "1.0"
        components:
          - name: fetcher
            type: myapp.FetchComponent
            config:
              url: "https://api.example.com"
        flow:
          type: sequential
          steps:
            - component: fetcher
        ```
    """

    name: str = Field(..., description="Flow name")
    version: str = Field(default="1.0", description="Configuration version")
    description: Optional[str] = Field(
        default=None,
        description="Flow description",
    )
    inputs: dict[str, IOFieldSpec] = Field(
        default_factory=dict,
        description=(
            "Declared flow inputs (the worker-agent contract). Maps a context "
            "key to its type/required/description. Optional."
        ),
    )
    outputs: dict[str, IOFieldSpec] = Field(
        default_factory=dict,
        description=(
            "Declared flow outputs the flow promises to produce. Used for "
            "semantic validation and flow-as-tool schemas. Optional."
        ),
    )
    components: list[ComponentConfig] = Field(
        ...,
        description="Component definitions",
        min_length=1,
    )
    flow: FlowDefinition = Field(..., description="Flow definition")

    @field_validator("components")
    @classmethod
    def validate_unique_names(
        cls, v: list[ComponentConfig]
    ) -> list[ComponentConfig]:
        """Ensure all component names are unique."""
        names = [c.name for c in v]
        if len(names) != len(set(names)):
            duplicates = [n for n in names if names.count(n) > 1]
            raise ValueError(f"Duplicate component names: {set(duplicates)}")
        return v

    @field_validator("flow")
    @classmethod
    def validate_step_components(
        cls, v: FlowDefinition, info: Any
    ) -> FlowDefinition:
        """Ensure all steps/nodes reference defined components."""
        if "components" in info.data:
            component_names = {c.name for c in info.data["components"]}
            # Validate step-based flows
            if v.steps:
                for step in v.steps:
                    if step.component not in component_names:
                        raise ValueError(
                            f"Step references undefined component: {step.component}"
                        )
            # Validate graph-based flows
            if v.nodes:
                for node in v.nodes:
                    if node.component not in component_names:
                        raise ValueError(
                            f"Node '{node.id}' references undefined component: "
                            f"{node.component}"
                        )
        return v

    @property
    def settings(self) -> FlowSettings:
        """Shortcut to flow settings."""
        return self.flow.settings

    @property
    def steps(self) -> list[StepConfig]:
        """Shortcut to flow steps."""
        return self.flow.steps or []

    def get_component_config(self, name: str) -> Optional[ComponentConfig]:
        """Get configuration for a named component.

        Args:
            name: Component name to find

        Returns:
            ComponentConfig if found, None otherwise
        """
        for comp in self.components:
            if comp.name == name:
                return comp
        return None
