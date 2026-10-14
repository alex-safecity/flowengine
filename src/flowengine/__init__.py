"""FlowEngine: Lightweight YAML-driven state machine for Python.

FlowEngine enables developers to:
- Define execution flows declaratively in YAML
- Build pluggable component systems with standardized interfaces
- Execute conditional branching based on runtime state
- Execute DAG-based graph flows with port-based routing
- Maintain context across component executions
- Pause and resume workflows with execution checkpoints

Example:
    ```python
    from flowengine import BaseComponent, FlowContext, FlowEngine, ConfigLoader

    # Define a custom component
    class GreetComponent(BaseComponent):
        def process(self, context: FlowContext) -> FlowContext:
            name = context.get("name", "World")
            context.set("greeting", f"Hello, {name}!")
            return context

    # Load configuration and run
    config = ConfigLoader.load("flow.yaml")
    components = {"greeter": GreetComponent("greeter")}
    engine = FlowEngine(config, components)

    result = engine.execute()
    print(result.data.greeting)  # "Hello, World!"
    ```
"""

__version__ = "0.7.0"

# Core classes
from flowengine.core.component import BaseComponent
from flowengine.core.context import DotDict, ExecutionMetadata, FlowContext, StepTiming
from flowengine.core.engine import ExecutionHook, FlowEngine
from flowengine.core.graph import GraphExecutor
from flowengine.core.checkpoint import (
    Checkpoint,
    CheckpointStore,
    InMemoryCheckpointStore,
)

# Configuration
from flowengine.config.loader import ConfigLoader
from flowengine.config.registry import ComponentRegistry, load_component_class
from flowengine.config.schema import (
    BindingExpr,
    BindingLiteral,
    ComponentConfig,
    FlowConfig,
    FlowDefinition,
    FlowSettings,
    GraphEdgeConfig,
    GraphNodeConfig,
    NodeBinding,
    StepConfig,
)
from flowengine.core.binding import BindingError, BoundComponent, ScopedContext

# Evaluation
from flowengine.eval.evaluator import ConditionEvaluator
from flowengine.eval.safe_ast import SafeASTValidator

# Errors
from flowengine.errors import (
    ComponentError,
    ConditionEvaluationError,
    ConfigurationError,
    DeadlineCheckError,
    FlowEngineError,
    FlowExecutionError,
    FlowTimeoutError,
    MaxIterationsError,
    PolicyViolationError,
)

# Agent-native API (v0.5.0)
from flowengine.agent import (
    AgentTrace,
    CompileResult,
    ComponentMeta,
    ExecutionPolicy,
    FlowCompiler,
    FlowIssue,
    FlowPlan,
    FlowTool,
    IOFieldSpec,
    IssueCode,
    JsonPatchOp,
    PortSpec,
    RepairSuggestion,
    apply_patch,
    build_catalog,
    explain,
    export_json_schema,
    normalize_yaml,
    validate_semantics,
)

# Contrib components
from flowengine.contrib.logging import LoggingComponent
from flowengine.contrib.subflow import SubflowComponent

# HTTPComponent is optional (requires httpx)
try:
    from flowengine.contrib.http import HTTPComponent

    _http_exports = ["HTTPComponent"]
except ImportError:
    _http_exports = []

__all__ = [
    # Version
    "__version__",
    # Core
    "BaseComponent",
    "FlowContext",
    "DotDict",
    "ExecutionMetadata",
    "StepTiming",
    "FlowEngine",
    "ExecutionHook",
    "GraphExecutor",
    "Checkpoint",
    "CheckpointStore",
    "InMemoryCheckpointStore",
    # Config
    "ConfigLoader",
    "ComponentRegistry",
    "load_component_class",
    "FlowConfig",
    "ComponentConfig",
    "StepConfig",
    "FlowSettings",
    "FlowDefinition",
    "GraphNodeConfig",
    "GraphEdgeConfig",
    "NodeBinding",
    "BindingLiteral",
    "BindingExpr",
    # Bindings (0.7.0)
    "BoundComponent",
    "ScopedContext",
    "BindingError",
    # Evaluation
    "ConditionEvaluator",
    "SafeASTValidator",
    # Errors
    "FlowEngineError",
    "ConfigurationError",
    "FlowExecutionError",
    "FlowTimeoutError",
    "MaxIterationsError",
    "DeadlineCheckError",
    "ComponentError",
    "ConditionEvaluationError",
    "PolicyViolationError",
    # Agent-native API (v0.5.0)
    "ComponentMeta",
    "IOFieldSpec",
    "PortSpec",
    "FlowIssue",
    "IssueCode",
    "RepairSuggestion",
    "JsonPatchOp",
    "validate_semantics",
    "FlowCompiler",
    "CompileResult",
    "FlowPlan",
    "explain",
    "AgentTrace",
    "normalize_yaml",
    "build_catalog",
    "export_json_schema",
    "apply_patch",
    "ExecutionPolicy",
    "FlowTool",
    # Contrib
    "LoggingComponent",
    "SubflowComponent",
    *_http_exports,
]
