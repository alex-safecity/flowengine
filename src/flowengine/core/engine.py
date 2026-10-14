"""FlowEngine execution engine.

This module provides the FlowEngine class that orchestrates
the execution of flows according to their configuration.
"""

from __future__ import annotations

import asyncio
import logging
import multiprocessing
import pickle
import time
import uuid
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import timezone
from datetime import datetime
from typing import Any, Literal, Optional, Protocol, runtime_checkable

from flowengine.config.registry import (
    ComponentRegistry,
    load_component_class,
    validate_component_type,
)
from flowengine.config.schema import FlowConfig, StepConfig
from flowengine.core.component import BaseComponent
from flowengine.core.context import FlowContext
from flowengine.errors import (
    ComponentError,
    ConditionEvaluationError,
    ConfigurationError,
    DeadlineCheckError,
    FlowExecutionError,
    FlowTimeoutError,
)
from flowengine.eval.evaluator import ConditionEvaluator

logger = logging.getLogger(__name__)

# Threshold for warning/enforcement about missing deadline checks (seconds)
DEADLINE_CHECK_WARNING_THRESHOLD = 1.0


@runtime_checkable
class ExecutionHook(Protocol):
    """Callback protocol for step lifecycle events."""

    def on_node_start(
        self, node_id: str, component_name: str, context: FlowContext
    ) -> None: ...

    def on_node_complete(
        self,
        node_id: str,
        component_name: str,
        context: FlowContext,
        duration: float,
    ) -> None: ...

    def on_node_error(
        self,
        node_id: str,
        component_name: str,
        error: Exception,
        context: FlowContext,
    ) -> None: ...

    def on_node_skipped(
        self, node_id: str, component_name: str, reason: str
    ) -> None: ...

    def on_flow_suspended(
        self, node_id: str, reason: str, checkpoint_id: str | None
    ) -> None: ...

    def on_iteration_start(
        self, iteration: int, cycle_entry_node: str, context: FlowContext
    ) -> None: ...

    def on_iteration_complete(
        self, iteration: int, cycle_entry_node: str, context: FlowContext,
        duration: float,
    ) -> None: ...

    def on_max_iterations(
        self, max_iterations: int, cycle_entry_node: str, context: FlowContext
    ) -> None: ...


class FlowEngine:
    """Executes a flow defined by a configuration.

    The engine:
    1. Loads components and their configurations
    2. Executes steps in order
    3. Evaluates conditions for conditional steps
    4. Handles errors according to step configuration
    5. Tracks execution metadata

    Example:
        ```python
        # Define components
        components = {
            "fetch": FetchComponent("fetch"),
            "process": ProcessComponent("process"),
            "save": SaveComponent("save"),
        }

        # Load configuration
        config = ConfigLoader.load("flow.yaml")

        # Create engine
        engine = FlowEngine(config, components)

        # Execute flow
        context = FlowContext()
        result = engine.execute(context)

        print(result.to_json())
        ```
    """

    def __init__(
        self,
        config: FlowConfig,
        components: dict[str, BaseComponent],
        evaluator: Optional[ConditionEvaluator] = None,
        validate_types: bool = True,
        checkpoint_store: Any = None,
        hooks: list[Any] | None = None,
    ) -> None:
        """Initialize the flow engine.

        Args:
            config: Parsed flow configuration
            components: Dictionary mapping component names to instances
            evaluator: Optional custom condition evaluator
            validate_types: If True (default), validates that component instances
                           match their declared type paths in the config
            checkpoint_store: Optional CheckpointStore for pause/resume support
            hooks: Optional list of ExecutionHook instances for lifecycle callbacks

        Raises:
            FlowExecutionError: If components are missing or invalid
            ConfigurationError: If validate_types is True and types don't match
        """
        self.config = config
        self.components = components
        self.evaluator = evaluator or ConditionEvaluator()
        self._checkpoint_store = checkpoint_store
        self._hooks = hooks or []

        # Flow type and settings
        self.flow_type = config.flow.type
        self.fail_fast = config.settings.fail_fast
        self.timeout = config.settings.timeout_seconds
        self.timeout_mode: Literal["cooperative", "hard_async", "hard_process"] = (
            config.settings.timeout_mode
        )
        self.require_deadline_check = config.settings.require_deadline_check
        self.on_condition_error = config.settings.on_condition_error

        # In conditional flow type, default to more lenient condition error handling
        # This allows flows designed for branching to skip invalid conditions gracefully
        if self.flow_type == "conditional":
            # Only override if using the schema default (fail)
            # Check if user explicitly set it by comparing to model default
            if self.on_condition_error == "fail":
                logger.debug(
                    "Conditional flow type: defaulting on_condition_error to 'skip'"
                )
                self.on_condition_error = "skip"

        # Initialize components
        self._initialize_components()

        # Validate component types if requested
        if validate_types:
            type_errors = self.validate_component_types()
            if type_errors:
                raise ConfigurationError(
                    "Component type validation failed",
                    details=type_errors,
                )

    def _initialize_components(self) -> None:
        """Initialize all components with their configurations.

        Raises:
            FlowExecutionError: If component not found or config invalid
        """
        if self.flow_type == "graph":
            # Graph flows: initialize from nodes
            for node in self.config.flow.nodes or []:
                component = self.components.get(node.component)
                if not component:
                    raise FlowExecutionError(
                        f"Component not found: {node.component}"
                    )
                comp_config = self._get_component_config(node.component)
                component.init(comp_config)
                errors = component.validate_config()
                if errors:
                    raise FlowExecutionError(
                        f"Invalid config for {node.component}: {errors}"
                    )
        else:
            # Sequential/conditional flows: initialize from steps
            for step in self.config.steps:
                component = self.components.get(step.component)
                if not component:
                    raise FlowExecutionError(
                        f"Component not found: {step.component}"
                    )

                # Find component config from flow config
                comp_config = self._get_component_config(step.component)
                component.init(comp_config)

                # Validate configuration
                errors = component.validate_config()
                if errors:
                    raise FlowExecutionError(
                        f"Invalid config for {step.component}: {errors}"
                    )

    def _get_component_config(self, name: str) -> dict[str, Any]:
        """Get configuration for a named component.

        Args:
            name: Component name

        Returns:
            Component configuration dictionary
        """
        comp = self.config.get_component_config(name)
        if comp:
            return comp.config
        return {}

    def execute(
        self,
        context: Optional[FlowContext] = None,
        input_data: Any = None,
    ) -> FlowContext:
        """Execute the flow.

        For sequential flows: executes all steps in order.
        For conditional flows: executes only the first step whose condition matches
        (first-match branching, like a switch/case statement).
        For graph flows: executes DAG with topological ordering and port routing.

        Args:
            context: Optional existing context (creates new if None)
            input_data: Optional input data to attach to context

        Returns:
            Final flow context with all accumulated data

        Raises:
            FlowExecutionError: If execution fails and fail_fast is True
        """
        # Create or use provided context
        context = context or FlowContext()
        if input_data is not None:
            context.input = input_data

        logger.info(
            f"Starting {self.flow_type} flow execution: {context.metadata.flow_id}"
        )

        try:
            if self.flow_type == "graph":
                context = self._execute_graph(context)
            else:
                context = self._execute_steps(context)
        except (
            FlowExecutionError,
            FlowTimeoutError,
            ComponentError,
            ConditionEvaluationError,
            ConfigurationError,
        ):
            raise
        except Exception as e:
            logger.error(f"Unexpected error in flow: {e}")
            raise FlowExecutionError(f"Flow execution failed: {e}") from e
        finally:
            if not context.metadata.suspended:
                context.metadata.completed_at = datetime.now(timezone.utc)

        # Handle suspension checkpoint
        if context.metadata.suspended and self._checkpoint_store:
            from flowengine.core.checkpoint import Checkpoint

            checkpoint = Checkpoint(
                flow_config=self.config.model_dump(),
                context=context.to_dict(),
                created_at=datetime.now(timezone.utc).isoformat(),
                checkpoint_id=str(uuid.uuid4()),
            )
            self._checkpoint_store.save(checkpoint)
            context.set("checkpoint_id", checkpoint.checkpoint_id)

        logger.info(
            f"Flow {'suspended' if context.metadata.suspended else 'completed'}: "
            f"{context.metadata.flow_id} "
            f"({len(context.metadata.component_timings)} components executed)"
        )

        return context

    def _execute_steps(self, context: FlowContext) -> FlowContext:
        """Execute sequential/conditional flow steps."""
        # Track flow start time for timeout enforcement
        flow_start_time = time.time()

        for step_idx, step in enumerate(self.config.steps):
            # Calculate remaining timeout
            elapsed = time.time() - flow_start_time
            remaining_timeout = None
            if self.timeout:
                remaining_timeout = self.timeout - elapsed
                if remaining_timeout <= 0:
                    raise FlowTimeoutError(
                        f"Flow timeout exceeded: {elapsed:.2f}s > {self.timeout}s",
                        timeout=self.timeout,
                        elapsed=elapsed,
                        flow_id=context.metadata.flow_id,
                        step=step.component,
                    )

            executed = self._execute_step(
                step, context, remaining_timeout, step_idx
            )

            # For conditional flows, stop after first matching step executes
            if self.flow_type == "conditional" and executed is not None:
                logger.debug(
                    f"Conditional flow: stopping after {step.component} matched"
                )
                context = executed
                break

            # Update context if step returned a new one (not skipped)
            if executed is not None:
                context = executed
            # else: step was skipped, context unchanged (modified in-place)

            # Safety assertion to catch any internal errors
            assert context is not None, (
                f"Internal error: context became None after step {step.component}"
            )

        return context

    def _execute_graph(
        self, context: FlowContext, resume_node: str | None = None
    ) -> FlowContext:
        """Execute a graph-type flow using the GraphExecutor."""
        from flowengine.core.graph import GraphExecutor

        executor = GraphExecutor(
            nodes=self.config.flow.nodes or [],
            edges=self.config.flow.edges or [],
            components=self.components,
            settings=self.config.flow.settings,
            hooks=self._hooks,
            evaluator=self.evaluator,
        )
        return executor.execute(context, resume_node=resume_node)

    def resume(
        self,
        checkpoint_id: str,
        resume_data: dict[str, Any] | None = None,
    ) -> FlowContext:
        """Resume a suspended flow from checkpoint.

        Args:
            checkpoint_id: ID from a previous suspended execution
            resume_data: Data to inject (e.g., approval decision)

        Returns:
            FlowContext with execution completed from suspension point
        """
        if not self._checkpoint_store:
            raise FlowExecutionError("No checkpoint store configured")

        checkpoint = self._checkpoint_store.load(checkpoint_id)
        if not checkpoint:
            raise FlowExecutionError(f"Checkpoint not found: {checkpoint_id}")

        context = FlowContext.from_dict(checkpoint.context)
        resume_node = context.metadata.suspended_at_node

        if resume_data:
            context.set("resume_data", resume_data)

        # Re-enter the graph at the suspended node; completed nodes are
        # skipped and propagate the ports they set (the executor clears the
        # suspension flags itself)
        result = self._execute_graph(context, resume_node=resume_node)

        self._checkpoint_store.delete(checkpoint_id)
        return result

    def _execute_step(
        self,
        step: StepConfig,
        context: FlowContext,
        remaining_timeout: Optional[float] = None,
        step_idx: Optional[int] = None,
    ) -> Optional[FlowContext]:
        """Execute a single step.

        Args:
            step: Step configuration
            context: Current flow context
            remaining_timeout: Remaining timeout in seconds (None = no timeout)
            step_idx: Index of this step in the flow definition (0-based)

        Returns:
            Updated flow context if step executed, None if skipped

        Raises:
            ComponentError: If component fails and on_error is "fail"
            FlowTimeoutError: If step execution exceeds remaining timeout
        """
        component = self.components[step.component]

        # Check condition
        if step.condition:
            try:
                should_run = self.evaluator.evaluate(step.condition, context)
            except ConditionEvaluationError as e:
                # Record the condition error in metadata
                context.metadata.add_condition_error(
                    step.component, e, step.condition
                )

                if self.on_condition_error == "fail":
                    logger.error(
                        f"Condition evaluation failed for {step.component}: {e}"
                    )
                    raise
                elif self.on_condition_error == "skip":
                    logger.info(
                        f"Skipping {step.component} due to condition error: {e}"
                    )
                    context.metadata.skipped_components.append(step.component)
                    return None  # Skipped
                else:  # "warn"
                    logger.warning(
                        f"Condition evaluation failed for {step.component}: {e}"
                    )
                    should_run = False
            except Exception as e:
                # Wrap unexpected errors in ConditionEvaluationError
                wrapped = ConditionEvaluationError(
                    f"Unexpected error evaluating condition: {e}"
                )
                context.metadata.add_condition_error(
                    step.component, wrapped, step.condition
                )

                if self.on_condition_error == "fail":
                    logger.error(
                        f"Condition evaluation failed for {step.component}: {e}"
                    )
                    raise wrapped from e
                elif self.on_condition_error == "skip":
                    logger.info(
                        f"Skipping {step.component} due to condition error: {e}"
                    )
                    context.metadata.skipped_components.append(step.component)
                    return None  # Skipped
                else:  # "warn"
                    logger.warning(
                        f"Condition evaluation failed for {step.component}: {e}"
                    )
                    should_run = False

            if not should_run:
                logger.info(f"Skipping {step.component}: condition not met")
                context.metadata.skipped_components.append(step.component)
                return None  # Skipped

        # Execute component
        logger.debug(f"Executing {step.component}")
        start_time = time.time()
        step_started_at = datetime.now(timezone.utc)

        try:
            # Set deadline for cooperative timeout
            # Components can check this via self.check_deadline(context)
            if remaining_timeout is not None:
                context.metadata.deadline = time.time() + remaining_timeout
            else:
                context.metadata.deadline = None

            # Reset deadline_checked flag before each step
            context.metadata.deadline_checked = False

            # Execute based on timeout mode
            if self.timeout_mode == "hard_async":
                context = self._execute_step_async(
                    component, context, remaining_timeout
                )
            elif self.timeout_mode == "hard_process":
                context = self._execute_step_process(
                    component, context, remaining_timeout
                )
            else:  # cooperative mode (default)
                context = self._execute_step_cooperative(component, context)

            # Record timing
            elapsed = time.time() - start_time
            context.metadata.record_timing(
                step.component, elapsed, step_started_at, step_idx
            )

            # Check deadline compliance in cooperative mode
            if (
                self.timeout_mode == "cooperative"
                and elapsed > DEADLINE_CHECK_WARNING_THRESHOLD
                and remaining_timeout is not None
                and not context.metadata.deadline_checked
            ):
                if self.require_deadline_check:
                    # Strict enforcement: raise error
                    raise DeadlineCheckError(
                        f"Component '{step.component}' took {elapsed:.2f}s but never "
                        f"called check_deadline(). This is required when "
                        f"require_deadline_check=True.",
                        component=step.component,
                        duration=elapsed,
                        threshold=DEADLINE_CHECK_WARNING_THRESHOLD,
                        flow_id=context.metadata.flow_id,
                    )
                else:
                    # Warning only (default)
                    logger.warning(
                        f"Component '{step.component}' took {elapsed:.2f}s but never "
                        f"called check_deadline(). Consider adding deadline checks "
                        f"for timeout compliance."
                    )

            logger.info(f"Completed {step.component} in {elapsed:.3f}s")

        except (FlowTimeoutError, DeadlineCheckError):
            # Re-raise timeout and deadline check errors
            elapsed = time.time() - start_time
            context.metadata.record_timing(
                step.component, elapsed, step_started_at, step_idx
            )
            raise
        except Exception as e:
            elapsed = time.time() - start_time
            context.metadata.record_timing(
                step.component, elapsed, step_started_at, step_idx
            )
            context.metadata.add_error(step.component, e)

            logger.error(f"Error in {step.component}: {e}")

            # Handle based on on_error setting
            if step.on_error == "fail" or self.fail_fast:
                raise ComponentError(
                    component=step.component,
                    message=str(e),
                    original_error=e,
                ) from e
            elif step.on_error == "skip":
                context.metadata.skipped_components.append(step.component)
            # on_error == "continue" just continues
        finally:
            # Ensure deadline state is always cleared
            context.metadata.deadline = None
            context.metadata.deadline_checked = False

        return context

    def _execute_step_cooperative(
        self,
        component: BaseComponent,
        context: FlowContext,
    ) -> FlowContext:
        """Execute component in cooperative mode (default).

        The component runs in the current thread and must voluntarily
        call check_deadline() to respect timeouts.

        Args:
            component: Component to execute
            context: Current flow context

        Returns:
            Updated flow context
        """
        component.setup(context)
        try:
            context = component.process(context)
        finally:
            component.teardown(context)
            context.metadata.deadline = None
        return context

    def _execute_step_async(
        self,
        component: BaseComponent,
        context: FlowContext,
        timeout: Optional[float],
    ) -> FlowContext:
        """Execute component with asyncio hard timeout.

        Wraps component execution in asyncio.wait_for for enforced cancellation.
        Teardown always runs even if timeout occurs.

        Args:
            component: Component to execute
            context: Current flow context
            timeout: Timeout in seconds (None = no timeout)

        Returns:
            Updated flow context

        Raises:
            FlowTimeoutError: If timeout expires before component completes
        """

        async def run_component() -> FlowContext:
            """Run component lifecycle in async context."""
            component.setup(context)
            try:
                # For sync components, run in executor to allow cancellation
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, component.process, context
                )
                return result
            finally:
                component.teardown(context)
                context.metadata.deadline = None

        try:
            # Run with timeout
            if timeout is not None:
                coro = asyncio.wait_for(run_component(), timeout=timeout)
            else:
                coro = run_component()

            # Use existing event loop or create new one
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None:
                # Already in async context - use create_task
                import concurrent.futures

                # Run in a new thread to avoid nested event loop issues
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, coro)
                    return future.result()
            else:
                return asyncio.run(coro)

        except asyncio.TimeoutError:
            # Ensure teardown runs on timeout
            try:
                component.teardown(context)
            except Exception as teardown_error:
                logger.warning(f"Teardown error after timeout: {teardown_error}")
            raise FlowTimeoutError(
                f"Hard async timeout: component '{component.name}' exceeded "
                f"{timeout:.2f}s",
                timeout=timeout,
                elapsed=timeout or 0.0,
                flow_id=context.metadata.flow_id,
                step=component.name,
            )

    def _execute_step_process(
        self,
        component: BaseComponent,
        context: FlowContext,
        timeout: Optional[float],
    ) -> FlowContext:
        """Execute component in separate process with hard timeout.

        Runs the component in an isolated process that can be terminated
        if timeout expires. Teardown always runs in the main process.

        Args:
            component: Component to execute
            context: Current flow context
            timeout: Timeout in seconds (None = no timeout)

        Returns:
            Updated flow context

        Raises:
            FlowTimeoutError: If timeout expires before component completes
        """
        component.setup(context)

        try:
            # Serialize context for process
            context_data = context.to_dict()

            with ProcessPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    _run_component_in_process,
                    component.__class__.__module__,
                    component.__class__.__name__,
                    component.name,
                    component.config,
                    context_data,
                )

                try:
                    result_data = future.result(timeout=timeout)
                    # Restore context from process result
                    context = FlowContext.from_dict(result_data)
                    return context
                except FuturesTimeoutError:
                    # Cancel the future (will terminate process)
                    future.cancel()
                    raise FlowTimeoutError(
                        f"Hard process timeout: component '{component.name}' exceeded "
                        f"{timeout:.2f}s",
                        timeout=timeout,
                        elapsed=timeout or 0.0,
                        flow_id=context.metadata.flow_id,
                        step=component.name,
                    )
        finally:
            # Always run teardown in main process
            component.teardown(context)
            context.metadata.deadline = None

    def validate(self) -> list[str]:
        """Validate the flow configuration.

        Returns:
            List of validation errors (empty if valid)
        """
        errors: list[str] = []

        if self.flow_type == "graph":
            # Validate graph nodes
            for node in self.config.flow.nodes or []:
                if node.component not in self.components:
                    errors.append(f"Unknown component: {node.component}")
        else:
            # Check all referenced components exist
            for step in self.config.steps:
                if step.component not in self.components:
                    errors.append(f"Unknown component: {step.component}")

        # Validate each component's config
        for name, component in self.components.items():
            comp_errors = component.validate_config()
            for err in comp_errors:
                errors.append(f"{name}: {err}")

        # Validate conditions (step-based flows only)
        for step in self.config.steps:
            if step.condition:
                condition_errors = self.evaluator.validate(step.condition)
                for err in condition_errors:
                    errors.append(f"{step.component} condition: {err}")

        return errors

    def dry_run(self, context: Optional[FlowContext] = None) -> list[str]:
        """Perform a dry run without executing components.

        Args:
            context: Optional context for condition evaluation

        Returns:
            List of steps/nodes that would be executed
        """
        context = context or FlowContext()

        if self.flow_type == "graph":
            from flowengine.core.graph import GraphExecutor

            executor = GraphExecutor(
                nodes=self.config.flow.nodes or [],
                edges=self.config.flow.edges or [],
                components=self.components,
                settings=self.config.flow.settings,
            )
            if executor._has_cycles:
                # Cyclic graphs can't be topologically sorted; return all nodes
                return [n.component for n in (self.config.flow.nodes or [])]
            order = executor._topological_sort()
            return [
                self.config.flow.nodes[
                    next(
                        i
                        for i, n in enumerate(self.config.flow.nodes or [])
                        if n.id == nid
                    )
                ].component
                for nid in order
            ]

        executed: list[str] = []
        for step in self.config.steps:
            if step.condition:
                try:
                    should_run = self.evaluator.evaluate(step.condition, context)
                    if not should_run:
                        continue
                except Exception:
                    continue

            executed.append(step.component)

        return executed

    def validate_component_types(self) -> list[str]:
        """Validate that component instances match their declared types.

        Returns:
            List of type mismatch errors (empty if all valid)
        """
        errors: list[str] = []

        for comp_config in self.config.components:
            component = self.components.get(comp_config.name)
            if not component:
                continue  # Missing components are caught by validate()

            error = validate_component_type(component, comp_config.type)
            if error:
                errors.append(f"{comp_config.name}: {error}")

        return errors

    def _notify(self, method: str, *args: Any, **kwargs: Any) -> None:
        """Notify all registered hooks."""
        for hook in self._hooks:
            fn = getattr(hook, method, None)
            if fn:
                try:
                    fn(*args, **kwargs)
                except Exception:
                    pass  # hooks must not break execution

    @classmethod
    def from_config(
        cls,
        config: FlowConfig,
        evaluator: Optional[ConditionEvaluator] = None,
        registry: Optional[ComponentRegistry] = None,
        checkpoint_store: Any = None,
        hooks: list[Any] | None = None,
    ) -> FlowEngine:
        """Create a FlowEngine by auto-instantiating components from config.

        This method loads component classes from their type paths and creates
        instances automatically. Use this when you want YAML-complete flows.

        Args:
            config: Parsed flow configuration
            evaluator: Optional custom condition evaluator
            registry: Optional pre-configured component registry
            checkpoint_store: Optional CheckpointStore for pause/resume
            hooks: Optional list of ExecutionHook instances

        Returns:
            Configured FlowEngine instance

        Raises:
            ConfigurationError: If component types cannot be loaded

        Example:
            ```python
            config = ConfigLoader.load("flow.yaml")
            engine = FlowEngine.from_config(config)
            result = engine.execute()
            ```
        """
        components: dict[str, BaseComponent] = {}
        registry = registry or ComponentRegistry()

        for comp_config in config.components:
            # Try to get from registry first
            registered_class = registry.get_class(comp_config.type)
            if registered_class:
                component = registered_class(comp_config.name)
            else:
                # Load from type path
                try:
                    component = registry.create_from_path(
                        comp_config.type,
                        comp_config.name,
                    )
                except ConfigurationError:
                    raise

            components[comp_config.name] = component

        return cls(
            config,
            components,
            evaluator,
            checkpoint_store=checkpoint_store,
            hooks=hooks,
        )


def _run_component_in_process(
    module_name: str,
    class_name: str,
    component_name: str,
    config: dict[str, Any],
    context_data: dict[str, Any],
) -> dict[str, Any]:
    """Helper function to run a component in a separate process.

    This is a module-level function so it can be pickled for multiprocessing.

    Args:
        module_name: Module path containing the component class
        class_name: Name of the component class
        component_name: Instance name for the component
        config: Component configuration
        context_data: Serialized FlowContext data

    Returns:
        Serialized FlowContext data after processing
    """
    import importlib

    # Dynamically import the component class
    module = importlib.import_module(module_name)
    component_class = getattr(module, class_name)

    # Create component instance
    component = component_class(component_name)
    component.init(config)

    # Restore context
    context = FlowContext.from_dict(context_data)

    # Run process (setup/teardown handled in main process)
    result = component.process(context)

    # Return serialized result
    return result.to_dict()
