"""FlowEngine context module.

This module provides the context classes that carry data through flow execution:
- DotDict: Dictionary with attribute-style access
- ExecutionMetadata: Tracks timing, errors, and execution state
- FlowContext: Main context object passed through all components
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


def _utc_now() -> datetime:
    """Get current UTC time as timezone-aware datetime."""
    return datetime.now(timezone.utc)


class DotDict:
    """Dictionary with attribute-style access.

    Allows accessing nested dictionary values using dot notation
    instead of bracket notation.

    Example:
        ```python
        d = DotDict({"user": {"name": "Alice"}})
        print(d.user.name)  # "Alice"
        d.user.age = 30
        print(d.to_dict())  # {"user": {"name": "Alice", "age": 30}}
        ```
    """

    def __init__(self, data: Optional[dict[str, Any]] = None) -> None:
        """Initialize DotDict with optional data.

        Args:
            data: Initial dictionary data (defaults to empty dict)
        """
        object.__setattr__(self, "_data", data or {})

    def __getattr__(self, key: str) -> Any:
        """Get attribute with automatic DotDict wrapping for nested dicts.

        Args:
            key: Attribute name to retrieve

        Returns:
            Value at key, wrapped in DotDict if it's a dict
        """
        if key.startswith("_"):
            return object.__getattribute__(self, key)

        # Check if key exists in data first (to avoid method name conflicts)
        if key in self._data:
            value = self._data[key]
            if isinstance(value, dict):
                return DotDict(value)
            return value

        # Return None for missing keys
        return None

    def __setattr__(self, key: str, value: Any) -> None:
        """Set attribute value.

        Args:
            key: Attribute name to set
            value: Value to store (DotDict values are unwrapped)
        """
        if key.startswith("_"):
            object.__setattr__(self, key, value)
        else:
            if isinstance(value, DotDict):
                self._data[key] = value._data
            else:
                self._data[key] = value

    def __contains__(self, key: str) -> bool:
        """Check if key exists in data.

        Args:
            key: Key to check

        Returns:
            True if key exists
        """
        return key in self._data

    def __delattr__(self, key: str) -> None:
        """Delete attribute from data.

        Args:
            key: Key to delete
        """
        if key.startswith("_"):
            object.__delattr__(self, key)
        elif key in self._data:
            del self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        """Get value with default fallback.

        Args:
            key: Key to retrieve
            default: Default value if key not found

        Returns:
            Value at key or default
        """
        value = getattr(self, key, None)
        if value is None:
            return default
        return value

    def keys(self) -> list[str]:
        """Get all keys in the data.

        Returns:
            List of keys
        """
        return list(self._data.keys())

    def values(self) -> list[Any]:
        """Get all values in the data.

        Returns:
            List of values
        """
        return list(self._data.values())

    def items(self) -> list[tuple[str, Any]]:
        """Get all key-value pairs.

        Returns:
            List of (key, value) tuples
        """
        return list(self._data.items())

    def update(self, data: dict[str, Any]) -> None:
        """Update with dictionary values.

        Args:
            data: Dictionary to merge
        """
        self._data.update(data)

    def to_dict(self) -> dict[str, Any]:
        """Convert to regular dictionary.

        Returns:
            Copy of underlying data dictionary
        """
        return self._data.copy()

    def __repr__(self) -> str:
        return f"DotDict({self._data})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DotDict):
            return self._data == other._data
        if isinstance(other, dict):
            return self._data == other
        return False


@dataclass
class StepTiming:
    """Timing information for a single step execution.

    Attributes:
        step_index: Index of the step in the flow definition (0-based, matches config order)
        component: Name of the component executed
        duration: Execution time in seconds
        started_at: When the step started executing
        execution_order: Order in which this step was executed (0-based, skips excluded)
    """

    step_index: int  # Position in flow definition
    component: str
    duration: float
    started_at: datetime
    execution_order: int = 0  # Order of actual execution


@dataclass
class ExecutionMetadata:
    """Metadata about flow execution.

    Tracks timing information, errors, and which components were skipped
    during flow execution.

    Attributes:
        flow_id: Unique identifier for this flow execution
        started_at: When execution started
        completed_at: When execution completed (None if still running)
        step_timings: List of timing info for each executed step
        skipped_components: Names of components skipped due to conditions
        errors: List of error details from failed components
        condition_errors: List of condition evaluation errors
        deadline: Absolute time (time.time()) by which current step must complete.
            Set by engine before each step, cleared after. Components can check
            this via BaseComponent.check_deadline() to cooperatively timeout.
    """

    flow_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=_utc_now)
    completed_at: Optional[datetime] = None

    # Step execution tracking (preserves order and handles repeated components)
    step_timings: list[StepTiming] = field(default_factory=list)
    skipped_components: list[str] = field(default_factory=list)

    # Error tracking
    errors: list[dict[str, Any]] = field(default_factory=list)
    condition_errors: list[dict[str, Any]] = field(default_factory=list)

    # Cooperative timeout support
    deadline: Optional[float] = field(default=None, repr=False)
    deadline_checked: bool = field(default=False, repr=False)

    # Suspension support (pause/resume)
    suspended: bool = False
    suspended_at_node: Optional[str] = None
    suspension_reason: Optional[str] = None
    completed_nodes: list[str] = field(default_factory=list)
    # Active output port recorded for each completed node (None when the node
    # set no port). Resume uses it to re-activate the same downstream edges.
    node_ports: dict[str, str | None] = field(default_factory=dict)
    # Nodes still queued when a cyclic graph suspended (resume re-enqueues them
    # after the suspended node).
    pending_nodes: list[str] = field(default_factory=list)

    # Cyclic graph execution tracking
    node_visit_counts: dict[str, int] = field(default_factory=dict)
    iteration_count: int = 0
    loop_timings: list[dict[str, Any]] = field(default_factory=list)
    max_iterations_reached: bool = False
    converged: bool = False  # Reserved for v0.3.1

    # Internal step counter
    _step_counter: int = field(default=0, repr=False)

    def add_error(
        self,
        component: str,
        error: Exception,
    ) -> None:
        """Record an error from a component.

        Args:
            component: Name of the component that errored
            error: The exception that was raised
        """
        self.errors.append(
            {
                "component": component,
                "error_type": type(error).__name__,
                "message": str(error),
                "timestamp": _utc_now().isoformat(),
            }
        )

    def add_condition_error(
        self,
        component: str,
        error: Exception,
        condition: str,
    ) -> None:
        """Record a condition evaluation error.

        Args:
            component: Name of the component with the failed condition
            error: The exception that was raised
            condition: The condition expression that failed
        """
        self.condition_errors.append(
            {
                "component": component,
                "error_type": type(error).__name__,
                "message": str(error),
                "condition": condition,
                "timestamp": _utc_now().isoformat(),
            }
        )

    def record_timing(
        self,
        component: str,
        seconds: float,
        started_at: Optional[datetime] = None,
        step_index: Optional[int] = None,
    ) -> None:
        """Record execution time for a step.

        Args:
            component: Name of the component
            seconds: Execution time in seconds
            started_at: When the step started (defaults to now)
            step_index: Position in the flow definition (0-based). If not provided,
                        uses execution order as step_index for backward compatibility.
        """
        if started_at is None:
            started_at = _utc_now()

        # If step_index not provided, use execution counter (backward compatibility)
        if step_index is None:
            step_index = self._step_counter

        self.step_timings.append(
            StepTiming(
                step_index=step_index,
                component=component,
                duration=seconds,
                started_at=started_at,
                execution_order=self._step_counter,
            )
        )
        self._step_counter += 1

    @property
    def component_timings(self) -> dict[str, float]:
        """Get aggregated timings by component name.

        If a component runs multiple times, returns the sum of all durations.

        Returns:
            Dictionary mapping component names to total execution time
        """
        timings: dict[str, float] = {}
        for step in self.step_timings:
            if step.component in timings:
                timings[step.component] += step.duration
            else:
                timings[step.component] = step.duration
        return timings

    @property
    def has_errors(self) -> bool:
        """Check if any errors were recorded.

        Returns:
            True if there are recorded errors
        """
        return len(self.errors) > 0

    @property
    def has_condition_errors(self) -> bool:
        """Check if any condition evaluation errors were recorded.

        Returns:
            True if there are recorded condition errors
        """
        return len(self.condition_errors) > 0

    @property
    def total_duration(self) -> Optional[float]:
        """Get total execution duration in seconds.

        Returns:
            Duration in seconds, or None if not completed
        """
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()


@dataclass
class FlowContext:
    """Context object passed through all components in a flow.

    The context accumulates data as it passes through components.
    Each component can read from and write to the context.

    Attributes:
        data: Main data container with attribute-style access
        metadata: Execution metadata (timings, errors, etc.)
        input: Optional initial input data

    Example:
        ```python
        context = FlowContext()

        # Set values
        context.set("user", {"name": "Alice", "age": 30})

        # Get values with dot notation
        print(context.data.user.name)  # "Alice"

        # Check for values
        print(context.get("missing", "default"))  # "default"

        # Serialize for debugging
        print(context.to_json())
        ```
    """

    data: DotDict = field(default_factory=DotDict)
    metadata: ExecutionMetadata = field(default_factory=ExecutionMetadata)

    # Optional initial input
    input: Any = None

    # Port-based routing (transient execution state)
    _active_port: Optional[str] = field(default=None, repr=False)

    def set(self, key: str, value: Any) -> None:
        """Set a value in the data container.

        Args:
            key: Key to set
            value: Value to store
        """
        setattr(self.data, key, value)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the data container.

        Args:
            key: Key to retrieve
            default: Default value if key not found

        Returns:
            Value or default
        """
        return self.data.get(key, default)

    def has(self, key: str) -> bool:
        """Check if a key exists in the data container.

        Args:
            key: Key to check

        Returns:
            True if key exists
        """
        return key in self.data

    def delete(self, key: str) -> None:
        """Delete a key from the data container.

        Args:
            key: Key to delete
        """
        if key in self.data:
            delattr(self.data, key)

    def set_port(self, port: str) -> None:
        """Set the active output port for the current node.

        Called by components like Condition to signal which branch to take.
        The graph executor reads this to determine edge routing.
        """
        self._active_port = port

    def get_active_port(self) -> Optional[str]:
        """Get the active output port (used by graph executor)."""
        return self._active_port

    def clear_port(self) -> None:
        """Reset active port (called by executor between nodes)."""
        self._active_port = None

    def suspend(self, node_id: str, reason: str = "") -> None:
        """Suspend execution at the current node.

        Called by components like HumanApproval to pause the workflow.
        The executor checks metadata.suspended after each node.
        """
        self.metadata.suspended = True
        self.metadata.suspended_at_node = node_id
        self.metadata.suspension_reason = reason

    def to_dict(self) -> dict[str, Any]:
        """Convert context to dictionary.

        Returns:
            Dictionary representation of context
        """
        return {
            "version": "1.0",
            "flow_id": self.metadata.flow_id,
            "started_at": self.metadata.started_at.isoformat(),
            "completed_at": (
                self.metadata.completed_at.isoformat()
                if self.metadata.completed_at
                else None
            ),
            "data": self.data.to_dict(),
            "metadata": {
                "step_timings": [
                    {
                        "step_index": st.step_index,
                        "component": st.component,
                        "duration": st.duration,
                        "started_at": st.started_at.isoformat(),
                        "execution_order": st.execution_order,
                    }
                    for st in self.metadata.step_timings
                ],
                "component_timings": self.metadata.component_timings,
                "skipped_components": self.metadata.skipped_components,
                "errors": self.metadata.errors,
                "condition_errors": self.metadata.condition_errors,
                "suspended": self.metadata.suspended,
                "suspended_at_node": self.metadata.suspended_at_node,
                "suspension_reason": self.metadata.suspension_reason,
                "completed_nodes": self.metadata.completed_nodes,
                "node_ports": self.metadata.node_ports,
                "pending_nodes": self.metadata.pending_nodes,
                "node_visit_counts": self.metadata.node_visit_counts,
                "iteration_count": self.metadata.iteration_count,
                "loop_timings": self.metadata.loop_timings,
                "max_iterations_reached": self.metadata.max_iterations_reached,
                "converged": self.metadata.converged,
            },
            "input": self.input,
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize context to JSON string.

        Args:
            indent: JSON indentation level

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), default=str, indent=indent)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FlowContext:
        """Create context from dictionary.

        Supports full round-trip: from_dict(to_dict()) preserves all fields.

        Args:
            data: Dictionary with context data

        Returns:
            New FlowContext instance
        """
        context = cls()

        # Restore data
        for key, value in data.get("data", {}).items():
            context.set(key, value)
        context.input = data.get("input")

        # Restore top-level metadata
        if "flow_id" in data:
            context.metadata.flow_id = data["flow_id"]
        if "started_at" in data:
            context.metadata.started_at = datetime.fromisoformat(data["started_at"])
        if "completed_at" in data and data["completed_at"]:
            context.metadata.completed_at = datetime.fromisoformat(data["completed_at"])

        # Restore nested metadata
        metadata_dict = data.get("metadata", {})

        # Restore step timings
        for st in metadata_dict.get("step_timings", []):
            context.metadata.step_timings.append(
                StepTiming(
                    step_index=st["step_index"],
                    component=st["component"],
                    duration=st["duration"],
                    started_at=datetime.fromisoformat(st["started_at"]),
                    execution_order=st.get("execution_order", st["step_index"]),
                )
            )
        # Update step counter to continue from where we left off
        if context.metadata.step_timings:
            context.metadata._step_counter = (
                max(st.execution_order for st in context.metadata.step_timings) + 1
            )

        # Restore skipped components
        context.metadata.skipped_components = list(
            metadata_dict.get("skipped_components", [])
        )

        # Restore errors
        context.metadata.errors = list(metadata_dict.get("errors", []))
        context.metadata.condition_errors = list(
            metadata_dict.get("condition_errors", [])
        )

        # Restore suspension state
        context.metadata.suspended = metadata_dict.get("suspended", False)
        context.metadata.suspended_at_node = metadata_dict.get(
            "suspended_at_node", None
        )
        context.metadata.suspension_reason = metadata_dict.get(
            "suspension_reason", None
        )
        context.metadata.completed_nodes = list(
            metadata_dict.get("completed_nodes", [])
        )
        context.metadata.node_ports = dict(metadata_dict.get("node_ports", {}))
        context.metadata.pending_nodes = list(
            metadata_dict.get("pending_nodes", [])
        )

        # Restore cyclic graph execution state
        context.metadata.node_visit_counts = dict(
            metadata_dict.get("node_visit_counts", {})
        )
        context.metadata.iteration_count = metadata_dict.get("iteration_count", 0)
        context.metadata.loop_timings = list(
            metadata_dict.get("loop_timings", [])
        )
        context.metadata.max_iterations_reached = metadata_dict.get(
            "max_iterations_reached", False
        )
        context.metadata.converged = metadata_dict.get("converged", False)

        return context

    @classmethod
    def from_json(cls, json_str: str) -> FlowContext:
        """Create context from JSON string.

        Args:
            json_str: JSON string representation

        Returns:
            New FlowContext instance
        """
        return cls.from_dict(json.loads(json_str))

    def copy(self) -> FlowContext:
        """Create a shallow copy of the context.

        Returns:
            New FlowContext with copied data
        """
        new_context = FlowContext()
        new_context.data = DotDict(self.data.to_dict())
        new_context.input = self.input
        # Metadata is new for each copy
        return new_context
