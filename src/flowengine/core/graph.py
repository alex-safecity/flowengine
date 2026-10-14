"""FlowEngine graph executor.

This module provides the GraphExecutor class that executes graph-type flows
using topological ordering with port-based routing, and cyclic graphs via
a ready-queue executor.
"""

from __future__ import annotations

import inspect
import logging
import time
from collections import deque
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from flowengine.config.schema import FlowSettings, GraphEdgeConfig, GraphNodeConfig
from flowengine.core.binding import bind_components, seed_flow_namespace
from flowengine.core.component import BaseComponent
from flowengine.core.context import FlowContext
from flowengine.errors import (
    ComponentError,
    ConditionEvaluationError,
    ConfigurationError,
    FlowExecutionError,
    FlowTimeoutError,
    MaxIterationsError,
)
from flowengine.eval.evaluator import ConditionEvaluator

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class GraphExecutor:
    """Executes a graph-type flow using topological ordering with port-based routing.

    For DAG graphs, uses topological sort (Kahn's algorithm) for execution order.
    For cyclic graphs, uses a ready-queue executor with iteration limits.

    Key semantics:
    - Root nodes (no incoming edges) execute first — these are triggers/entry points
    - Port routing: When a component sets active_port, only matching edges propagate
    - Unreachable nodes: If no incoming edge was activated, the node is skipped
    - Completed nodes: Nodes in completed_nodes are skipped (for resume support)
    - Resume: ``execute(context, resume_node=...)`` (or a context whose
      ``metadata.suspended_at_node`` is still set) re-enters the graph at the
      node that suspended. Completed nodes propagate the output port they set
      when they ran (``metadata.node_ports``), so the suspended node is reached
      through the same edges as before and nothing upstream re-executes.
    """

    def __init__(
        self,
        nodes: list[GraphNodeConfig],
        edges: list[GraphEdgeConfig],
        components: dict[str, BaseComponent],
        settings: FlowSettings,
        hooks: list[Any] | None = None,
        evaluator: ConditionEvaluator | None = None,
    ) -> None:
        self._nodes = {n.id: n for n in nodes}
        self._edges = edges
        self._components = components
        self._settings = settings
        self._hooks = hooks or []
        self._evaluator = evaluator or ConditionEvaluator()

        # Adjacency structures
        self._forward: dict[str, list[GraphEdgeConfig]] = {}
        self._reverse: dict[str, list[GraphEdgeConfig]] = {}
        self._build_adjacency()

        # Detect cycles at construction time
        self._has_cycles, self._back_edges = self._detect_cycles()
        self._back_edge_targets: set[str] = {
            target for _, target in self._back_edges
        }

        # Data bindings: in namespaced mode every node runs behind a
        # BoundComponent (its own namespace); legacy flows are not wrapped
        self._bound: dict[str, BaseComponent] | None = bind_components(
            nodes, components
        )

    def execute(
        self, context: FlowContext, *, resume_node: str | None = None
    ) -> FlowContext:
        """Execute the graph flow (synchronous components only).

        Dispatches to the DAG executor for acyclic graphs, or the cyclic
        executor for graphs containing cycles. For graphs whose components have
        async ``process`` coroutines, use :meth:`execute_async`.

        Args:
            context: Fresh context, or one restored from a suspended run.
            resume_node: Node to re-enter at after a suspension. Defaults to
                ``context.metadata.suspended_at_node`` when that is still set;
                ``None`` with no suspended node starts from the roots.
        """
        resume_node = self._begin(context, resume_node)
        if self._has_cycles:
            return self._execute_cyclic(context, resume_node)
        return self._execute_dag(context, resume_node)

    async def execute_async(
        self, context: FlowContext, *, resume_node: str | None = None
    ) -> FlowContext:
        """Execute the graph flow, awaiting components with coroutine ``process``.

        Mirrors :meth:`execute` but supports both sync and async components: a
        component whose ``process`` returns a coroutine is awaited; sync
        components run inline. Routing, port/condition gating, cycle handling,
        suspension, resume and hooks are identical to the sync path.
        """
        resume_node = self._begin(context, resume_node)
        if self._has_cycles:
            return await self._execute_cyclic_async(context, resume_node)
        return await self._execute_dag_async(context, resume_node)

    def _begin(self, context: FlowContext, resume_node: str | None) -> str | None:
        """Resolve the resume node and clear the suspension flags for a resume.

        The suspended node is re-executed (it re-processes with any
        ``resume_data``), so the flags it set last time are cleared here; the
        node may suspend again. Returns ``None`` for a fresh run.
        """
        if resume_node is None:
            resume_node = context.metadata.suspended_at_node
        if resume_node is None:
            if self._bound is not None:
                seed_flow_namespace(context, list(self._nodes.values()))
            return None
        if resume_node not in self._nodes:
            # Contexts suspended before 0.7 may carry the component name the
            # component passed to ``suspend()``; accept it when unambiguous
            by_component = [
                nid for nid, n in self._nodes.items() if n.component == resume_node
            ]
            if len(by_component) != 1:
                raise FlowExecutionError(
                    f"Cannot resume: unknown node '{resume_node}'"
                )
            resume_node = by_component[0]
        context.metadata.suspended = False
        context.metadata.suspended_at_node = None
        context.metadata.suspension_reason = None
        return resume_node

    def _execute_node(
        self,
        node_id: str,
        node: GraphNodeConfig,
        context: FlowContext,
        flow_start_time: float,
    ) -> FlowContext:
        """Execute a single node with full lifecycle.

        Handles: hooks (pre/post), timing, error policies,
        deadline checking, port clearing, setup/process/teardown.
        Used by both DAG and cyclic executors.

        Returns:
            Updated FlowContext after node execution.

        Raises:
            FlowTimeoutError: If flow timeout exceeded before node starts.
            ComponentError: If node fails and error policy is 'fail'.
        """
        component = self._component_for(node_id, node)

        # Calculate remaining timeout
        elapsed = time.time() - flow_start_time
        if self._settings.timeout_seconds:
            remaining = self._settings.timeout_seconds - elapsed
            if remaining <= 0:
                raise FlowTimeoutError(
                    f"Flow timeout exceeded: {elapsed:.2f}s > "
                    f"{self._settings.timeout_seconds}s",
                    timeout=self._settings.timeout_seconds,
                    elapsed=elapsed,
                    flow_id=context.metadata.flow_id,
                    step=node_id,
                )
        else:
            remaining = None

        # Clear port before execution
        context.clear_port()

        self._notify("on_node_start", node_id, node.component, context)

        start_time = time.time()
        step_started_at = datetime.now(UTC)

        try:
            # Set deadline for cooperative timeout
            if remaining is not None:
                context.metadata.deadline = time.time() + remaining
            else:
                context.metadata.deadline = None
            context.metadata.deadline_checked = False

            # Execute component
            component.setup(context)
            try:
                context = component.process(context)
            finally:
                component.teardown(context)
                context.metadata.deadline = None
            self._record_suspension(node_id, context)

            node_elapsed = time.time() - start_time
            context.metadata.record_timing(
                node.component, node_elapsed, step_started_at
            )
            self._notify(
                "on_node_complete",
                node_id,
                node.component,
                context,
                node_elapsed,
            )

            logger.info(f"Completed node {node_id} in {node_elapsed:.3f}s")

        except FlowTimeoutError:
            node_elapsed = time.time() - start_time
            context.metadata.record_timing(
                node.component, node_elapsed, step_started_at
            )
            raise
        except Exception as e:
            node_elapsed = time.time() - start_time
            context.metadata.record_timing(
                node.component, node_elapsed, step_started_at
            )
            context.metadata.add_error(node.component, e)

            self._notify(
                "on_node_error", node_id, node.component, e, context
            )

            logger.error(f"Error in node {node_id}: {e}")

            if node.on_error == "fail" or self._settings.fail_fast:
                raise ComponentError(
                    component=node.component,
                    message=str(e),
                    original_error=e,
                ) from e
            elif node.on_error == "skip":
                context.metadata.skipped_components.append(node.component)
            # on_error == "continue" falls through
        finally:
            context.metadata.deadline = None
            context.metadata.deadline_checked = False

        return context

    def _component_for(self, node_id: str, node: GraphNodeConfig) -> BaseComponent:
        """The component to run for a node: bound in namespaced mode, raw otherwise."""
        component = (
            self._bound.get(node_id) if self._bound is not None
            else self._components.get(node.component)
        )
        if not component:
            raise FlowExecutionError(
                f"Component not found for node '{node_id}': {node.component}"
            )
        return component

    @staticmethod
    def _record_suspension(node_id: str, context: FlowContext) -> None:
        """Pin ``suspended_at_node`` to the executing node's id.

        Components call ``context.suspend(name, ...)`` without knowing which
        node they run as; the executor does, and resume needs the node id.
        """
        if context.metadata.suspended:
            context.metadata.suspended_at_node = node_id

    def _execute_dag(
        self, context: FlowContext, resume_node: str | None = None
    ) -> FlowContext:
        """Execute a DAG flow using topological ordering.

        On resume, completed nodes are skipped but propagate the port they set
        when they ran, and ``resume_node`` is activated explicitly, so execution
        re-enters exactly where it suspended.
        """
        flow_start_time = time.time()

        # Get execution order
        order = self._topological_sort()

        # Track which nodes are reachable (activated by an incoming edge)
        # Root nodes are always reachable; so is the node being resumed
        roots = set(self._find_roots())
        activated: set[str] = set(roots)
        if resume_node is not None:
            activated.add(resume_node)

        for node_id in order:
            node = self._nodes[node_id]

            # Skip already-completed nodes (resume support)
            # But still propagate activation to downstream nodes, through the
            # port the node set when it ran
            if node_id in context.metadata.completed_nodes:
                logger.debug(f"Skipping already-completed node: {node_id}")
                stored_port = context.metadata.node_ports.get(node_id)
                reachable_targets = self._get_reachable_targets(
                    node_id, stored_port, context
                )
                activated.update(reachable_targets)
                continue

            # Skip unreachable nodes
            if node_id not in activated:
                logger.info(f"Skipping unreachable node: {node_id}")
                context.metadata.skipped_components.append(node.component)
                self._notify(
                    "on_node_skipped", node_id, node.component, "unreachable"
                )
                continue

            # Execute the node
            context = self._execute_node(node_id, node, context, flow_start_time)

            # Check for suspension — do NOT mark node as completed so it
            # re-runs on resume (e.g., HumanApproval re-processes with
            # resume_data to see the approval decision)
            if context.metadata.suspended:
                self._notify(
                    "on_flow_suspended",
                    node_id,
                    context.metadata.suspension_reason or "",
                    None,
                )
                return context

            # Mark node completed only after successful non-suspended execution
            context.metadata.completed_nodes.append(node_id)

            # Determine which downstream nodes to activate based on port, and
            # remember the port so a resume activates the same edges
            active_port = context.get_active_port()
            context.metadata.node_ports[node_id] = active_port
            reachable_targets = self._get_reachable_targets(node_id, active_port, context)
            activated.update(reachable_targets)

        return context

    def _execute_cyclic(
        self, context: FlowContext, resume_node: str | None = None
    ) -> FlowContext:
        """Execute a cyclic graph using a ready-queue executor.

        Uses a BFS-style ready queue. Iteration count increments each time
        execution re-enters a back-edge target. Stops when:
        - The ready queue is empty (natural termination via port routing)
        - max_iterations is exceeded (controlled by on_max_iterations policy)
        - A per-node max_visits limit is reached
        - Global timeout expires

        On resume the queue holds the suspended node followed by the nodes
        that were still queued when it suspended (``metadata.pending_nodes``);
        the roots are never re-enqueued. A suspended execution is not a visit:
        ``node_visit_counts`` counts completed executions only, so the resumed
        node is neither double-counted nor exhausted by its own suspension.
        """
        flow_start_time = time.time()

        # Identify which nodes participate in cycles
        cycle_nodes = self._identify_cycle_nodes()

        # Find roots: nodes with no incoming non-back-edges
        roots = self._find_roots_for_cyclic()

        # Initialize or resume visit tracking
        visit_counts: dict[str, int] = dict(context.metadata.node_visit_counts)
        iteration = context.metadata.iteration_count

        # Build ready queue — on resume, start from the suspended node and
        # whatever was still queued behind it
        ready_queue: deque[str] = self._initial_queue(context, resume_node, roots)
        re_entering = resume_node is not None

        iter_start: float | None = None  # Track iteration start for duration

        while ready_queue:
            node_id = ready_queue.popleft()
            node = self._nodes[node_id]

            # On re-entry the suspended node already passed the visit and
            # iteration checks before it ran; do not apply them twice
            if re_entering and node_id == resume_node:
                re_entering = False
            else:
                # 1. Check per-node visit limit
                effective_max = self._effective_max_visits(node_id)
                if visit_counts.get(node_id, 0) >= effective_max:
                    continue  # This path is exhausted

                # 2. Check iteration limit (only at back-edge targets that have
                #    already been visited — first visit is not a "loop")
                if (
                    node_id in self._back_edge_targets
                    and visit_counts.get(node_id, 0) > 0
                ):
                    iteration, iter_start = self._enter_iteration(
                        context, node_id, iteration, iter_start, flow_start_time
                    )
                    if iteration > self._settings.max_iterations:
                        self._handle_max_iterations(context, node_id, iteration)
                        break

            # 3. Execute the node
            context = self._execute_node(node_id, node, context, flow_start_time)

            # 4. Handle suspension (checkpoint/resume) — the queue is kept so
            #    the resume continues the fan-out; the visit is not counted
            if context.metadata.suspended:
                context.metadata.pending_nodes = list(ready_queue)
                self._notify(
                    "on_flow_suspended",
                    node_id,
                    context.metadata.suspension_reason or "",
                    None,
                )
                return context

            # 5. Update visit tracking
            visit_counts[node_id] = visit_counts.get(node_id, 0) + 1
            context.metadata.node_visit_counts = dict(visit_counts)

            # 6. Track completion for non-cycle nodes only
            # Cycle-participating nodes use visit_counts, not completed_nodes
            if node_id not in cycle_nodes:
                context.metadata.completed_nodes.append(node_id)

            # 7. Enqueue downstream based on port routing
            active_port = context.get_active_port()
            context.metadata.node_ports[node_id] = active_port
            for target in self._get_reachable_targets(node_id, active_port, context):
                ready_queue.append(target)

        # Fire on_iteration_complete for the final iteration (natural exit)
        if iteration > 0 and not context.metadata.suspended:
            iter_end = time.time()
            # Use the last back-edge target as entry node, or first root
            entry_node = (
                list(self._back_edge_targets)[0]
                if self._back_edge_targets else roots[0]
            )
            self._notify(
                "on_iteration_complete", iteration, entry_node, context,
                iter_end - (iter_start if iter_start else flow_start_time),
            )

        return context

    def _initial_queue(
        self, context: FlowContext, resume_node: str | None, roots: list[str]
    ) -> deque[str]:
        """Ready queue for a cyclic run: the roots, or the resume node plus
        the nodes that were still queued when the graph suspended."""
        if resume_node is None:
            return deque(roots)
        queue: deque[str] = deque([resume_node])
        queue.extend(context.metadata.pending_nodes)
        context.metadata.pending_nodes = []
        return queue

    def _enter_iteration(
        self,
        context: FlowContext,
        node_id: str,
        iteration: int,
        iter_start: float | None,
        flow_start_time: float,
    ) -> tuple[int, float]:
        """Close the previous iteration and open the next at a back-edge target."""
        iter_end = time.time()
        self._notify(
            "on_iteration_complete", iteration, node_id, context,
            iter_end - (iter_start if iter_start else flow_start_time),
        )
        iteration += 1
        context.metadata.iteration_count = iteration
        iter_start = time.time()
        self._notify("on_iteration_start", iteration, node_id, context)
        return iteration, iter_start

    # ── Async mirrors (support components with coroutine `process`) ──────
    # These mirror the sync _execute_node/_execute_dag/_execute_cyclic above;
    # the only behavioral difference is awaiting a coroutine-returning process.
    # Sync components work here too (a non-coroutine result is used as-is).

    async def _execute_node_async(
        self,
        node_id: str,
        node: GraphNodeConfig,
        context: FlowContext,
        flow_start_time: float,
    ) -> FlowContext:
        """Async twin of :meth:`_execute_node` (awaits coroutine ``process``)."""
        component = self._component_for(node_id, node)

        elapsed = time.time() - flow_start_time
        if self._settings.timeout_seconds:
            remaining = self._settings.timeout_seconds - elapsed
            if remaining <= 0:
                raise FlowTimeoutError(
                    f"Flow timeout exceeded: {elapsed:.2f}s > "
                    f"{self._settings.timeout_seconds}s",
                    timeout=self._settings.timeout_seconds,
                    elapsed=elapsed,
                    flow_id=context.metadata.flow_id,
                    step=node_id,
                )
        else:
            remaining = None

        context.clear_port()
        self._notify("on_node_start", node_id, node.component, context)

        start_time = time.time()
        step_started_at = datetime.now(UTC)

        try:
            if remaining is not None:
                context.metadata.deadline = time.time() + remaining
            else:
                context.metadata.deadline = None
            context.metadata.deadline_checked = False

            component.setup(context)
            try:
                result = component.process(context)
                if inspect.iscoroutine(result):
                    result = await result
                context = result
            finally:
                component.teardown(context)
                context.metadata.deadline = None
            self._record_suspension(node_id, context)

            node_elapsed = time.time() - start_time
            context.metadata.record_timing(node.component, node_elapsed, step_started_at)
            self._notify(
                "on_node_complete", node_id, node.component, context, node_elapsed
            )
            logger.info(f"Completed node {node_id} in {node_elapsed:.3f}s")

        except FlowTimeoutError:
            node_elapsed = time.time() - start_time
            context.metadata.record_timing(node.component, node_elapsed, step_started_at)
            raise
        except Exception as e:
            node_elapsed = time.time() - start_time
            context.metadata.record_timing(node.component, node_elapsed, step_started_at)
            context.metadata.add_error(node.component, e)
            self._notify("on_node_error", node_id, node.component, e, context)
            logger.error(f"Error in node {node_id}: {e}")
            if node.on_error == "fail" or self._settings.fail_fast:
                raise ComponentError(
                    component=node.component, message=str(e), original_error=e
                ) from e
            elif node.on_error == "skip":
                context.metadata.skipped_components.append(node.component)
        finally:
            context.metadata.deadline = None
            context.metadata.deadline_checked = False

        return context

    async def _execute_dag_async(
        self, context: FlowContext, resume_node: str | None = None
    ) -> FlowContext:
        """Async twin of :meth:`_execute_dag`."""
        flow_start_time = time.time()
        order = self._topological_sort()
        roots = set(self._find_roots())
        activated: set[str] = set(roots)
        if resume_node is not None:
            activated.add(resume_node)

        for node_id in order:
            node = self._nodes[node_id]
            if node_id in context.metadata.completed_nodes:
                stored_port = context.metadata.node_ports.get(node_id)
                activated.update(
                    self._get_reachable_targets(node_id, stored_port, context)
                )
                continue
            if node_id not in activated:
                logger.info(f"Skipping unreachable node: {node_id}")
                context.metadata.skipped_components.append(node.component)
                self._notify("on_node_skipped", node_id, node.component, "unreachable")
                continue

            context = await self._execute_node_async(
                node_id, node, context, flow_start_time
            )
            if context.metadata.suspended:
                self._notify(
                    "on_flow_suspended", node_id,
                    context.metadata.suspension_reason or "", None,
                )
                return context

            context.metadata.completed_nodes.append(node_id)
            active_port = context.get_active_port()
            context.metadata.node_ports[node_id] = active_port
            activated.update(
                self._get_reachable_targets(node_id, active_port, context)
            )
        return context

    async def _execute_cyclic_async(
        self, context: FlowContext, resume_node: str | None = None
    ) -> FlowContext:
        """Async twin of :meth:`_execute_cyclic`."""
        flow_start_time = time.time()
        cycle_nodes = self._identify_cycle_nodes()
        roots = self._find_roots_for_cyclic()
        visit_counts: dict[str, int] = dict(context.metadata.node_visit_counts)
        iteration = context.metadata.iteration_count

        ready_queue: deque[str] = self._initial_queue(context, resume_node, roots)
        re_entering = resume_node is not None

        iter_start: float | None = None
        while ready_queue:
            node_id = ready_queue.popleft()
            node = self._nodes[node_id]

            if re_entering and node_id == resume_node:
                re_entering = False
            else:
                if visit_counts.get(node_id, 0) >= self._effective_max_visits(node_id):
                    continue

                if (
                    node_id in self._back_edge_targets
                    and visit_counts.get(node_id, 0) > 0
                ):
                    iteration, iter_start = self._enter_iteration(
                        context, node_id, iteration, iter_start, flow_start_time
                    )
                    if iteration > self._settings.max_iterations:
                        self._handle_max_iterations(context, node_id, iteration)
                        break

            context = await self._execute_node_async(
                node_id, node, context, flow_start_time
            )

            if context.metadata.suspended:
                context.metadata.pending_nodes = list(ready_queue)
                self._notify(
                    "on_flow_suspended", node_id,
                    context.metadata.suspension_reason or "", None,
                )
                return context

            visit_counts[node_id] = visit_counts.get(node_id, 0) + 1
            context.metadata.node_visit_counts = dict(visit_counts)

            if node_id not in cycle_nodes:
                context.metadata.completed_nodes.append(node_id)

            active_port = context.get_active_port()
            context.metadata.node_ports[node_id] = active_port
            for target in self._get_reachable_targets(node_id, active_port, context):
                ready_queue.append(target)

        if iteration > 0 and not context.metadata.suspended:
            iter_end = time.time()
            entry_node = (
                list(self._back_edge_targets)[0]
                if self._back_edge_targets else roots[0]
            )
            self._notify(
                "on_iteration_complete", iteration, entry_node, context,
                iter_end - (iter_start if iter_start else flow_start_time),
            )
        return context

    # ── Helper methods ──────────────────────────────────────────────────

    def _build_adjacency(self) -> None:
        """Build forward (successors) and reverse (predecessors) adjacency lists."""
        for node_id in self._nodes:
            self._forward[node_id] = []
            self._reverse[node_id] = []

        for edge in self._edges:
            self._forward[edge.source].append(edge)
            self._reverse[edge.target].append(edge)

    def _find_roots(self) -> list[str]:
        """Find nodes with no incoming edges (entry points for DAG)."""
        return [
            node_id
            for node_id in self._nodes
            if len(self._reverse[node_id]) == 0
        ]

    def _find_roots_for_cyclic(self) -> list[str]:
        """Find nodes with no incoming non-back-edges (entry points for cyclic graphs).

        A node is a root if it has no incoming edges, or if all its incoming
        edges are back-edges. This handles self-loops correctly: a self-looping
        node with no other incoming edges is still a root.
        """
        roots: list[str] = []
        for node_id in self._nodes:
            incoming = self._reverse.get(node_id, [])
            # Check if all incoming edges are back-edges
            non_back_incoming = [
                e for e in incoming
                if (e.source, e.target) not in self._back_edges
            ]
            if not non_back_incoming:
                roots.append(node_id)
        return roots

    def _topological_sort(self) -> list[str]:
        """Kahn's algorithm — returns execution order.

        Raises:
            ConfigurationError: If cycle detected.
        """
        in_degree: dict[str, int] = dict.fromkeys(self._nodes, 0)
        for edge in self._edges:
            in_degree[edge.target] += 1

        queue: deque[str] = deque()
        for nid, degree in in_degree.items():
            if degree == 0:
                queue.append(nid)

        order: list[str] = []
        while queue:
            node_id = queue.popleft()
            order.append(node_id)
            for edge in self._forward[node_id]:
                in_degree[edge.target] -= 1
                if in_degree[edge.target] == 0:
                    queue.append(edge.target)

        if len(order) != len(self._nodes):
            raise ConfigurationError(
                "Cycle detected in graph flow",
                details=[
                    f"Processed {len(order)} of {len(self._nodes)} nodes. "
                    f"Remaining nodes are part of a cycle."
                ],
            )

        return order

    def _get_reachable_targets(
        self, node_id: str, active_port: str | None, context: FlowContext | None = None
    ) -> list[str]:
        """Given a node and its active port, return which downstream nodes to activate.

        Port rules:
        - Edges with port=None always pass the port check (unconditional)
        - If node has port-specific edges and active_port is set:
          only edges matching active_port pass (plus unconditional)
        - If node has port-specific edges but no active_port:
          only unconditional edges pass
        - If node has no port-specific edges: all outgoing edges pass

        Condition rule (applied after the port check): an edge with a
        ``condition`` expression activates only if it evaluates True against
        ``context``. Evaluation errors follow ``settings.on_condition_error``.
        """
        outgoing = self._forward.get(node_id, [])
        if not outgoing:
            return []

        has_port_edges = any(e.port is not None for e in outgoing)

        targets: list[str] = []
        for edge in outgoing:
            if edge.port is None:
                port_ok = True
            elif has_port_edges and active_port is not None:
                port_ok = edge.port == active_port
            elif not has_port_edges:
                port_ok = True
            else:
                port_ok = False
            if port_ok and self._edge_condition_ok(edge, context):
                targets.append(edge.target)

        return targets

    def _edge_condition_ok(
        self, edge: GraphEdgeConfig, context: FlowContext | None
    ) -> bool:
        """Evaluate an edge's condition expression (True if no condition).

        Errors are handled per ``settings.on_condition_error``:
        ``fail`` re-raises, ``skip``/``warn`` treat the edge as inactive
        (``warn`` also logs and records the error).
        """
        if not edge.condition:
            return True
        if context is None:
            return True
        try:
            return self._evaluator.evaluate(edge.condition, context)
        except ConditionEvaluationError as exc:
            policy = self._settings.on_condition_error
            if policy == "fail":
                raise
            context.metadata.add_condition_error(
                f"{edge.source}->{edge.target}", exc, edge.condition
            )
            if policy == "warn":
                logger.warning(
                    "Edge condition error %s->%s: %s (treating edge as inactive)",
                    edge.source, edge.target, exc,
                )
            return False

    def _detect_cycles(self) -> tuple[bool, set[tuple[str, str]]]:
        """Detect cycles and identify back-edges using DFS.

        Uses white/gray/black coloring:
        - white: unvisited
        - gray: currently in DFS stack (ancestor)
        - black: fully explored

        An edge to a gray node is a back-edge (creates a cycle).

        Returns:
            Tuple of (has_cycles, back_edges) where back_edges is a set of
            (source_id, target_id) tuples that create cycles.
        """
        white, gray, black = 0, 1, 2
        color: dict[str, int] = dict.fromkeys(self._nodes, white)
        back_edges: set[tuple[str, str]] = set()

        def dfs(node_id: str) -> None:
            color[node_id] = gray
            for edge in self._forward.get(node_id, []):
                target = edge.target
                if color[target] == gray:
                    # Back-edge: target is an ancestor in current DFS path
                    back_edges.add((node_id, target))
                elif color[target] == white:
                    dfs(target)
            color[node_id] = black

        for node_id in self._nodes:
            if color[node_id] == white:
                dfs(node_id)

        return (len(back_edges) > 0, back_edges)

    def _identify_cycle_nodes(self) -> set[str]:
        """Find all nodes that participate in any cycle.

        A node participates in a cycle if it lies on a path from a
        back-edge target back to a back-edge source (following forward edges).
        """
        if not self._back_edges:
            return set()

        cycle_nodes: set[str] = set()
        for source, target in self._back_edges:
            # Find all nodes on paths from target to source (the cycle body)
            # BFS from target, looking for source, through forward edges
            # (excluding the back-edge itself)
            visited: set[str] = set()
            queue: deque[str] = deque([target])
            parent: dict[str, str | None] = {target: None}

            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)

                if current == source:
                    # Trace path from target to source — all nodes are cycle nodes
                    path_node: str | None = source
                    while path_node is not None:
                        cycle_nodes.add(path_node)
                        path_node = parent.get(path_node)
                    break

                for edge in self._forward.get(current, []):
                    # Don't follow this back-edge
                    if (edge.source, edge.target) in self._back_edges:
                        continue
                    if edge.target not in visited:
                        parent[edge.target] = current
                        queue.append(edge.target)

        return cycle_nodes

    def _effective_max_visits(self, node_id: str) -> int:
        """Return the effective visit limit for a node.

        Uses per-node max_visits if set, otherwise flow-level max_iterations.
        """
        node = self._nodes[node_id]
        if node.max_visits is not None:
            return node.max_visits
        return self._settings.max_iterations

    def _handle_max_iterations(
        self, context: FlowContext, node_id: str, iteration: int
    ) -> None:
        """Handle reaching max_iterations based on policy."""
        context.metadata.max_iterations_reached = True

        # Fire on_max_iterations hook before applying policy
        self._notify("on_max_iterations", self._settings.max_iterations, node_id, context)

        if self._settings.on_max_iterations == "fail":
            raise MaxIterationsError(
                f"Cycle at node '{node_id}' reached "
                f"max_iterations={self._settings.max_iterations}",
                max_iterations=self._settings.max_iterations,
                actual_iterations=iteration,
                cycle_entry_node=node_id,
                flow_id=context.metadata.flow_id,
            )
        elif self._settings.on_max_iterations == "warn":
            logger.warning(
                "Max iterations reached: node=%s max=%d actual=%d",
                node_id,
                self._settings.max_iterations,
                iteration,
            )
        # "exit" silently stops

    def _notify(self, method: str, *args: Any, **kwargs: Any) -> None:
        """Notify all registered hooks."""
        for hook in self._hooks:
            fn = getattr(hook, method, None)
            if fn:
                try:
                    fn(*args, **kwargs)
                except Exception:
                    pass  # hooks must not break execution
