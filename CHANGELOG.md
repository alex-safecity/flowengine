# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-10-14

Correct **resume** in the `GraphExecutor`. In 0.6.0 a suspended graph could only
be resumed by re-running it from the roots: the DAG executor skipped completed
nodes but did not know which port they had set, so a suspended node behind a
port edge was silently skipped and the run reported complete; the cyclic
executor restarted from the roots when the caller cleared `suspended_at_node`,
re-executing everything upstream of the suspension. Both are fixed; the
executor now re-enters the graph at the node that suspended.

### Added

- **`GraphExecutor.execute(context, resume_node=...)`** and
  `execute_async(..., resume_node=...)`. The resume node defaults to
  `context.metadata.suspended_at_node` when still set, so a restored context
  resumes without the caller clearing anything; the executor clears the
  suspension flags itself before re-executing the node (which may suspend
  again). An unknown node raises `FlowExecutionError`; a component name (what
  pre-0.7 contexts may carry) is mapped to its node when unambiguous.
- **`metadata.node_ports`** — the output port each completed node set when it
  ran (`None` when it set none). On resume, completed nodes propagate through
  these ports instead of through unconditional edges only.
- **`metadata.pending_nodes`** — the nodes still in the cyclic executor's ready
  queue when the graph suspended; the resume re-enqueues them after the
  suspended node so a fan-out in progress completes. Both fields round-trip
  through `FlowContext.to_dict()` / `from_dict()` and default to empty for
  contexts saved by earlier versions.

- **Data bindings / per-node namespaces** (`flowengine.core.binding`). A graph
  node may carry a `bind:` block — `inputs` map a key the component reads to a
  reference `<node_id>.<key>[.path]` (`flow.<key>` for flow inputs), a literal
  `{ value: ... }` or a safe-AST expression `{ expr: ... }`; `outputs` may
  rename a produced key within the node's own namespace. A flow with a `bind`
  on any node runs in **namespaced mode**: every node executes behind a
  `BoundComponent` on a `ScopedContext` (reads fall through to the parent,
  writes land in `context.data.<node_id>`), two instances of one component no
  longer collide, a node sees its own earlier outputs on later visits (loop
  state), unbound nodes are mirrored to raw keys for consumers that predate
  bindings, and the raw initial keys are snapshotted as `context.data.flow`.
  Components and skills are untouched. Flows without `bind` are not wrapped and
  behave byte-identically. `GraphNodeConfig` also gains `provenance` (a
  per-input provenance class carried for policy, not interpreted by the engine).
- **Binding checks in `validate_semantics`** (`flowengine.agent.bindings`), with
  repair patches: `UNKNOWN_BINDING_NODE`, `UNDECLARED_BINDING_KEY`,
  `BINDING_NOT_UPSTREAM` (a warning when the source only reaches the node
  through a cycle), `BINDING_FOREIGN_TARGET`, `UNBOUND_INPUT`,
  `BINDING_TYPE_MISMATCH`, `BINDING_EXPR_INVALID`.
- `ComponentMeta.data_classes` — the data classes a component may emit or
  handle (policy vocabulary), exported in the catalog entry.

### Changed

- **`metadata.suspended_at_node` is always the node id.** Components call
  `context.suspend(name, ...)` without knowing their node; the executor now
  records the id of the node it was executing. Code that compared the field
  with a component name must compare with the node id.
- **A suspended execution is not a visit.** The cyclic executor increments
  `node_visit_counts` only after a completed (non-suspended) execution, so a
  node is neither double-counted nor exhausted (`max_visits: 1`) by its own
  suspension, and a resume at a back-edge target does not open a second
  iteration for the same re-entry.
- `FlowEngine.resume()` passes the checkpoint's suspended node to the executor
  instead of clearing it.

## [0.6.0] - 2026-06-15

Richer graph execution: edge **conditions** and **async** components in the
`GraphExecutor`. All additions are non-breaking — the synchronous `execute()`
path is unchanged, and existing flows behave identically.

### Added

- **Edge conditions** — `GraphEdgeConfig` gains an optional `condition` Python
  expression (evaluated against `context`, e.g. `context.data.score > 0.5`). An
  edge activates only when its **port matches** *and* its **condition is True**.
  Evaluation errors follow `FlowSettings.on_condition_error` (`fail` re-raises;
  `skip`/`warn` deactivate the edge, recording it in
  `metadata.condition_errors`). `GraphExecutor` now accepts an optional
  `evaluator` (wired from `FlowEngine.evaluator`).
- **Async graph execution** — `GraphExecutor.execute_async()` plus async mirrors
  (`_execute_dag_async`, `_execute_cyclic_async`, `_execute_node_async`) that
  `await` components whose `process` returns a coroutine. Sync components run
  inline on the async path too, so a graph may mix sync and async components.
  Routing, port/condition gating, cyclic/`max_iterations` handling, suspension,
  and hooks are identical to the sync path — letting higher-level runtimes (e.g.
  NeuroCore) route async-skill graph flows through the canonical graph engine.

### Notes

- Port-based routing and cyclic execution with `max_iterations` / per-node
  `max_visits` were already present (v0.2.0 / v0.3.0); this release adds the
  missing condition + async pieces so realistic conditional, looping, async
  graphs execute end-to-end.

## [0.5.0] - 2026-06-13

Agent-native release: FlowEngine YAML becomes a constrained **Agent Workflow IR**
that AI agents (and NeuroCore) can generate, validate, run, observe, and repair.
All additions are non-breaking — existing flows, components, and the public API
are unchanged. See `design/v0.5.0_agent_native.md`. New API lives under
`flowengine.agent` and is re-exported from the top-level `flowengine` package.

### Added

- **Component & contract metadata** (`flowengine.agent.meta`)
  - `ComponentMeta` capability manifest (inputs/outputs/ports/tags/cost/risk/effects/
    requires_approval/requires_llm); `BaseComponent.get_meta()` (optional, defaults to `None`).
  - `IOFieldSpec` + top-level `inputs`/`outputs` on `FlowConfig` — a declarative worker contract.
  - `PortSpec` for declaring named output ports.

- **Machine-readable validation** (`flowengine.agent.issues`, `semantic`, `compiler`)
  - `FlowIssue` (stable `IssueCode`, document `path`, `suggestion`), `RepairSuggestion`
    + `JsonPatchOp` (RFC-6902) for self-correcting agents.
  - `validate_semantics()` — ports declared, reachability, cycle exits, terminal output,
    input producers, output contract, approval surfacing. Degrades to warnings without metadata.
  - `FlowCompiler.compile_yaml()` → `CompileResult{valid, flow_config, errors, warnings,
    normalized_yaml}` with "did you mean" component suggestions.

- **Agent tooling** (`flowengine.agent`)
  - `explain()`/`FlowPlan` — dry-run plan (execution order, branches, cycles, required
    components, context I/O).
  - `AgentTrace.from_context()` — structured run trace (run_id, status, outputs, steps, errors).
  - `normalize_yaml()` — canonical, diff-friendly YAML.
  - `build_catalog()` — machine-readable component catalog.
  - `export_json_schema()` — JSON Schema export (flow/component/graph/component-meta) for
    constrained generation.
  - `apply_patch()` — minimal RFC-6902 JSON Patch applier.

- **Safety / sandbox** (`flowengine.agent.policy`)
  - `ExecutionPolicy` (allow/deny lists, risk & approval gates, resource caps) enforced
    statically at compile time and at runtime via `apply_to_config()`.
  - `PolicyViolationError`.

- **Composition**
  - `FlowTool` — expose a whole flow as a callable, schema-bearing tool (flow-as-tool).
  - `SubflowComponent` (`flowengine.contrib.subflow`) — run a nested flow as a component,
    with input/output mapping, namespacing, and recursion guard.

- **Templates, replay & docs**
  - 7 canonical flow templates + `flowengine.agent.templates` (`list_templates`/`get_template`).
  - `RunRecord`/`RunStore`/`InMemoryRunStore` + `replay()` for deterministic replay.
  - `docs/for-agents/` prompt pack (system-prompt, yaml-generation-rules,
    validation-error-repair, component-selection, safety-policy).

- **CLI** — new `flowengine` console script: `validate`, `plan`, `schema`, `normalize`,
  `apply-patch`, `components`, `template`, `run`, `replay` (most support `--json`).

- **NeuroCore bridge** — NeuroCore `Skill.get_meta()` now derives a `ComponentMeta` from
  its `SkillMeta` (`provides→outputs`, `consumes→inputs`, tags/config_schema/requires_llm);
  `SkillRegistry.component_catalog()` surfaces skills in the FlowEngine catalog. Requires
  `flowengine>=0.5.0`.

## [0.4.1] - 2026-02-27

### Fixed

- **`on_iteration_complete` hook now fires** — called after each iteration finishes (at back-edge re-entry for the previous iteration, and at natural loop exit for the final iteration). Includes iteration number, cycle entry node, context, and duration.
- **`on_max_iterations` hook now fires** — called when the iteration limit is reached, before the policy (fail/exit/warn) is applied. Includes max_iterations value, cycle entry node, and context.

## [0.4.0] - 2026-02-27

### Added

- **Cyclic Graph Execution** (`max_iterations` + ready-queue BFS)
  - Graphs with cycles are now executed via a ready-queue BFS executor
  - Cycle detection via DFS with white/gray/black coloring identifies back-edges
  - Iteration counting at back-edge targets with configurable `max_iterations` limit
  - Three `on_max_iterations` policies: `fail` (raise `MaxIterationsError`), `exit` (silent stop), `warn` (log + stop)
  - Per-node `max_visits` limit to cap individual node executions
  - DAG fast path preserved untouched — zero behavioral change for acyclic graphs

- **`MaxIterationsError` Exception**
  - New exception with `max_iterations`, `actual_iterations`, and `cycle_entry_node` attributes
  - Raised when `on_max_iterations: fail` and the iteration limit is reached

- **Cyclic Execution Metadata**
  - `ExecutionMetadata.node_visit_counts`: per-node visit tracking (source of truth for cyclic flows)
  - `ExecutionMetadata.iteration_count`: total iteration count across all cycles
  - `ExecutionMetadata.max_iterations_reached`: flag indicating whether the limit was hit
  - Full round-trip serialization of cyclic state via `to_dict()`/`from_dict()`

- **Execution Hooks for Cycles**
  - `on_iteration_start(iteration, context)`: called at each iteration boundary
  - `on_iteration_complete(iteration, context)`: called after each iteration completes
  - `on_max_iterations(iteration, policy, context)`: called when max iterations is reached

- **Checkpoint/Resume in Cyclic Flows**
  - Suspend and resume works within cyclic execution paths
  - Visit counts and iteration state preserved across checkpoint boundaries

- **New Flow Settings**
  - `max_iterations` (default: 10): maximum iteration count for cyclic graphs
  - `on_max_iterations` (default: `"fail"`): policy when limit is reached
  - `max_visits` on `GraphNodeConfig`: per-node visit cap

- **Agent Loop Example** (`examples/agent_loop.py`)
  - Runnable example demonstrating `plan → execute → observe → evaluate → [refine|deliver]`
  - YAML configuration in `examples/flows/agent_loop.yaml`

### Changed

- `GraphExecutor.execute()` now dispatches to `_execute_dag()` or `_execute_cyclic()` based on cycle detection
- Previous `execute()` renamed to `_execute_dag()` (no behavioral change for DAG flows)
- Shared `_execute_node()` method extracted for both DAG and cyclic paths
- Cycle-participating nodes are NOT added to `completed_nodes` (only terminal/non-cycle nodes are)
- `FlowEngine.dry_run()` returns all components for cyclic graphs instead of topological sort

## [0.2.0] - 2025-02-10

### Added

- **Graph-Based DAG Execution** (`flow.type: "graph"`)
  - Topological ordering via Kahn's algorithm
  - `GraphNodeConfig` and `GraphEdgeConfig` schema models
  - `GraphExecutor` class in `core/graph.py`
  - Cycle detection with clear error messages

- **Port-Based Output Routing**
  - Components can signal an active output port via `set_output_port(context, port)`
  - Graph edges with `port` field route conditionally based on active port
  - `FlowContext.set_port()`, `get_active_port()`, `clear_port()` methods
  - Unconditional edges (no port) always activate

- **Async Component Support**
  - `BaseComponent.process_async()` with automatic sync fallback
  - `BaseComponent.is_async` property for detection
  - Full async lifecycle support

- **Execution Checkpoints (Suspend/Resume)**
  - `FlowContext.suspend(node_id, reason)` for pausing flows mid-execution
  - `Checkpoint` dataclass with full JSON serialization
  - `CheckpointStore` abstract base class with `InMemoryCheckpointStore` implementation
  - `FlowEngine.resume(checkpoint_id, resume_data)` for continuing suspended flows
  - Suspended nodes re-execute on resume (not skipped)

- **Step Lifecycle Hooks**
  - `ExecutionHook` Protocol with five hook points:
    `on_node_start`, `on_node_complete`, `on_node_error`, `on_node_skipped`, `on_flow_suspended`
  - Hooks are fault-tolerant — broken hooks don't break flow execution
  - Multiple hooks supported simultaneously

- **Enhanced FlowDefinition Validation**
  - `model_validator` ensures correct fields for each flow type
  - Graph-specific validation: unique node IDs, valid edge references
  - Backward-compatible: `steps` remains for sequential/conditional flows

### Changed

- `FlowEngine.__init__` accepts optional `checkpoint_store` and `hooks` parameters
- `FlowEngine.from_config` accepts optional `checkpoint_store` and `hooks` parameters
- `FlowDefinition.type` now accepts `"graph"` in addition to `"sequential"` and `"conditional"`
- `FlowDefinition.steps` is now optional (not required for graph flows)
- `ExecutionMetadata` includes `suspended`, `suspended_at_node`, `suspension_reason`, `completed_nodes` fields
- Context serialization includes suspension and completed_nodes state

## [0.1.0] - 2024-12-28

### Added

- Initial release
- `BaseComponent` abstract base class with lifecycle methods
- `FlowContext` data container with attribute-style access
- `DotDict` helper for nested dictionary access
- `FlowEngine` executor for running component flows
- `ConfigLoader` for loading YAML configurations
- Pydantic schemas for configuration validation
- `ConditionEvaluator` for safe expression evaluation
- `SafeASTValidator` for security validation
- Custom exception hierarchy
- Comprehensive test suite
- Documentation and examples

### Security

- Safe expression evaluation prevents code injection
- Restricted AST nodes prevent malicious code execution
- No access to Python builtins in condition evaluation

## [0.1.1] - 2025-12-28

### Added

- Documentation and examples https://flowengine.readthedocs.io