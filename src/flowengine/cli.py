"""``flowengine`` command-line interface.

A thin, agent-oriented CLI over the v0.5.0 agent API. Every command that emits
structured data supports ``--json`` so a supervisor agent can drive the
generate → validate → plan → run → repair loop from a shell.

Commands::

    flowengine validate FLOW.yaml [--json]
    flowengine plan FLOW.yaml [--json]
    flowengine schema [--kind flow|component|graph|component-meta] [--all]
    flowengine normalize FLOW.yaml [-o OUT]
    flowengine apply-patch FLOW.yaml PATCH.json [-o OUT]
    flowengine components [--module MOD ...] [--json]
    flowengine template list | show NAME
    flowengine run FLOW.yaml [--input-json '{...}']
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

import yaml


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _load_policy(path: str | None):
    """Load an ExecutionPolicy from a YAML/JSON file, or None."""
    if not path:
        return None
    from flowengine.agent.policy import ExecutionPolicy

    data = yaml.safe_load(_read(path)) or {}
    # Accept either a bare policy mapping or one nested under 'policy'.
    if isinstance(data, dict) and "policy" in data:
        data = data["policy"]
    return ExecutionPolicy.model_validate(data)


def _build_registry(modules: list[str] | None):
    """Registry seeded with builtin contrib components plus any --module imports."""
    from flowengine.config.registry import ComponentRegistry
    from flowengine.core.component import BaseComponent

    registry = ComponentRegistry()

    def _register_module(mod_name: str) -> None:
        module = importlib.import_module(mod_name)
        for attr in vars(module).values():
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseComponent)
                and attr is not BaseComponent
                and attr.__module__ == module.__name__
            ):
                meta = getattr(attr, "meta", None)
                name = getattr(meta, "name", None) or attr.__name__
                if registry.get_class(name) is None:
                    registry.register_class(name, attr)

    _register_module("flowengine.contrib.logging")
    try:
        _register_module("flowengine.contrib.subflow")
    except Exception:
        pass
    for mod in modules or []:
        _register_module(mod)
    return registry


# ── commands ────────────────────────────────────────────────────────────


def cmd_validate(args: argparse.Namespace) -> int:
    from flowengine.agent.compiler import FlowCompiler

    registry = _build_registry(args.module)
    policy = _load_policy(getattr(args, "policy", None))
    result = FlowCompiler.compile_yaml(
        _read(args.file), registry=registry, policy=policy
    )
    if args.json:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        status = "VALID" if result.valid else "INVALID"
        print(f"{status}: {args.file}")
        for issue in result.errors:
            print(f"  [error] {issue.code.value} @ {issue.path}: {issue.message}")
            if issue.suggestion:
                print(f"          → {issue.suggestion}")
        for issue in result.warnings:
            print(f"  [warn]  {issue.code.value} @ {issue.path}: {issue.message}")
    return 0 if result.valid else 1


def cmd_plan(args: argparse.Namespace) -> int:
    from flowengine.agent.plan import explain
    from flowengine.config.loader import ConfigLoader

    config = ConfigLoader.load(args.file)
    registry = _build_registry(args.module)
    plan = explain(config, registry=registry)
    if args.json:
        print(json.dumps(plan.to_dict(), indent=2))
    else:
        print(f"flow_type: {plan.flow_type}")
        print(f"execution_order: {' → '.join(plan.execution_order)}")
        if plan.branches:
            print("branches:")
            for b in plan.branches:
                print(f"  {b.source} --[{b.port}]--> {b.target}")
        if plan.data_edges:
            print("data_edges:")
            for d in plan.data_edges:
                print(f"  {d.source} ==> {d.target}")
        print(f"possible_cycles: {plan.possible_cycles}")
        if plan.possible_cycles:
            print(f"max_iterations: {plan.max_iterations}")
        print(f"required_components: {plan.required_components}")
        print(f"context_inputs: {plan.context_inputs}")
        print(f"context_outputs: {plan.context_outputs}")
    return 0


def cmd_schema(args: argparse.Namespace) -> int:
    from flowengine.agent.schema_export import export_all_schemas, export_json_schema

    if args.all:
        print(json.dumps(export_all_schemas(), indent=2))
    else:
        print(json.dumps(export_json_schema(args.kind), indent=2))
    return 0


def cmd_normalize(args: argparse.Namespace) -> int:
    from flowengine.agent.normalize import normalize_yaml

    out = normalize_yaml(_read(args.file))
    if args.output:
        Path(args.output).write_text(out, encoding="utf-8")
    else:
        sys.stdout.write(out)
    return 0


def cmd_apply_patch(args: argparse.Namespace) -> int:
    from flowengine.agent.patch import apply_patch

    document = yaml.safe_load(_read(args.file))
    patch = json.loads(_read(args.patch))
    if isinstance(patch, dict) and "patches" in patch:
        patch = patch["patches"]
    result = apply_patch(document, patch)
    out = yaml.safe_dump(result, sort_keys=False, default_flow_style=False)
    if args.output:
        Path(args.output).write_text(out, encoding="utf-8")
    else:
        sys.stdout.write(out)
    return 0


def cmd_components(args: argparse.Namespace) -> int:
    from flowengine.agent.catalog import build_catalog

    registry = _build_registry(args.module)
    catalog = build_catalog(registry)
    if args.json:
        print(json.dumps(catalog, indent=2))
    else:
        for entry in catalog:
            risk = entry["risk_level"]
            print(f"{entry['type']:<24} risk={risk:<8} {entry['description']}")
    return 0


def cmd_template(args: argparse.Namespace) -> int:
    from flowengine.agent import templates

    if args.action == "list":
        for name in templates.list_templates():
            print(name)
    elif args.action == "show":
        if not args.name:
            print("template show requires a NAME", file=sys.stderr)
            return 2
        sys.stdout.write(templates.get_template(args.name))
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    from flowengine.agent.replay import InMemoryRunStore, RunRecord, replay
    from flowengine.agent.trace import AgentTrace

    record = RunRecord.model_validate(json.loads(_read(args.record)))
    store = InMemoryRunStore()
    store.save(record)
    result = replay(record.run_id, store, from_node=args.from_node)
    trace = AgentTrace.from_context(result, record.to_config())
    print(json.dumps(trace.to_dict(), indent=2))
    return 0 if trace.status != "error" else 1


def cmd_run(args: argparse.Namespace) -> int:
    from flowengine.agent.trace import AgentTrace
    from flowengine.config.loader import ConfigLoader
    from flowengine.core.context import FlowContext
    from flowengine.core.engine import FlowEngine

    config = ConfigLoader.load(args.file)
    policy = _load_policy(getattr(args, "policy", None))
    if policy is not None:
        config = policy.apply_to_config(config)
    engine = FlowEngine.from_config(config)
    context = FlowContext()
    if args.input_json:
        for key, value in json.loads(args.input_json).items():
            context.set(key, value)
    result = engine.execute(context)
    trace = AgentTrace.from_context(result, config)
    print(json.dumps(trace.to_dict(), indent=2))
    return 0 if trace.status != "error" else 1


# ── parser ──────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="flowengine",
        description="Agent-native workflow IR: validate, plan, run, and repair flows.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_module_arg(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--module",
            action="append",
            help="Import a module so its components are known (repeatable).",
        )

    p_validate = sub.add_parser("validate", help="Validate a flow YAML file.")
    p_validate.add_argument("file")
    p_validate.add_argument("--json", action="store_true")
    p_validate.add_argument("--policy", help="Path to an execution-policy YAML/JSON file.")
    add_module_arg(p_validate)
    p_validate.set_defaults(func=cmd_validate)

    p_plan = sub.add_parser("plan", help="Explain how a flow would execute.")
    p_plan.add_argument("file")
    p_plan.add_argument("--json", action="store_true")
    add_module_arg(p_plan)
    p_plan.set_defaults(func=cmd_plan)

    p_schema = sub.add_parser("schema", help="Export JSON Schema for the YAML format.")
    p_schema.add_argument(
        "--kind",
        default="flow",
        choices=["flow", "component", "graph", "component-meta"],
    )
    p_schema.add_argument("--all", action="store_true", help="Export every schema.")
    p_schema.set_defaults(func=cmd_schema)

    p_norm = sub.add_parser("normalize", help="Emit canonical YAML for a flow.")
    p_norm.add_argument("file")
    p_norm.add_argument("-o", "--output")
    p_norm.set_defaults(func=cmd_normalize)

    p_patch = sub.add_parser("apply-patch", help="Apply a JSON Patch to a flow.")
    p_patch.add_argument("file")
    p_patch.add_argument("patch")
    p_patch.add_argument("-o", "--output")
    p_patch.set_defaults(func=cmd_apply_patch)

    p_comp = sub.add_parser("components", help="List the component catalog.")
    p_comp.add_argument("--json", action="store_true")
    add_module_arg(p_comp)
    p_comp.set_defaults(func=cmd_components)

    p_tmpl = sub.add_parser("template", help="List or show flow templates.")
    p_tmpl.add_argument("action", choices=["list", "show"])
    p_tmpl.add_argument("name", nargs="?")
    p_tmpl.set_defaults(func=cmd_template)

    p_run = sub.add_parser("run", help="Execute a flow and print its agent trace.")
    p_run.add_argument("file")
    p_run.add_argument("--input-json", help="JSON object of initial context data.")
    p_run.add_argument("--policy", help="Path to an execution-policy YAML/JSON file.")
    p_run.set_defaults(func=cmd_run)

    p_replay = sub.add_parser("replay", help="Replay a saved run-record JSON file.")
    p_replay.add_argument("record", help="Path to a RunRecord JSON file.")
    p_replay.add_argument("--from-node", help="Resume a graph flow from this node.")
    p_replay.set_defaults(func=cmd_replay)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except FileNotFoundError as e:
        print(f"error: file not found: {e.filename}", file=sys.stderr)
        return 2
    except Exception as e:  # surface clean errors to the shell
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
