"""Compiler issue codes for data bindings (design/07 §4) with their repair patches."""

import pytest

from flowengine.agent.issues import IssueCode
from flowengine.agent.semantic import validate_semantics
from flowengine.config.registry import ComponentRegistry
from flowengine.config.schema import FlowConfig

from .conftest import SearchComponent, SummarizeComponent


@pytest.fixture
def registry():
    reg = ComponentRegistry()
    reg.register_class("web_search", SearchComponent)
    reg.register_class("llm_summarizer", SummarizeComponent)
    return reg


def _graph(nodes, edges, inputs=None):
    return FlowConfig.model_validate(
        {
            "name": "t",
            "inputs": inputs or {},
            "components": [
                {"name": "search", "type": "web_search"},
                {"name": "summarize", "type": "llm_summarizer"},
            ],
            "flow": {"type": "graph", "nodes": nodes, "edges": edges},
        }
    )


def _codes(issues):
    return [i.code for i in issues]


def _only(issues, code):
    found = [i for i in issues if i.code == code]
    assert len(found) == 1, _codes(issues)
    return found[0]


GOOD = [
    {"id": "n_search", "component": "search", "bind": {"inputs": {"query": "flow.query"}}},
    {
        "id": "n_sum",
        "component": "summarize",
        "bind": {"inputs": {"search_results": "n_search.search_results"}},
    },
]
EDGE = [{"source": "n_search", "target": "n_sum", "port": "success"}]


def test_well_bound_flow_is_clean(registry):
    issues = validate_semantics(_graph(GOOD, EDGE, {"query": {"type": "string"}}), registry=registry)
    assert not [i for i in issues if i.code.value.startswith(("UNKNOWN_BINDING", "BINDING", "UNBOUND"))], _codes(issues)


def test_unknown_binding_node_with_repair(registry):
    nodes = [GOOD[0], {**GOOD[1], "bind": {"inputs": {"search_results": "n_serch.search_results"}}}]
    issue = _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.UNKNOWN_BINDING_NODE)
    assert issue.severity == "error"
    assert issue.path == "flow.nodes[1].bind.inputs.search_results"
    patch = issue.repair.yaml_patch[0]
    assert patch.path == "/flow/nodes/1/bind/inputs/search_results"
    assert patch.value == "n_search.search_results"


def test_undeclared_binding_key_warns_with_repair(registry):
    nodes = [GOOD[0], {**GOOD[1], "bind": {"inputs": {"search_results": "n_search.search_result"}}}]
    issue = _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.UNDECLARED_BINDING_KEY)
    assert issue.severity == "warning"
    assert issue.repair.yaml_patch[0].value == "n_search.search_results"


def test_binding_not_upstream_is_an_error(registry):
    nodes = [
        {"id": "n_search", "component": "search", "bind": {"inputs": {"query": "n_sum.answer"}}},
        GOOD[1],
    ]
    issue = _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.BINDING_NOT_UPSTREAM)
    assert issue.severity == "error"


def test_binding_not_upstream_is_a_warning_inside_a_cycle(registry):
    nodes = [
        {"id": "n_search", "component": "search", "bind": {"inputs": {"query": "n_sum.answer"}}},
        GOOD[1],
    ]
    edges = EDGE + [{"source": "n_sum", "target": "n_search", "port": "revise"}]
    issue = _only(validate_semantics(_graph(nodes, edges), registry=registry), IssueCode.BINDING_NOT_UPSTREAM)
    assert issue.severity == "warning"


def test_self_reference_is_allowed(registry):
    nodes = [{**GOOD[0], "bind": {"inputs": {"query": "n_search.search_results"}}}, GOOD[1]]
    issues = validate_semantics(_graph(nodes, EDGE), registry=registry)
    assert IssueCode.BINDING_NOT_UPSTREAM not in _codes(issues)


def test_foreign_output_target_with_repair(registry):
    nodes = [{**GOOD[0], "bind": {"outputs": {"search_results": "n_sum.search_results"}}}, GOOD[1]]
    issue = _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.BINDING_FOREIGN_TARGET)
    assert issue.severity == "error"
    assert issue.repair.yaml_patch[0].value == "n_search.search_results"


def test_unbound_input_warns_with_repair(registry):
    nodes = [GOOD[0], {"id": "n_sum", "component": "summarize", "bind": {}}]
    issue = _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.UNBOUND_INPUT)
    assert issue.severity == "warning"
    patch = issue.repair.yaml_patch[0]
    assert patch.op == "add" and patch.value == "n_search.search_results"


def test_unbound_input_served_by_unbound_upstream_raw_key(registry):
    nodes = [
        {"id": "n_search", "component": "search"},  # unbound: mirrors raw keys
        {"id": "n_sum", "component": "summarize", "bind": {}},
    ]
    issues = validate_semantics(_graph(nodes, EDGE), registry=registry)
    assert IssueCode.UNBOUND_INPUT not in _codes(issues)


def test_type_mismatch_warns(registry):
    nodes = [
        {"id": "n_search", "component": "search"},
        {"id": "n_sum", "component": "summarize", "bind": {"inputs": {"search_results": "n_search.search_results"}}},
        {"id": "n_again", "component": "search", "bind": {"inputs": {"query": "n_search.search_results"}}},
    ]
    edges = EDGE + [{"source": "n_search", "target": "n_again", "port": "no_results"}]
    issue = _only(validate_semantics(_graph(nodes, edges), registry=registry), IssueCode.BINDING_TYPE_MISMATCH)
    assert "'array'" in issue.message and "'string'" in issue.message


def test_expression_invalid(registry):
    nodes = [{**GOOD[0], "bind": {"inputs": {"query": {"expr": "__import__('os').system('x')"}}}}, GOOD[1]]
    issue = _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.BINDING_EXPR_INVALID)
    assert issue.severity == "error"
    nodes = [{**GOOD[0], "bind": {"inputs": {"query": {"expr": "flow.q or"}}}}, GOOD[1]]
    assert _only(validate_semantics(_graph(nodes, EDGE), registry=registry), IssueCode.BINDING_EXPR_INVALID)


def test_legacy_flow_has_no_binding_issues(registry):
    nodes = [{"id": "n_search", "component": "search"}, {"id": "n_sum", "component": "summarize"}]
    issues = validate_semantics(_graph(nodes, EDGE), registry=registry)
    assert not [c for c in _codes(issues) if "BINDING" in c.value or c == IssueCode.UNBOUND_INPUT]
