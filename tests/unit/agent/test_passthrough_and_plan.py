"""0.7.0: authoring blocks (service, triggers, studio) pass through the engine untouched,
the schema export carries the binding fields, and `plan` shows data edges."""

import yaml

from flowengine.agent.normalize import normalize_config
from flowengine.agent.plan import explain
from flowengine.agent.schema_export import export_json_schema
from flowengine.config.loader import ConfigLoader

DOC = """
name: parking-fines
version: "1.0"
service: { service: Parking fines, department: Traffic, channel: portal }
components:
  - { name: identity, type: identity.verify }
  - { name: fines, type: fines.outstanding, config: { preset: adp } }
  - { name: choose, type: citizen.choose }
flow:
  type: graph
  settings: { executor: graph, max_iterations: 10 }
  nodes:
    - { id: n_identity, component: identity }
    - id: n_fines
      component: fines
      bind:
        inputs: { subject_id: n_identity.subject_id, channel: { value: portal } }
      provenance: { subject_id: identity.verified }
    - id: n_choose
      component: choose
      bind:
        inputs: { options: n_fines.fines, limit: { expr: "flow.limit or 10" } }
  edges:
    - { source: n_identity, target: n_fines, port: verified }
    - { source: n_fines, target: n_choose, port: found }
triggers:
  - { type: citizen, profile: moi-portal-fines }
studio:
  layout: { n_identity: { x: 100, y: 80 }, n_fines: { x: 420, y: 180 } }
  notes: [{ text: "pilot scope", at: n_fines }]
"""


def test_authoring_blocks_survive_load_normalise_dump():
    config = ConfigLoader.loads(DOC)
    assert config.service == {"service": "Parking fines", "department": "Traffic", "channel": "portal"}
    assert config.triggers == [{"type": "citizen", "profile": "moi-portal-fines"}]
    assert config.studio is not None and config.studio["layout"]["n_fines"] == {"x": 420, "y": 180}

    normalised = yaml.safe_load(normalize_config(config))
    original = yaml.safe_load(DOC)
    for block in ("service", "triggers", "studio"):
        assert normalised[block] == original[block], block
    # canonical order keeps them after the flow, in a fixed order
    keys = list(normalised)
    assert keys.index("flow") < keys.index("service") < keys.index("triggers") < keys.index("studio")
    # and a second normalisation is a fixed point
    assert normalize_config(ConfigLoader.loads(normalize_config(config))) == normalize_config(config)


def test_blocks_absent_stay_absent():
    doc = yaml.safe_load(DOC)
    for block in ("service", "triggers", "studio"):
        doc.pop(block)
    normalised = yaml.safe_load(normalize_config(ConfigLoader.loads(yaml.safe_dump(doc))))
    assert not ({"service", "triggers", "studio"} & set(normalised))


def test_schema_export_carries_bindings_and_blocks():
    schema = export_json_schema("flow")
    node = schema["$defs"]["GraphNodeConfig"]["properties"]
    assert "bind" in node and "provenance" in node
    assert {"NodeBinding", "BindingLiteral", "BindingExpr"} <= set(schema["$defs"])
    assert "executor" in schema["$defs"]["FlowSettings"]["properties"]
    for block in ("service", "triggers", "studio"):
        assert block in schema["properties"]


def test_plan_lists_data_edges_next_to_control_edges():
    plan = explain(ConfigLoader.loads(DOC))
    assert [(b.source, b.port, b.target) for b in plan.branches] == [
        ("n_identity", "verified", "n_fines"),
        ("n_fines", "found", "n_choose"),
    ]
    assert [(d.source, d.target, d.kind) for d in plan.data_edges] == [
        ("n_identity.subject_id", "n_fines.subject_id", "reference"),
        ("literal", "n_fines.channel", "literal"),
        ("n_fines.fines", "n_choose.options", "reference"),
        ("expr: flow.limit or 10", "n_choose.limit", "expr"),
    ]
    assert plan.execution_order == ["n_identity", "n_fines", "n_choose"]
