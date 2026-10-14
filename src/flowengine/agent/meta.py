"""Agent-facing metadata models.

These models let components, and whole flows, declare a machine-readable
contract: what data they consume and produce, which output ports they expose,
and what real-world effects/risk they carry. Agents use this metadata to compose
flows intelligently, validate them semantically, and decide what is safe to run.

`ComponentMeta` is intentionally a superset of NeuroCore's ``SkillMeta`` so the
two map cleanly (see ``design/v0.5.0_agent_native.md``):

    SkillMeta.provides  -> ComponentMeta.outputs (keys)
    SkillMeta.consumes  -> ComponentMeta.inputs  (keys)
    SkillMeta.tags/config_schema/requires_llm -> mirrored 1:1
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

RiskLevel = Literal["low", "medium", "high", "critical"]

# Canonical side-effect vocabulary. Not enforced as an enum (components may
# declare custom effects), but these are the well-known names policies reason about.
KNOWN_EFFECTS = (
    "read_web",
    "read_file",
    "write_file",
    "send_email",
    "execute_code",
    "spend_money",
    "modify_repo",
    "network",
)


class IOFieldSpec(BaseModel):
    """Declaration of a single input or output field in a contract.

    Used both for top-level flow ``inputs``/``outputs`` and, optionally, to give
    rich type info inside ``ComponentMeta`` contracts.
    """

    type: Literal[
        "string", "number", "integer", "boolean", "array", "object", "any"
    ] = Field(default="any", description="JSON-schema-style type of the field")
    required: bool = Field(default=False, description="Whether the field is mandatory")
    description: Optional[str] = Field(
        default=None, description="Human/agent-readable description"
    )
    default: Optional[Any] = Field(
        default=None, description="Default value when not provided"
    )

    def to_json_schema(self) -> dict[str, Any]:
        """Render this field as a JSON Schema fragment."""
        schema: dict[str, Any] = {}
        if self.type != "any":
            schema["type"] = self.type
        if self.description:
            schema["description"] = self.description
        if self.default is not None:
            schema["default"] = self.default
        return schema


class PortSpec(BaseModel):
    """A named output port a component can route execution through."""

    name: str = Field(..., description="Port name, e.g. 'done' or 'revise'")
    description: str = Field(default="", description="When this port is taken")


class ComponentMeta(BaseModel):
    """Machine-readable capability manifest for a component.

    Components opt in by setting a class attribute ``meta`` or overriding
    :meth:`~flowengine.core.component.BaseComponent.get_meta`. Absent metadata is
    valid — agentic checks that need it simply degrade to warnings.
    """

    name: str = Field(..., description="Component type name, e.g. 'web_search'")
    description: str = Field(default="", description="What the component does")
    version: str = Field(default="1.0", description="Component version")

    inputs: dict[str, IOFieldSpec] = Field(
        default_factory=dict, description="Context keys this component reads"
    )
    outputs: dict[str, IOFieldSpec] = Field(
        default_factory=dict, description="Context keys this component writes"
    )
    ports: list[PortSpec] = Field(
        default_factory=list, description="Output ports this component may activate"
    )

    tags: list[str] = Field(default_factory=list, description="Categorization tags")
    config_schema: dict[str, Any] = Field(
        default_factory=dict, description="JSON Schema for the component's config"
    )
    cost: Optional[Literal["low", "medium", "high"]] = Field(
        default=None, description="Rough cost-per-call estimate"
    )

    # Risk / permission model
    risk_level: RiskLevel = Field(default="low", description="Worst-case risk level")
    effects: list[str] = Field(
        default_factory=list, description="Real-world effects (see KNOWN_EFFECTS)"
    )
    requires_approval: bool = Field(
        default=False, description="Whether a human must approve before running"
    )
    requires_llm: bool = Field(
        default=False, description="Whether the component needs an LLM provider"
    )
    data_classes: list[str] = Field(
        default_factory=list,
        description="Data classes this component may emit or handle (policy vocabulary)",
    )

    @property
    def port_names(self) -> list[str]:
        """Names of all declared ports."""
        return [p.name for p in self.ports]

    @property
    def input_keys(self) -> list[str]:
        """Context keys this component consumes."""
        return list(self.inputs.keys())

    @property
    def output_keys(self) -> list[str]:
        """Context keys this component produces."""
        return list(self.outputs.keys())

    @property
    def is_safe_for_agents(self) -> bool:
        """True when the component is low-risk and needs no approval."""
        return self.risk_level == "low" and not self.requires_approval

    def to_catalog_entry(self) -> dict[str, Any]:
        """Render an agent-facing catalog entry (see ``flowengine components``)."""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "inputs": {k: v.to_json_schema() for k, v in self.inputs.items()},
            "outputs": {k: v.to_json_schema() for k, v in self.outputs.items()},
            "ports": self.port_names,
            "tags": self.tags,
            "cost": self.cost,
            "risk_level": self.risk_level,
            "effects": self.effects,
            "requires_approval": self.requires_approval,
            "requires_llm": self.requires_llm,
            "data_classes": self.data_classes,
            "safe_for_agents": self.is_safe_for_agents,
        }
