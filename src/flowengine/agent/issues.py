"""Machine-readable validation issues and repair suggestions.

The whole point of this module is to make FlowEngine feedback *self-correctable*
by an LLM. Instead of a free-text ``ValidationError``, validation produces a list
of :class:`FlowIssue` objects, each with a stable ``code``, a JSON ``path`` into
the document, a human/agent message, and — where possible — a structured
:class:`RepairSuggestion` carrying RFC-6902 JSON Patch operations the agent can
apply verbatim before retrying.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

Severity = Literal["error", "warning"]


class IssueCode(str, Enum):
    """Stable, machine-matchable issue codes.

    Agents can branch on these without parsing English. New codes may be added
    in minor releases; existing codes are never repurposed.
    """

    # Schema / structural
    SCHEMA_INVALID = "SCHEMA_INVALID"
    MISSING_FIELD = "MISSING_FIELD"
    INVALID_VALUE = "INVALID_VALUE"
    YAML_PARSE_ERROR = "YAML_PARSE_ERROR"

    # References
    UNKNOWN_COMPONENT = "UNKNOWN_COMPONENT"
    UNDEFINED_COMPONENT_REF = "UNDEFINED_COMPONENT_REF"
    DUPLICATE_NAME = "DUPLICATE_NAME"
    UNKNOWN_EDGE_NODE = "UNKNOWN_EDGE_NODE"

    # Semantic
    UNDECLARED_PORT = "UNDECLARED_PORT"
    MISSING_INPUT_PRODUCER = "MISSING_INPUT_PRODUCER"
    OUTPUT_NOT_PRODUCED = "OUTPUT_NOT_PRODUCED"
    UNREACHABLE_NODE = "UNREACHABLE_NODE"
    CYCLE_WITHOUT_LIMIT = "CYCLE_WITHOUT_LIMIT"
    CYCLE_WITHOUT_EXIT = "CYCLE_WITHOUT_EXIT"
    NO_TERMINAL_OUTPUT = "NO_TERMINAL_OUTPUT"

    # Data bindings (0.7.0)
    UNKNOWN_BINDING_NODE = "UNKNOWN_BINDING_NODE"
    UNDECLARED_BINDING_KEY = "UNDECLARED_BINDING_KEY"
    BINDING_NOT_UPSTREAM = "BINDING_NOT_UPSTREAM"
    BINDING_FOREIGN_TARGET = "BINDING_FOREIGN_TARGET"
    UNBOUND_INPUT = "UNBOUND_INPUT"
    BINDING_TYPE_MISMATCH = "BINDING_TYPE_MISMATCH"
    BINDING_EXPR_INVALID = "BINDING_EXPR_INVALID"

    # Safety / policy
    APPROVAL_REQUIRED = "APPROVAL_REQUIRED"
    DENIED_COMPONENT = "DENIED_COMPONENT"
    NOT_ALLOWLISTED = "NOT_ALLOWLISTED"
    RISK_EXCEEDS_POLICY = "RISK_EXCEEDS_POLICY"


class JsonPatchOp(BaseModel):
    """A single RFC-6902 JSON Patch operation."""

    op: Literal["add", "replace", "remove"] = Field(..., description="Patch operation")
    path: str = Field(..., description="JSON Pointer, e.g. '/flow/settings/max_iterations'")
    value: Optional[Any] = Field(default=None, description="Value for add/replace")

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {"op": self.op, "path": self.path}
        if self.op in ("add", "replace"):
            d["value"] = self.value
        return d


class RepairSuggestion(BaseModel):
    """A structured, applyable fix for a :class:`FlowIssue`."""

    explanation: str = Field(..., description="Why this repair resolves the issue")
    yaml_patch: list[JsonPatchOp] = Field(
        default_factory=list, description="JSON Patch ops to apply to the document"
    )
    confidence: float = Field(
        default=0.5, ge=0.0, le=1.0, description="0..1 confidence the patch is correct"
    )


class FlowIssue(BaseModel):
    """One validation finding, optimized for LLM repair."""

    code: IssueCode = Field(..., description="Stable machine-matchable code")
    severity: Severity = Field(default="error", description="error | warning")
    path: str = Field(
        default="",
        description="Dotted document path, e.g. 'components[2].type'",
    )
    message: str = Field(..., description="What is wrong")
    why: Optional[str] = Field(
        default=None, description="Why it matters / what it would break"
    )
    suggestion: Optional[str] = Field(
        default=None, description="Short natural-language hint, e.g. 'Did you mean web_search?'"
    )
    repair: Optional[RepairSuggestion] = Field(
        default=None, description="Structured, applyable repair"
    )

    @property
    def is_error(self) -> bool:
        return self.severity == "error"

    def to_dict(self) -> dict[str, Any]:
        """Render as a plain dict (e.g. for ``--json`` CLI output)."""
        return self.model_dump(mode="json", exclude_none=True)


def dotted_to_pointer(path: str) -> str:
    """Convert a dotted/indexed path (``a.b[2].c``) to a JSON Pointer (``/a/b/2/c``).

    Best-effort: used to derive patch paths from validation error locations.
    """
    if not path:
        return ""
    tokens: list[str] = []
    for part in path.split("."):
        while "[" in part:
            head, _, rest = part.partition("[")
            if head:
                tokens.append(head)
            idx, _, part = rest.partition("]")
            tokens.append(idx)
        if part:
            tokens.append(part)
    return "/" + "/".join(tokens) if tokens else ""
