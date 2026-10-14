"""Canonical YAML normalization.

Agents produce semantically valid but stylistically inconsistent YAML. Running
it through :func:`normalize_yaml` yields a canonical form — defaults filled,
unknown fields dropped, ``flow.type`` and enums normalized, keys ordered, and
ambiguous scalars quoted — so generated workflows are easy to diff, review, and
store.
"""

from __future__ import annotations

from typing import Any

import yaml

from flowengine.config.schema import FlowConfig

# Preferred top-level key order; anything else is appended alphabetically.
_TOP_LEVEL_ORDER = [
    "name",
    "version",
    "description",
    "inputs",
    "outputs",
    "components",
    "flow",
    "service",
    "triggers",
    "studio",
]


def normalize_config(config: FlowConfig) -> str:
    """Render a validated :class:`FlowConfig` as canonical YAML."""
    dumped = config.model_dump(mode="json", exclude_none=True)
    ordered = _order_top_level(dumped)
    return yaml.safe_dump(
        ordered,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    )


def normalize_yaml(yaml_text: str) -> str:
    """Validate and canonicalize a YAML flow document.

    Raises:
        flowengine.errors.ConfigurationError: If the document is not a valid flow.
    """
    from flowengine.config.loader import ConfigLoader

    config = ConfigLoader.loads(yaml_text)
    return normalize_config(config)


def _order_top_level(data: dict[str, Any]) -> dict[str, Any]:
    """Reorder top-level keys into canonical order, defaults-filled dict intact."""
    ordered: dict[str, Any] = {}
    for key in _TOP_LEVEL_ORDER:
        if key in data:
            ordered[key] = data[key]
    for key in sorted(data):
        if key not in ordered:
            ordered[key] = data[key]
    return ordered
