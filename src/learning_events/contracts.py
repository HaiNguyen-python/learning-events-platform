"""Event contracts: load versioned JSON Schemas, validate events, check compatibility.

Each file in `contracts/` is one version of one event type, named `<event_type>.v<N>.json`.
This mirrors a schema registry (Confluent, Apicurio, AWS Glue): a *subject* (event type) has an
ordered list of immutable *versions*, every event says which version it was written with, and a
new version is only registered if it passes a compatibility check against the previous one.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
_FILENAME = re.compile(r"^(?P<event_type>[a-z_]+)\.v(?P<version>\d+)\.json$")


@dataclass(frozen=True)
class Verdict:
    """Outcome of validating one event. `reason` is a stable code used for dead-letter stats."""

    ok: bool
    reason: str | None = None
    errors: tuple[str, ...] = ()


ACCEPTED = Verdict(ok=True)


@dataclass(frozen=True)
class FieldSpec:
    """What a consumer can rely on for one field path: its JSON type, enum and if it is required."""

    types: frozenset[str]
    required: bool
    enum: frozenset[str] | None = None


class ContractRegistry:
    """All contract versions, with a compiled validator per (event_type, version)."""

    def __init__(self, schemas: dict[tuple[str, int], dict[str, Any]]) -> None:
        self.schemas = schemas
        self._validators = {key: Draft202012Validator(s) for key, s in schemas.items()}

    @classmethod
    def load(cls, directory: Path = CONTRACTS_DIR) -> ContractRegistry:
        schemas: dict[tuple[str, int], dict[str, Any]] = {}
        for path in sorted(directory.glob("*.json")):
            match = _FILENAME.match(path.name)
            if match is None:
                raise ValueError(f"contract file name must be <event_type>.v<N>.json: {path.name}")
            schema = json.loads(path.read_text())
            Draft202012Validator.check_schema(schema)
            key = (match["event_type"], int(match["version"]))
            _check_header(schema, *key)
            schemas[key] = schema
        return cls(schemas)

    @property
    def event_types(self) -> list[str]:
        return sorted({event_type for event_type, _ in self.schemas})

    def versions(self, event_type: str) -> list[int]:
        return sorted(v for t, v in self.schemas if t == event_type)

    def validate(self, event: Any) -> Verdict:
        """Check the envelope first, so the dead-letter reason says *why* in one word."""
        if not isinstance(event, dict):
            return Verdict(False, "malformed_envelope", ("event is not a JSON object",))
        event_type, version = event.get("event_type"), event.get("schema_version")
        if not isinstance(event_type, str) or type(version) is not int:
            return Verdict(
                False,
                "malformed_envelope",
                ("event_type must be a string and schema_version an integer",),
            )
        if event_type not in self.event_types:
            return Verdict(False, "unknown_event_type", (f"no contract for '{event_type}'",))
        validator = self._validators.get((event_type, version))
        if validator is None:
            known = ", ".join(f"v{v}" for v in self.versions(event_type))
            return Verdict(
                False,
                "unsupported_schema_version",
                (f"{event_type} v{version} is not registered (known: {known})",),
            )
        errors = sorted(validator.iter_errors(event), key=lambda e: list(e.absolute_path))
        if errors:
            return Verdict(False, "schema_violation", tuple(_describe(e) for e in errors))
        return ACCEPTED


def _check_header(schema: dict[str, Any], event_type: str, version: int) -> None:
    """The file name, `event_type` const and `schema_version` const must agree."""
    props = schema.get("properties", {})
    if props.get("event_type", {}).get("const") != event_type:
        raise ValueError(f"{event_type}.v{version}: event_type const does not match file name")
    if props.get("schema_version", {}).get("const") != version:
        raise ValueError(f"{event_type}.v{version}: schema_version const does not match file name")


def _describe(error: Any) -> str:
    path = ".".join(str(p) for p in error.absolute_path) or "(root)"
    return f"{path}: {error.message}"


def flatten(
    schema: dict[str, Any], prefix: str = "", parent_required: bool = True
) -> dict[str, FieldSpec]:
    """Map every field path to a FieldSpec.

    A nested field only counts as required if its parent object is required too, because a
    consumer can only rely on `result.score` if `result` itself is always present.
    """
    fields: dict[str, FieldSpec] = {}
    required = set(schema.get("required", []))
    for name, sub in schema.get("properties", {}).items():
        path = f"{prefix}{name}"
        is_required = parent_required and name in required
        fields[path] = FieldSpec(_types(sub), is_required, _enum(sub))
        if "properties" in sub:
            fields.update(flatten(sub, f"{path}.", is_required))
    return fields


def _types(sub: dict[str, Any]) -> frozenset[str]:
    if "const" in sub:
        return frozenset({type(sub["const"]).__name__})
    declared = sub.get("type", "any")
    return frozenset([declared] if isinstance(declared, str) else declared)


def _enum(sub: dict[str, Any]) -> frozenset[str] | None:
    return frozenset(map(str, sub["enum"])) if "enum" in sub else None


def breaking_changes(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """List the changes that would break a consumer moving from `old` to `new`.

    Rule (stricter than Avro BACKWARD, close to FULL for required fields):
    - adding an optional field is fine;
    - removing or renaming a required field, making a required field optional, adding a new
      required field, changing a type, or removing an enum value is breaking.
    Removing an optional field is allowed: consumers must already handle it being absent.
    """
    before, after = flatten(old), flatten(new)
    problems: list[str] = []
    for path, spec in before.items():
        if path in ("event_type", "schema_version"):
            continue  # these consts change by design with every version
        new_spec = after.get(path)
        if new_spec is None:
            if spec.required:
                problems.append(f"required field removed or renamed: {path}")
            continue
        if spec.required and not new_spec.required:
            problems.append(f"required field became optional: {path}")
        if spec.types != new_spec.types:
            problems.append(
                f"type changed: {path} {sorted(spec.types)} -> {sorted(new_spec.types)}"
            )
        if spec.enum is not None and new_spec.enum is not None and spec.enum - new_spec.enum:
            problems.append(f"enum values removed: {path} {sorted(spec.enum - new_spec.enum)}")
    for path, spec in after.items():
        if path not in before and spec.required:
            problems.append(f"new required field: {path}")
    return problems


def check_registry(registry: ContractRegistry) -> dict[str, list[str]]:
    """Compare every version with the one before it; returns problems per `type vN->vM`."""
    report: dict[str, list[str]] = {}
    for event_type in registry.event_types:
        versions = registry.versions(event_type)
        for prev, cur in zip(versions, versions[1:], strict=False):
            key = f"{event_type} v{prev}->v{cur}"
            report[key] = breaking_changes(
                registry.schemas[(event_type, prev)], registry.schemas[(event_type, cur)]
            )
    return report
