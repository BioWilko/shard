"""Validate a shard.yml spec file."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_WORKFLOW_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)?$")
_NAME_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
_PLATFORM_RE = re.compile(r"^linux/(amd64|arm64)$")
_DOCKER_IMAGE_RE = re.compile(r"^[a-zA-Z0-9._\-/:@]+$")
_BAD_SCHEMES = ("https://", "oras://", "shub://")


@dataclass
class SpecValidationResult:
    ok: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.ok = False
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def validate_spec(spec_path: Path, check_paths: bool = True) -> SpecValidationResult:
    result = SpecValidationResult()

    try:
        text = spec_path.read_text()
    except OSError as exc:
        result.fail(f"Cannot read {spec_path}: {exc}")
        return result

    try:
        spec = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        result.fail(f"Invalid YAML: {exc}")
        return result

    if not isinstance(spec, dict):
        result.fail("shard.yml must be a YAML mapping")
        return result

    _check_name(spec, result)
    _check_version(spec, result)
    _check_description(spec, result)
    _check_workflow(spec, result, check_paths)
    _check_containers(spec, result)
    _check_data(spec, result, check_paths)
    _check_platforms(spec, result)
    _check_unresolved(spec, result)

    return result


def _check_name(spec: dict[str, Any], result: SpecValidationResult) -> None:
    if "name" not in spec:
        result.fail("Missing required field: 'name'")
        return
    name = spec["name"]
    if not isinstance(name, str):
        result.fail(f"'name' must be a string, got {type(name).__name__}")
        return
    if not _WORKFLOW_NAME_RE.match(name):
        result.fail(f"'name' must match [a-zA-Z0-9_-] or [org/repo] format, got: {name!r}")


def _check_version(spec: dict[str, Any], result: SpecValidationResult) -> None:
    if "version" not in spec:
        result.fail("Missing required field: 'version'")
        return
    v = spec["version"]
    if isinstance(v, (int, float)):
        result.warn(f"'version' is numeric ({v!r}); it will be coerced to a string")
    elif not isinstance(v, str):
        result.fail(f"'version' must be a string, got {type(v).__name__}")


def _check_description(spec: dict[str, Any], result: SpecValidationResult) -> None:
    desc = spec.get("description")
    if desc is not None and not isinstance(desc, str):
        result.fail(f"'description' must be a string, got {type(desc).__name__}")


def _check_workflow(spec: dict[str, Any], result: SpecValidationResult, check_paths: bool) -> None:
    if "workflow" not in spec:
        result.fail("Missing required field: 'workflow'")
        return
    wf = spec["workflow"]
    if not isinstance(wf, dict):
        result.fail("'workflow' must be a mapping")
        return

    path_val = wf.get("path")
    if path_val is not None:
        if not isinstance(path_val, str):
            result.fail("'workflow.path' must be a string")
        elif check_paths and path_val != ".":
            p = Path(path_val)
            if not p.exists():
                result.fail(f"'workflow.path' does not exist: {path_val!r}")

    if "ref" not in wf:
        result.warn("'workflow.ref' is absent — will default to HEAD when packing")


def _check_containers(spec: dict[str, Any], result: SpecValidationResult) -> None:
    containers = spec.get("containers")
    if containers is None:
        return
    if not isinstance(containers, list):
        result.fail("'containers' must be a list")
        return

    seen: set[str] = set()
    for i, entry in enumerate(containers):
        if not isinstance(entry, str):
            result.fail(f"containers[{i}] must be a string, got {type(entry).__name__}")
            continue
        if any(entry.startswith(s) for s in _BAD_SCHEMES):
            result.fail(f"containers[{i}]: unsupported scheme in {entry!r}")
            continue
        if not _DOCKER_IMAGE_RE.match(entry):
            result.fail(f"containers[{i}]: invalid image reference {entry!r}")
            continue
        if entry in seen:
            result.warn(f"containers: duplicate entry {entry!r}")
        seen.add(entry)


def _check_data(spec: dict[str, Any], result: SpecValidationResult, check_paths: bool) -> None:
    data = spec.get("data")
    if data is None:
        return
    if not isinstance(data, list):
        result.fail("'data' must be a list")
        return

    seen_names: set[str] = set()
    for i, entry in enumerate(data):
        if not isinstance(entry, dict):
            result.fail(f"data[{i}] must be a mapping")
            continue

        for key in ("name", "source", "destination"):
            if key not in entry:
                result.fail(f"data[{i}] missing required field: '{key}'")

        name = entry.get("name", "")
        if isinstance(name, str):
            if not _NAME_RE.match(name):
                result.fail(f"data[{i}].name must match [a-zA-Z0-9_-], got: {name!r}")
            if name in seen_names:
                result.warn(f"data: duplicate name {name!r}")
            seen_names.add(name)

        source = entry.get("source", "")
        if isinstance(source, str) and check_paths:
            if not Path(source).exists():
                result.fail(f"data[{i}].source does not exist: {source!r}")

        dest = entry.get("destination", "")
        if isinstance(dest, str) and "$GLACIER_DIR" not in dest:
            result.warn(f"data[{i}].destination does not contain $GLACIER_DIR: {dest!r}")


def _check_platforms(spec: dict[str, Any], result: SpecValidationResult) -> None:
    platforms = spec.get("platforms")
    if platforms is None:
        return
    if not isinstance(platforms, list):
        result.fail("'platforms' must be a list")
        return
    for i, p in enumerate(platforms):
        if not isinstance(p, str) or not _PLATFORM_RE.match(p):
            result.fail(f"platforms[{i}]: must be 'linux/amd64' or 'linux/arm64', got {p!r}")


def _check_unresolved(spec: dict[str, Any], result: SpecValidationResult) -> None:
    unresolved = spec.get("unresolved_containers")
    if unresolved:
        result.fail(
            f"'unresolved_containers' contains {len(unresolved)} entry(ies) that must be "
            "resolved or removed before packing"
        )
