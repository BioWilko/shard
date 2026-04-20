"""Generate a shard.yml spec from a Nextflow repository."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import yaml

from .nfconfig import NextflowConfigParser

_DOCKER_RE = re.compile(
    r"^[a-zA-Z0-9._\-]+"
    r"(/[a-zA-Z0-9._\-]+)*"
    r"(:[a-zA-Z0-9._\-]+(\-[a-zA-Z0-9._]+)*)?"
    r"(@sha256:[0-9a-f]+)?$"
)
_BAD_SCHEMES = ("https://", "oras://", "shub://", "docker://")

_INCLUDE_CONFIG_RE = re.compile(r"""^\s*includeConfig\s+['"]([^'"]+)['"]""", re.MULTILINE)

_CONTAINER_LITERAL = re.compile(r"""\bcontainer\s+['"]([^'"]+)['"]""")
_INTERP_VAR = re.compile(r"\$\{([^}]+)\}")
# Matches container "..." blocks where the string spans multiple lines (nf-core ternary pattern).
# Inner content uses single quotes so no " appears inside the outer double-quoted string.
_MULTILINE_CONTAINER = re.compile(r'\bcontainer\s+"(\$\{[^"]*?\})"', re.DOTALL)
# Extracts the false branch of a Groovy ternary: `: 'value'` or `: "value"`
_TERNARY_FALSE = re.compile(r":\s+'([^']+)'|:\s+\"([^\"]+)\"")


def generate(repo_path: Path, data_path: Path | None = None) -> tuple[dict[str, Any], list[str]]:
    """Return (spec_dict, warnings). spec_dict is suitable for yaml.dump."""
    warnings: list[str] = []
    parser = NextflowConfigParser()

    name, version, description = _detect_metadata(repo_path, parser, warnings)

    containers, unresolved = _detect_containers(repo_path, parser)

    docker_registry = _detect_docker_registry(repo_path, parser)
    if docker_registry:
        containers = [
            f"{docker_registry}/{c}" if not _has_registry(c) else c
            for c in containers
        ]

    data_entries: list[dict[str, str]] = []
    if data_path is not None:
        data_entries = _build_data_entries(data_path)

    spec: dict[str, Any] = {
        "name": name,
        "version": version,
    }
    if description:
        spec["description"] = description
    resolved = repo_path.resolve()
    workflow_path = "." if resolved == Path.cwd() else str(resolved)
    spec["workflow"] = {"path": workflow_path, "ref": "HEAD"}
    spec["platforms"] = ["linux/amd64", "linux/arm64"]
    spec["containers"] = containers
    if unresolved:
        spec["unresolved_containers"] = unresolved
        warnings.append(
            f"{len(unresolved)} container(s) could not be resolved — "
            "edit 'unresolved_containers' in the generated shard.yml"
        )
    if data_entries:
        spec["data"] = data_entries

    return spec, warnings


def _detect_metadata(
    repo_path: Path,
    parser: NextflowConfigParser,
    warnings: list[str],
) -> tuple[str, str, str]:
    name = version = description = ""

    config_path = repo_path / "nextflow.config"
    if config_path.exists():
        blocks = parser.parse(config_path.read_text())
        manifest_block = blocks.get("manifest", {})
        name = manifest_block.get("name", "")
        version = manifest_block.get("version", "")
        description = manifest_block.get("description", "")

    if not version:
        version = _git_latest_tag(repo_path)
    if not version:
        version = "0.1.0"
        warnings.append("Could not detect version — defaulting to '0.1.0'")

    if not name:
        name = repo_path.resolve().name
        warnings.append(f"Could not detect name from nextflow.config — using directory name '{name}'")

    return name, version, description


def _git_latest_tag(repo_path: Path) -> str:
    result = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0"],
        capture_output=True,
        text=True,
        cwd=repo_path,
    )
    if result.returncode == 0:
        return result.stdout.strip()
    return ""


_DOCKER_REGISTRY_RE = re.compile(r"""docker\.registry\s*=\s*['"]([^'"]+)['"]""")


def _detect_docker_registry(repo_path: Path, parser: NextflowConfigParser) -> str:
    config_path = repo_path / "nextflow.config"
    if not config_path.exists():
        return ""
    text = config_path.read_text()
    # Structured parse covers top-level docker.registry = '...' assignments.
    blocks = parser.parse(text)
    docker_block = blocks.get("docker", {})
    if isinstance(docker_block, dict) and docker_block.get("registry"):
        return docker_block["registry"]
    # Regex fallback for docker.registry inside nested blocks (e.g. profiles).
    m = _DOCKER_REGISTRY_RE.search(text)
    return m.group(1) if m else ""


def _has_registry(image: str) -> bool:
    """Return True if image already has an explicit registry hostname (contains a dot before the first slash)."""
    if "/" not in image:
        return False
    first = image.split("/")[0]
    return "." in first or ":" in first or first == "localhost"


def _collect_included_configs(config_path: Path, visited: set[Path] | None = None) -> list[Path]:
    """Return config_path plus all transitively included configs, in include order."""
    if visited is None:
        visited = set()
    resolved = config_path.resolve()
    if resolved in visited or not resolved.exists():
        return []
    visited.add(resolved)
    result = [resolved]
    try:
        text = resolved.read_text(errors="replace")
    except OSError:
        return result
    for m in _INCLUDE_CONFIG_RE.finditer(text):
        included = (resolved.parent / m.group(1)).resolve()
        result.extend(_collect_included_configs(included, visited))
    return result


def _flatten_params(d: dict, prefix: str = "") -> dict[str, str]:
    """Recursively flatten a nested params dict into dotted keys."""
    result: dict[str, str] = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, str):
            result[key] = v
        elif isinstance(v, dict):
            result.update(_flatten_params(v, key))
    return result


def _detect_containers(
    repo_path: Path,
    parser: NextflowConfigParser,
) -> tuple[list[str], list[str]]:
    seen: dict[str, None] = {}
    unresolved: list[str] = []

    # First pass: collect params. Follow includeConfig from nextflow.config first
    # (preserving include order), then fall back to rglob for any remaining configs.
    global_params: dict[str, str] = {}
    included: list[Path] = []
    main_config = repo_path / "nextflow.config"
    if main_config.exists():
        included = _collect_included_configs(main_config)
    included_set = set(included)
    remaining = sorted(p.resolve() for p in repo_path.rglob("*.config") if p.resolve() not in included_set)

    for path in included + remaining:
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        blocks = parser.parse(text)
        raw_params = blocks.get("params", {})
        if isinstance(raw_params, dict):
            global_params.update(_flatten_params(raw_params))
        blocks_no_params = {k: v for k, v in blocks.items() if k != "params"}
        for container_val in NextflowConfigParser.get_all(blocks_no_params, "container"):
            if _is_docker_image(container_val):
                seen.setdefault(container_val, None)
        _extract_containers(text, global_params, seen, unresolved)

    # Second pass: process .nf files with the full accumulated params.
    for path in sorted(repo_path.rglob("*.nf")):
        try:
            text = path.read_text(errors="replace")
        except OSError:
            continue
        _extract_containers(text, global_params, seen, unresolved)

    return list(seen.keys()), unresolved


def _extract_containers(
    text: str,
    params: dict[str, str],
    seen: dict[str, None],
    unresolved: list[str],
) -> None:
    # Pass 0: multi-line container blocks (nf-core Groovy ternary pattern).
    # Matches container "${...}" spanning multiple lines; inner strings use single
    # quotes so the outer double-quoted boundary is unambiguous.
    for m in _MULTILINE_CONTAINER.finditer(text):
        for grp1, grp2 in _TERNARY_FALSE.findall(m.group(1)):
            lit = grp1 or grp2
            if _is_docker_image(lit):
                seen.setdefault(lit, None)

    lines = text.splitlines()
    for line in lines:
        # Pass 1: simple literal on same line as container directive
        for m in _CONTAINER_LITERAL.finditer(line):
            candidate = m.group(1)
            if _is_docker_image(candidate):
                seen.setdefault(candidate, None)

        # Pass 2: interpolated string `"image:${params.X}"` on a single line
        if "container" in line and "${" in line:
            interp_strings = re.findall(r'\bcontainer\s+"([^"]*\$\{[^}]+\}[^"]*)"', line)
            interp_strings += re.findall(r"\bcontainer\s+'([^']*\$\{[^}]+\}[^']*)'", line)
            for tmpl in interp_strings:
                resolved, ok = _resolve_interpolation(tmpl, params)
                if ok and _is_docker_image(resolved):
                    seen.setdefault(resolved, None)
                elif not ok:
                    if tmpl not in unresolved:
                        unresolved.append(tmpl)


def _resolve_interpolation(tmpl: str, params: dict[str, str]) -> tuple[str, bool]:
    """Substitute ${params.X} or ${X} from params dict. Return (result, resolved)."""
    result = tmpl
    all_resolved = True
    for m in _INTERP_VAR.finditer(tmpl):
        expr = m.group(1)
        # params.foo → look up foo; bare foo → look up foo
        key = expr.removeprefix("params.")
        if key in params:
            result = result.replace(m.group(0), params[key])
        else:
            all_resolved = False
    return result, all_resolved


def _is_docker_image(s: str) -> bool:
    if not s:
        return False
    if any(s.startswith(scheme) for scheme in _BAD_SCHEMES):
        return False
    return bool(_DOCKER_RE.match(s))


def _build_data_entries(data_path: Path) -> list[dict[str, str]]:
    entries = []
    for child in sorted(data_path.iterdir()):
        if child.name.startswith("."):
            continue
        entries.append({
            "name": child.name,
            "source": str(child.resolve()),
            "destination": f"$GLACIER_DIR/data/{child.name}",
        })
    return entries
