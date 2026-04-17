"""Build a .shard archive from a shard.yml spec file."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from .manifest import (
    SHARD_SPEC_VERSION,
    ContainerEntry,
    DataEntry,
    DataFile,
    Manifest,
    PlatformEntry,
    WorkflowEntry,
)
from .validate import sha256_file

DEFAULT_PLATFORMS = ["linux/amd64", "linux/arm64"]
_MANIFEST_LIST_MEDIA_TYPE = "application/vnd.docker.distribution.manifest.list.v2+json"


class PackError(RuntimeError):
    pass


def pack(spec_path: Path, out_dir: Path) -> Path:
    spec = _load_spec(spec_path)
    name = spec["name"]
    version = str(spec["version"])
    target_platforms = spec.get("platforms", DEFAULT_PLATFORMS)

    with tempfile.TemporaryDirectory() as tmp:
        staging = Path(tmp)
        workflow_dir = staging / "workflow"
        containers_dir = staging / "containers"
        data_dir = staging / "data"
        workflow_dir.mkdir()
        containers_dir.mkdir()
        data_dir.mkdir()

        workflow_entry = _bundle_workflow(spec["workflow"], name, workflow_dir)
        container_entries = _save_containers_multiarch(
            spec.get("containers", []), containers_dir, target_platforms
        )
        data_entries = _copy_data(spec.get("data", []), data_dir)

        manifest = Manifest(
            shard_spec_version=SHARD_SPEC_VERSION,
            name=name,
            version=version,
            created_at=datetime.now(timezone.utc).isoformat(),
            description=spec.get("description", ""),
            workflow=workflow_entry,
            containers=container_entries,
            data=data_entries,
        )
        (staging / "manifest.json").write_text(manifest.to_json())

        safe_name = name.replace("/", "-")
        archive_path = out_dir / f"{safe_name}-{version}.shard"
        _create_archive(staging, archive_path)

    return archive_path


def _load_spec(path: Path) -> dict[str, Any]:
    with open(path) as fh:
        spec = yaml.safe_load(fh)
    if not isinstance(spec, dict):
        raise PackError(f"{path}: expected a YAML mapping")
    for key in ("name", "version", "workflow"):
        if key not in spec:
            raise PackError(f"{path}: missing required key '{key}'")
    return spec


def _bundle_workflow(wf_spec: dict[str, Any], name: str, dest: Path) -> WorkflowEntry:
    repo_path = Path(wf_spec.get("path", ".")).resolve()
    ref = wf_spec.get("ref", "HEAD")

    if not (repo_path / ".git").exists():
        raise PackError(f"Workflow path is not a git repository: {repo_path}")

    try:
        commit = _git(["rev-parse", ref], cwd=repo_path).strip()
    except PackError:
        no_commits = (
            subprocess.run(
                ["git", "log", "-1"], capture_output=True, cwd=repo_path
            ).returncode
            != 0
        )
        if no_commits:
            raise PackError(
                f"Workflow repository at {repo_path} has no commits. "
                "Create at least one commit before packing."
            )
        raise

    bundle_file = dest / f"{name.replace('/', '-')}.bundle"
    _git(["bundle", "create", str(bundle_file), "--all"], cwd=repo_path)

    return WorkflowEntry(
        path=f"workflow/{bundle_file.name}",
        sha256=sha256_file(bundle_file),
        git_commit=commit,
        git_ref=ref,
    )


def _save_containers_multiarch(
    images: list[str],
    dest: Path,
    target_platforms: list[str],
) -> list[ContainerEntry]:
    entries = []
    for image in images:
        platforms = _save_image_multiarch(image, dest, target_platforms)
        if not platforms:
            raise PackError(f"No platform variants saved for {image!r}")
        entries.append(ContainerEntry(image=image, platforms=platforms))
    return entries


def _save_image_multiarch(
    image: str,
    dest: Path,
    target_platforms: list[str],
) -> dict[str, PlatformEntry]:
    result = subprocess.run(
        ["docker", "manifest", "inspect", image],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        try:
            manifest_data = json.loads(result.stdout)
        except json.JSONDecodeError:
            manifest_data = {}
    else:
        manifest_data = {}

    media_type = manifest_data.get("mediaType", "")

    if media_type == _MANIFEST_LIST_MEDIA_TYPE:
        return _save_manifest_list(image, manifest_data, dest, target_platforms)
    else:
        return _save_single_arch(image, dest, target_platforms)


def _save_manifest_list(
    image: str,
    manifest_data: dict[str, Any],
    dest: Path,
    target_platforms: list[str],
) -> dict[str, PlatformEntry]:
    target_arches = {p.split("/")[1] for p in target_platforms if "/" in p}
    platform_entries: dict[str, PlatformEntry] = {}

    for m in manifest_data.get("manifests", []):
        platform = m.get("platform", {})
        os_ = platform.get("os", "")
        arch = platform.get("architecture", "")
        if os_ != "linux" or arch not in target_arches:
            continue

        digest = m["digest"]
        platform_key = f"linux/{arch}"
        safe_name = _image_to_filename(image, arch)
        tar_path = dest / safe_name

        _docker_run(["pull", f"{image}@{digest}"])
        _docker_run(["tag", f"{image}@{digest}", image])
        _docker_run(["save", image, "-o", str(tar_path)])
        _docker_run(["rmi", image])

        platform_entries[platform_key] = PlatformEntry(
            path=f"containers/{safe_name}",
            sha256=sha256_file(tar_path),
        )

    missing = [p for p in target_platforms if p not in platform_entries]
    for p in missing:
        print(f"warning: {image!r} has no variant for {p} — skipping")

    return platform_entries


def _save_single_arch(
    image: str,
    dest: Path,
    target_platforms: list[str],
) -> dict[str, PlatformEntry]:
    _docker_run(["pull", image])

    result = subprocess.run(
        ["docker", "image", "inspect", image, "--format", "{{.Architecture}}"],
        capture_output=True,
        text=True,
    )
    arch = result.stdout.strip() if result.returncode == 0 else "amd64"
    platform_key = f"linux/{arch}"

    if platform_key not in target_platforms:
        print(
            f"warning: {image!r} architecture {platform_key!r} not in target platforms"
        )

    safe_name = _image_to_filename(image, arch)
    tar_path = dest / safe_name
    _docker_run(["save", image, "-o", str(tar_path)])

    return {
        platform_key: PlatformEntry(
            path=f"containers/{safe_name}",
            sha256=sha256_file(tar_path),
        )
    }


def _docker_run(args: list[str]) -> None:
    result = subprocess.run(["docker", *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise PackError(f"docker {args[0]} failed:\n{result.stderr}")


def _copy_data(data_specs: list[dict[str, Any]], dest: Path) -> list[DataEntry]:
    entries = []
    for spec in data_specs:
        for key in ("name", "source", "destination"):
            if key not in spec:
                raise PackError(f"data entry missing required key '{key}'")
        entry_name = spec["name"]
        source = Path(spec["source"])
        if not source.exists():
            raise PackError(f"data source does not exist: {source}")

        entry_dest = dest / entry_name
        if source.is_dir():
            shutil.copytree(source, entry_dest)
        else:
            entry_dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, entry_dest / source.name)

        files = _collect_files(entry_dest, entry_dest)
        entries.append(
            DataEntry(
                name=entry_name,
                path=f"data/{entry_name}",
                destination=spec["destination"],
                files=files,
            )
        )
    return entries


def _collect_files(root: Path, base: Path) -> list[DataFile]:
    files = []
    for p in sorted(root.rglob("*")):
        if p.is_file():
            rel = p.relative_to(base)
            files.append(DataFile(path=str(rel), sha256=sha256_file(p)))
    return files


def _create_archive(staging: Path, out: Path) -> None:
    with tarfile.open(out, "w:gz") as tf:
        for item in sorted(staging.rglob("*")):
            arcname = item.relative_to(staging)
            tf.add(item, arcname=str(arcname))


def _git(args: list[str], cwd: Path) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        raise PackError(f"git {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def _image_to_filename(image: str, arch: str) -> str:
    safe = re.sub(r"[/:@]", "-", image).strip("-")
    return f"{safe}-{arch}.tar"
