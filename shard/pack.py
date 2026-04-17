"""Build a .shard archive from a shard.yml spec file."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
_MANIFEST_LIST_MEDIA_TYPES = {
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.index.v1+json",
}


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

        t0 = time.time()
        workflow_entry = _bundle_workflow(spec["workflow"], name, workflow_dir)
        bundle_size = _human_size(
            (workflow_dir / Path(workflow_entry.path).name).stat().st_size
        )
        print(
            f"[workflow] bundled {workflow_entry.git_commit[:12]} ({bundle_size}) in {time.time()-t0:.1f}s"
        )

        n_images = len(spec.get("containers", []))
        n_platforms = len(target_platforms)
        print(
            f"[containers] pulling {n_images} image(s) x {n_platforms} platform(s)..."
        )
        t0 = time.time()
        container_entries = _save_containers_multiarch(
            spec.get("containers", []), containers_dir, target_platforms
        )
        containers_size = _human_size(
            sum(f.stat().st_size for f in containers_dir.rglob("*") if f.is_file())
        )
        print(
            f"[containers] saved {n_images} image(s) ({containers_size}) in {time.time()-t0:.1f}s"
        )

        t0 = time.time()
        data_entries = _copy_data(spec.get("data", []), data_dir)
        n_files = sum(len(e.files) for e in data_entries)
        data_size = _human_size(
            sum(f.stat().st_size for f in data_dir.rglob("*") if f.is_file())
        )
        print(
            f"[data] copied {len(data_entries)} entr{'y' if len(data_entries) == 1 else 'ies'}, {n_files} file(s) ({data_size}) in {time.time()-t0:.1f}s"
        )

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
        staging_size = _human_size(
            sum(f.stat().st_size for f in staging.rglob("*") if f.is_file())
        )
        print(f"[archive] compressing {staging_size} of staged content...")
        t0 = time.time()
        _create_archive(staging, archive_path)
        archive_size = _human_size(archive_path.stat().st_size)
        print(
            f"[archive] wrote {archive_path.name} ({archive_size}) in {time.time()-t0:.1f}s"
        )

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
    if not images:
        return []

    def _save_one(image: str) -> ContainerEntry:
        platforms = _save_image_multiarch(image, dest, target_platforms)
        if not platforms:
            raise PackError(f"No platform variants saved for {image!r}")
        return ContainerEntry(image=image, platforms=platforms)

    with ThreadPoolExecutor(max_workers=len(images)) as pool:
        futures = {pool.submit(_save_one, img): img for img in images}
        results: dict[str, ContainerEntry] = {}
        for fut in as_completed(futures):
            results[futures[fut]] = fut.result()

    return [results[img] for img in images]


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

    if media_type in _MANIFEST_LIST_MEDIA_TYPES:
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

    targets = []
    for m in manifest_data.get("manifests", []):
        platform = m.get("platform", {})
        os_ = platform.get("os", "")
        arch = platform.get("architecture", "")
        if os_ != "linux" or arch not in target_arches:
            continue
        targets.append((arch, m["digest"]))

    def _save_platform(arch: str, digest: str) -> tuple[str, PlatformEntry]:
        ref = f"{image}@{digest}"
        safe_name = _image_to_filename(image, arch)
        tar_path = dest / safe_name
        _docker_run(["pull", ref])
        _docker_run(["save", ref, "-o", str(tar_path)])
        return f"linux/{arch}", PlatformEntry(
            path=f"containers/{safe_name}",
            sha256=sha256_file(tar_path),
        )

    platform_entries: dict[str, PlatformEntry] = {}
    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = [
            pool.submit(_save_platform, arch, digest) for arch, digest in targets
        ]
        for fut in as_completed(futures):
            platform_key, entry = fut.result()
            platform_entries[platform_key] = entry

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


def _docker_run(args: list[str], retries: int = 3) -> None:
    for attempt in range(retries):
        result = subprocess.run(["docker", *args], capture_output=True, text=True)
        if result.returncode == 0:
            return
        if attempt < retries - 1:
            print(
                f"warning: docker {args[0]} failed (attempt {attempt + 1}/{retries}), retrying..."
            )
            time.sleep(2**attempt)
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


def _human_size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _create_archive(staging: Path, out: Path) -> None:
    compressor = shutil.which("pigz") or shutil.which("gzip")
    if compressor is None:
        raise PackError("pigz or gzip not found on PATH")

    cmd = [compressor]
    if "pigz" in compressor:
        import os

        cmd += ["-p", str(os.cpu_count() or 1)]

    with out.open("wb") as outf:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=outf)
        assert proc.stdin is not None
        try:
            with tarfile.open(fileobj=proc.stdin, mode="w|") as tf:
                for item in sorted(
                    staging.rglob("*"),
                    key=lambda p: (p.name != "manifest.json", str(p)),
                ):
                    arcname = item.relative_to(staging)
                    tf.add(item, arcname=str(arcname))
        finally:
            proc.stdin.close()
        proc.wait()
        if proc.returncode != 0:
            raise PackError(f"{compressor} failed with exit code {proc.returncode}")


def _git(args: list[str], cwd: Path) -> str:
    result = subprocess.run(["git", *args], capture_output=True, text=True, cwd=cwd)
    if result.returncode != 0:
        raise PackError(f"git {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout


def _image_to_filename(image: str, arch: str) -> str:
    safe = re.sub(r"[/:@]", "-", image).strip("-")
    return f"{safe}-{arch}.tar"
