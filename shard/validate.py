"""Validate the integrity of a .shard archive."""

from __future__ import annotations

import hashlib
import io
import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from .manifest import Manifest, ManifestError

MANIFEST_PATH = "manifest.json"
CHUNK = 1 << 20  # 1 MiB


@dataclass
class ValidationResult:
    ok: bool = True
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.ok = False
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)


def validate(archive_path: Path) -> ValidationResult:
    result = ValidationResult()

    try:
        tf = tarfile.open(archive_path, "r:gz")
    except tarfile.TarError as exc:
        result.fail(f"Not a valid gzipped tar archive: {exc}")
        return result
    except OSError as exc:
        result.fail(f"Cannot open archive: {exc}")
        return result

    with tf:
        members = {m.name: m for m in tf.getmembers()}

        if MANIFEST_PATH not in members:
            result.fail("manifest.json not found in archive")
            return result

        manifest_bytes = _extract_bytes(tf, members[MANIFEST_PATH])
        try:
            manifest = Manifest.from_json(manifest_bytes.decode())
        except ManifestError as exc:
            result.fail(f"Invalid manifest: {exc}")
            return result

        _check_member(tf, members, manifest.workflow.path, manifest.workflow.sha256, result)

        for container in manifest.containers:
            for platform_entry in container.platforms.values():
                _check_member(tf, members, platform_entry.path, platform_entry.sha256, result)

        for entry in manifest.data:
            for f in entry.files:
                member_path = f"{entry.path}/{f.path}"
                _check_member(tf, members, member_path, f.sha256, result)

    return result


def _check_member(
    tf: tarfile.TarFile,
    members: dict[str, tarfile.TarInfo],
    path: str,
    expected_sha256: str,
    result: ValidationResult,
) -> None:
    if path not in members:
        result.fail(f"Declared path not found in archive: {path}")
        return
    actual = _sha256_member(tf, members[path])
    if actual != expected_sha256:
        result.fail(
            f"SHA-256 mismatch for {path}\n"
            f"  expected: {expected_sha256}\n"
            f"  actual:   {actual}"
        )


def _extract_bytes(tf: tarfile.TarFile, member: tarfile.TarInfo) -> bytes:
    fh = tf.extractfile(member)
    if fh is None:
        return b""
    return fh.read()


def _sha256_member(tf: tarfile.TarFile, member: tarfile.TarInfo) -> str:
    fh = tf.extractfile(member)
    if fh is None:
        return hashlib.sha256(b"").hexdigest()
    return _sha256_stream(fh)


def _sha256_stream(fh: BinaryIO) -> str:
    h = hashlib.sha256()
    while chunk := fh.read(CHUNK):
        h.update(chunk)
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(CHUNK):
            h.update(chunk)
    return h.hexdigest()
