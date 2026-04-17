import gzip
import hashlib
import io
import json
import tarfile
from pathlib import Path

import pytest

from shard.manifest import SHARD_SPEC_VERSION
from shard.validate import validate


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_archive(tmp_path: Path, manifest: dict, members: dict[str, bytes]) -> Path:
    """Build a .shard archive in tmp_path with given manifest dict and extra members."""
    archive = tmp_path / "test.shard"
    manifest_bytes = json.dumps(manifest, indent=2).encode()
    with tarfile.open(archive, "w:gz") as tf:
        _add_bytes(tf, "manifest.json", manifest_bytes)
        for name, data in members.items():
            _add_bytes(tf, name, data)
    return archive


def _add_bytes(tf: tarfile.TarFile, name: str, data: bytes) -> None:
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    tf.addfile(info, io.BytesIO(data))


def _minimal_manifest(workflow_sha: str) -> dict:
    return {
        "shard_spec_version": SHARD_SPEC_VERSION,
        "name": "test-workflow",
        "version": "1.0.0",
        "created_at": "2026-04-17T12:00:00+00:00",
        "workflow": {
            "path": "workflow/test-workflow.bundle",
            "sha256": workflow_sha,
            "git_commit": "a" * 40,
            "git_ref": "v1.0.0",
        },
        "containers": [],
        "data": [],
    }


def _container_entry(image: str, platforms: dict[str, tuple[str, str]]) -> dict:
    """Build a container manifest entry. platforms = {key: (path, sha256)}"""
    return {
        "image": image,
        "platforms": {
            k: {"path": path, "sha256": sha}
            for k, (path, sha) in platforms.items()
        },
    }


class TestValidate:
    def test_valid_minimal(self, tmp_path):
        bundle_data = b"fake git bundle content"
        manifest = _minimal_manifest(_sha256(bundle_data))
        archive = _make_archive(
            tmp_path,
            manifest,
            {"workflow/test-workflow.bundle": bundle_data},
        )
        result = validate(archive)
        assert result.ok
        assert result.errors == []

    def test_not_a_tar(self, tmp_path):
        bad = tmp_path / "bad.shard"
        bad.write_bytes(b"this is not a tar file")
        result = validate(bad)
        assert not result.ok
        assert any("gzipped tar" in e for e in result.errors)

    def test_missing_manifest(self, tmp_path):
        archive = tmp_path / "no-manifest.shard"
        with tarfile.open(archive, "w:gz") as tf:
            _add_bytes(tf, "workflow/foo.bundle", b"data")
        result = validate(archive)
        assert not result.ok
        assert any("manifest.json" in e for e in result.errors)

    def test_corrupt_manifest_json(self, tmp_path):
        archive = tmp_path / "bad.shard"
        with tarfile.open(archive, "w:gz") as tf:
            _add_bytes(tf, "manifest.json", b"{ not valid json }")
        result = validate(archive)
        assert not result.ok
        assert any("manifest" in e.lower() for e in result.errors)

    def test_workflow_sha_mismatch(self, tmp_path):
        bundle_data = b"fake git bundle"
        wrong_sha = "0" * 64
        manifest = _minimal_manifest(wrong_sha)
        archive = _make_archive(
            tmp_path,
            manifest,
            {"workflow/test-workflow.bundle": bundle_data},
        )
        result = validate(archive)
        assert not result.ok
        assert any("SHA-256 mismatch" in e for e in result.errors)

    def test_missing_workflow_member(self, tmp_path):
        manifest = _minimal_manifest("a" * 64)
        archive = _make_archive(tmp_path, manifest, {})
        result = validate(archive)
        assert not result.ok
        assert any("workflow/test-workflow.bundle" in e for e in result.errors)

    def test_valid_with_container_single_platform(self, tmp_path):
        bundle_data = b"bundle"
        container_data = b"docker image tar"
        container_path = "containers/example-image-1.0-amd64.tar"
        manifest = _minimal_manifest(_sha256(bundle_data))
        manifest["containers"] = [
            _container_entry(
                "example/image:1.0",
                {"linux/amd64": (container_path, _sha256(container_data))},
            )
        ]
        archive = _make_archive(
            tmp_path,
            manifest,
            {
                "workflow/test-workflow.bundle": bundle_data,
                container_path: container_data,
            },
        )
        result = validate(archive)
        assert result.ok

    def test_valid_with_container_two_platforms(self, tmp_path):
        bundle_data = b"bundle"
        amd64_data = b"amd64 image tar"
        arm64_data = b"arm64 image tar"
        manifest = _minimal_manifest(_sha256(bundle_data))
        manifest["containers"] = [
            _container_entry(
                "example/image:1.0",
                {
                    "linux/amd64": ("containers/example-image-1.0-amd64.tar", _sha256(amd64_data)),
                    "linux/arm64": ("containers/example-image-1.0-arm64.tar", _sha256(arm64_data)),
                },
            )
        ]
        archive = _make_archive(
            tmp_path,
            manifest,
            {
                "workflow/test-workflow.bundle": bundle_data,
                "containers/example-image-1.0-amd64.tar": amd64_data,
                "containers/example-image-1.0-arm64.tar": arm64_data,
            },
        )
        result = validate(archive)
        assert result.ok

    def test_container_sha_mismatch(self, tmp_path):
        bundle_data = b"bundle"
        container_data = b"docker image tar"
        container_path = "containers/example-image-1.0-amd64.tar"
        manifest = _minimal_manifest(_sha256(bundle_data))
        manifest["containers"] = [
            _container_entry(
                "example/image:1.0",
                {"linux/amd64": (container_path, "0" * 64)},
            )
        ]
        archive = _make_archive(
            tmp_path,
            manifest,
            {
                "workflow/test-workflow.bundle": bundle_data,
                container_path: container_data,
            },
        )
        result = validate(archive)
        assert not result.ok
        assert any("SHA-256 mismatch" in e for e in result.errors)

    def test_valid_with_data(self, tmp_path):
        bundle_data = b"bundle"
        model_data = b"model weights"
        manifest = _minimal_manifest(_sha256(bundle_data))
        manifest["data"] = [{
            "name": "models",
            "path": "data/models",
            "destination": "$GLACIER_DIR/data/models",
            "files": [{"path": "model.bin", "sha256": _sha256(model_data)}],
        }]
        archive = _make_archive(
            tmp_path,
            manifest,
            {
                "workflow/test-workflow.bundle": bundle_data,
                "data/models/model.bin": model_data,
            },
        )
        result = validate(archive)
        assert result.ok

    def test_data_file_sha_mismatch(self, tmp_path):
        bundle_data = b"bundle"
        model_data = b"model weights"
        manifest = _minimal_manifest(_sha256(bundle_data))
        manifest["data"] = [{
            "name": "models",
            "path": "data/models",
            "destination": "$GLACIER_DIR/data/models",
            "files": [{"path": "model.bin", "sha256": "0" * 64}],
        }]
        archive = _make_archive(
            tmp_path,
            manifest,
            {
                "workflow/test-workflow.bundle": bundle_data,
                "data/models/model.bin": model_data,
            },
        )
        result = validate(archive)
        assert not result.ok

    def test_archive_does_not_exist(self, tmp_path):
        result = validate(tmp_path / "nonexistent.shard")
        assert not result.ok
        assert any("Cannot open" in e for e in result.errors)

    def test_multiple_errors_reported(self, tmp_path):
        bundle_data = b"bundle"
        container_data = b"image"
        amd64_path = "containers/example-amd64.tar"
        arm64_path = "containers/example-arm64.tar"
        manifest = _minimal_manifest("0" * 64)
        manifest["containers"] = [
            _container_entry(
                "example/image:1.0",
                {
                    "linux/amd64": (amd64_path, "0" * 64),
                    "linux/arm64": (arm64_path, "0" * 64),
                },
            )
        ]
        archive = _make_archive(
            tmp_path,
            manifest,
            {
                "workflow/test-workflow.bundle": bundle_data,
                amd64_path: container_data,
                arm64_path: container_data,
            },
        )
        result = validate(archive)
        assert not result.ok
        assert len(result.errors) == 3  # workflow + amd64 + arm64
