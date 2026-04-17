"""Manifest schema, dataclasses, and validation for .shard archives."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

SHARD_SPEC_VERSION = 1


@dataclass
class WorkflowEntry:
    path: str
    sha256: str
    git_commit: str
    git_ref: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> WorkflowEntry:
        _require_fields(d, ("path", "sha256", "git_commit", "git_ref"), "workflow")
        return cls(
            path=d["path"],
            sha256=d["sha256"],
            git_commit=d["git_commit"],
            git_ref=d["git_ref"],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "git_commit": self.git_commit,
            "git_ref": self.git_ref,
        }


@dataclass
class PlatformEntry:
    path: str
    sha256: str

    @classmethod
    def from_dict(cls, d: dict[str, Any], context: str = "platforms[]") -> PlatformEntry:
        _require_fields(d, ("path", "sha256"), context)
        return cls(path=d["path"], sha256=d["sha256"])

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "sha256": self.sha256}


@dataclass
class ContainerEntry:
    image: str
    platforms: dict[str, PlatformEntry]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ContainerEntry:
        _require_fields(d, ("image", "platforms"), "containers[]")
        if not isinstance(d["platforms"], dict):
            raise ManifestError("containers[].platforms must be an object")
        platforms = {
            k: PlatformEntry.from_dict(v, f"containers[].platforms[{k!r}]")
            for k, v in d["platforms"].items()
        }
        return cls(image=d["image"], platforms=platforms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "image": self.image,
            "platforms": {k: v.to_dict() for k, v in self.platforms.items()},
        }


@dataclass
class DataFile:
    path: str
    sha256: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DataFile:
        _require_fields(d, ("path", "sha256"), "data[].files[]")
        return cls(path=d["path"], sha256=d["sha256"])

    def to_dict(self) -> dict[str, Any]:
        return {"path": self.path, "sha256": self.sha256}


@dataclass
class DataEntry:
    name: str
    path: str
    destination: str
    files: list[DataFile] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> DataEntry:
        _require_fields(d, ("name", "path", "destination"), "data[]")
        files = [DataFile.from_dict(f) for f in d.get("files", [])]
        return cls(
            name=d["name"],
            path=d["path"],
            destination=d["destination"],
            files=files,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "path": self.path,
            "destination": self.destination,
            "files": [f.to_dict() for f in self.files],
        }


@dataclass
class Manifest:
    shard_spec_version: int
    name: str
    version: str
    created_at: str
    workflow: WorkflowEntry
    containers: list[ContainerEntry] = field(default_factory=list)
    data: list[DataEntry] = field(default_factory=list)
    description: str = ""

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Manifest:
        _require_fields(d, ("shard_spec_version", "name", "version", "created_at", "workflow"), "manifest")
        spec_version = d["shard_spec_version"]
        if not isinstance(spec_version, int) or spec_version < 1:
            raise ManifestError(f"Invalid shard_spec_version: {spec_version!r}")
        if spec_version > SHARD_SPEC_VERSION:
            raise ManifestError(
                f"shard_spec_version {spec_version} is newer than supported version {SHARD_SPEC_VERSION}"
            )
        return cls(
            shard_spec_version=spec_version,
            name=d["name"],
            version=d["version"],
            created_at=d["created_at"],
            description=d.get("description", ""),
            workflow=WorkflowEntry.from_dict(d["workflow"]),
            containers=[ContainerEntry.from_dict(c) for c in d.get("containers", [])],
            data=[DataEntry.from_dict(e) for e in d.get("data", [])],
        )

    @classmethod
    def from_json(cls, text: str) -> Manifest:
        try:
            d = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ManifestError(f"manifest.json is not valid JSON: {exc}") from exc
        if not isinstance(d, dict):
            raise ManifestError("manifest.json must be a JSON object")
        return cls.from_dict(d)

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "shard_spec_version": self.shard_spec_version,
            "name": self.name,
            "version": self.version,
            "created_at": self.created_at,
            "workflow": self.workflow.to_dict(),
            "containers": [c.to_dict() for c in self.containers],
            "data": [e.to_dict() for e in self.data],
        }
        if self.description:
            d["description"] = self.description
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


class ManifestError(ValueError):
    pass


def _require_fields(d: dict[str, Any], fields: tuple[str, ...], context: str) -> None:
    missing = [f for f in fields if f not in d]
    if missing:
        raise ManifestError(f"Missing required field(s) in {context}: {', '.join(missing)}")
