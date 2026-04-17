import copy
import json
import pytest
from shard.manifest import (
    Manifest,
    ManifestError,
    SHARD_SPEC_VERSION,
)

MINIMAL_MANIFEST = {
    "shard_spec_version": 1,
    "name": "test-workflow",
    "version": "1.0.0",
    "created_at": "2026-04-17T12:00:00+00:00",
    "workflow": {
        "path": "workflow/test-workflow.bundle",
        "sha256": "a" * 64,
        "git_commit": "b" * 40,
        "git_ref": "v1.0.0",
    },
}


def make_manifest(**overrides):
    d = copy.deepcopy(MINIMAL_MANIFEST)
    d.update(overrides)
    return d


def _platform_entry(arch: str, prefix: str = "c") -> dict:
    return {
        "path": f"containers/example-image-1.0-{arch}.tar",
        "sha256": prefix * 64,
    }


class TestManifestParsing:
    def test_minimal_valid(self):
        m = Manifest.from_dict(MINIMAL_MANIFEST)
        assert m.name == "test-workflow"
        assert m.version == "1.0.0"
        assert m.shard_spec_version == 1
        assert m.containers == []
        assert m.data == []
        assert m.description == ""

    def test_description_optional(self):
        d = make_manifest(description="A workflow")
        m = Manifest.from_dict(d)
        assert m.description == "A workflow"

    def test_missing_top_level_field(self):
        for field in ("shard_spec_version", "name", "version", "created_at", "workflow"):
            d = dict(MINIMAL_MANIFEST)
            del d[field]
            with pytest.raises(ManifestError, match=field):
                Manifest.from_dict(d)

    def test_invalid_spec_version_zero(self):
        with pytest.raises(ManifestError, match="shard_spec_version"):
            Manifest.from_dict(make_manifest(shard_spec_version=0))

    def test_spec_version_too_new(self):
        with pytest.raises(ManifestError, match="newer than supported"):
            Manifest.from_dict(make_manifest(shard_spec_version=SHARD_SPEC_VERSION + 1))

    def test_missing_workflow_field(self):
        for field in ("path", "sha256", "git_commit", "git_ref"):
            d = make_manifest()
            del d["workflow"][field]
            with pytest.raises(ManifestError, match=field):
                Manifest.from_dict(d)

    def test_container_entry(self):
        d = make_manifest(containers=[{
            "image": "example/image:1.0",
            "platforms": {
                "linux/amd64": _platform_entry("amd64"),
            },
        }])
        m = Manifest.from_dict(d)
        assert len(m.containers) == 1
        assert m.containers[0].image == "example/image:1.0"
        assert "linux/amd64" in m.containers[0].platforms

    def test_container_two_platforms(self):
        d = make_manifest(containers=[{
            "image": "example/image:1.0",
            "platforms": {
                "linux/amd64": _platform_entry("amd64", "c"),
                "linux/arm64": _platform_entry("arm64", "d"),
            },
        }])
        m = Manifest.from_dict(d)
        assert set(m.containers[0].platforms.keys()) == {"linux/amd64", "linux/arm64"}

    def test_container_missing_image(self):
        d = make_manifest(containers=[{
            "platforms": {"linux/amd64": _platform_entry("amd64")},
        }])
        with pytest.raises(ManifestError, match="image"):
            Manifest.from_dict(d)

    def test_container_missing_platforms(self):
        d = make_manifest(containers=[{
            "image": "example/image:1.0",
        }])
        with pytest.raises(ManifestError, match="platforms"):
            Manifest.from_dict(d)

    def test_container_platforms_not_object(self):
        d = make_manifest(containers=[{
            "image": "example/image:1.0",
            "platforms": "linux/amd64",
        }])
        with pytest.raises(ManifestError, match="platforms"):
            Manifest.from_dict(d)

    def test_container_platform_entry_missing_field(self):
        for field in ("path", "sha256"):
            entry = _platform_entry("amd64")
            del entry[field]
            d = make_manifest(containers=[{
                "image": "example/image:1.0",
                "platforms": {"linux/amd64": entry},
            }])
            with pytest.raises(ManifestError, match=field):
                Manifest.from_dict(d)

    def test_data_entry(self):
        d = make_manifest(data=[{
            "name": "models",
            "path": "data/models",
            "destination": "$GLACIER_DIR/data/models",
            "files": [{"path": "model.bin", "sha256": "e" * 64}],
        }])
        m = Manifest.from_dict(d)
        assert len(m.data) == 1
        assert m.data[0].name == "models"
        assert len(m.data[0].files) == 1

    def test_data_missing_field(self):
        for field in ("name", "path", "destination"):
            d = make_manifest(data=[{
                "name": "models",
                "path": "data/models",
                "destination": "$GLACIER_DIR/data/models",
                "files": [],
            }])
            del d["data"][0][field]
            with pytest.raises(ManifestError, match=field):
                Manifest.from_dict(d)


class TestManifestSerialization:
    def test_round_trip(self):
        m = Manifest.from_dict(MINIMAL_MANIFEST)
        d = m.to_dict()
        m2 = Manifest.from_dict(d)
        assert m2.name == m.name
        assert m2.workflow.sha256 == m.workflow.sha256

    def test_round_trip_with_container(self):
        d = make_manifest(containers=[{
            "image": "example/image:1.0",
            "platforms": {
                "linux/amd64": _platform_entry("amd64"),
                "linux/arm64": _platform_entry("arm64", "d"),
            },
        }])
        m = Manifest.from_dict(d)
        m2 = Manifest.from_dict(m.to_dict())
        assert m2.containers[0].image == "example/image:1.0"
        assert set(m2.containers[0].platforms.keys()) == {"linux/amd64", "linux/arm64"}

    def test_to_json_is_valid_json(self):
        m = Manifest.from_dict(MINIMAL_MANIFEST)
        parsed = json.loads(m.to_json())
        assert parsed["name"] == "test-workflow"

    def test_from_json_invalid(self):
        with pytest.raises(ManifestError, match="not valid JSON"):
            Manifest.from_json("not json {{{")

    def test_from_json_not_object(self):
        with pytest.raises(ManifestError, match="JSON object"):
            Manifest.from_json("[1, 2, 3]")
