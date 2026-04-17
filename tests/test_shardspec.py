import pytest
from pathlib import Path

import yaml

from shard.shardspec import validate_spec


def _write_spec(tmp_path: Path, spec: dict) -> Path:
    p = tmp_path / "shard.yml"
    p.write_text(yaml.dump(spec, sort_keys=False))
    return p


MINIMAL_SPEC = {
    "name": "my-workflow",
    "version": "1.0.0",
    "workflow": {"path": ".", "ref": "v1.0.0"},
}


class TestValidateSpec:
    def test_minimal_valid(self, tmp_path):
        p = _write_spec(tmp_path, MINIMAL_SPEC)
        result = validate_spec(p, check_paths=False)
        assert result.ok
        assert result.errors == []

    def test_missing_name(self, tmp_path):
        spec = {k: v for k, v in MINIMAL_SPEC.items() if k != "name"}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("name" in e for e in result.errors)

    def test_missing_version(self, tmp_path):
        spec = {k: v for k, v in MINIMAL_SPEC.items() if k != "version"}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("version" in e for e in result.errors)

    def test_missing_workflow(self, tmp_path):
        spec = {k: v for k, v in MINIMAL_SPEC.items() if k != "workflow"}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("workflow" in e for e in result.errors)

    def test_invalid_name_chars(self, tmp_path):
        spec = {**MINIMAL_SPEC, "name": "my workflow!"}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("name" in e for e in result.errors)

    def test_org_repo_name_valid(self, tmp_path):
        spec = {**MINIMAL_SPEC, "name": "artic-network/amplicon-nf"}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok

    def test_numeric_version_warns(self, tmp_path):
        spec = {**MINIMAL_SPEC, "version": 1}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok
        assert any("version" in w for w in result.warnings)

    def test_missing_workflow_ref_warns(self, tmp_path):
        spec = {**MINIMAL_SPEC, "workflow": {"path": "."}}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok
        assert any("ref" in w for w in result.warnings)

    def test_valid_containers(self, tmp_path):
        spec = {**MINIMAL_SPEC, "containers": ["ubuntu:22.04", "artic/fieldbioinformatics:1.10.0"]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok

    def test_container_not_string(self, tmp_path):
        spec = {**MINIMAL_SPEC, "containers": [{"image": "ubuntu:22.04"}]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok

    def test_container_bad_scheme(self, tmp_path):
        spec = {**MINIMAL_SPEC, "containers": ["https://example.com/image"]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("scheme" in e for e in result.errors)

    def test_duplicate_container_warns(self, tmp_path):
        spec = {**MINIMAL_SPEC, "containers": ["ubuntu:22.04", "ubuntu:22.04"]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok
        assert any("duplicate" in w for w in result.warnings)

    def test_valid_data(self, tmp_path):
        data_src = tmp_path / "mydata"
        data_src.mkdir()
        spec = {**MINIMAL_SPEC, "data": [{
            "name": "mydata",
            "source": str(data_src),
            "destination": "$GLACIER_DIR/data/mydata",
        }]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=True)
        assert result.ok

    def test_data_missing_source_field(self, tmp_path):
        spec = {**MINIMAL_SPEC, "data": [{"name": "d", "destination": "$GLACIER_DIR/d"}]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("source" in e for e in result.errors)

    def test_data_nonexistent_source(self, tmp_path):
        spec = {**MINIMAL_SPEC, "data": [{
            "name": "missing",
            "source": str(tmp_path / "doesnotexist"),
            "destination": "$GLACIER_DIR/data/missing",
        }]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=True)
        assert not result.ok

    def test_data_no_glacier_dir_warns(self, tmp_path):
        data_src = tmp_path / "mydata"
        data_src.mkdir()
        spec = {**MINIMAL_SPEC, "data": [{
            "name": "mydata",
            "source": str(data_src),
            "destination": "/absolute/path",
        }]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=True)
        assert result.ok
        assert any("GLACIER_DIR" in w for w in result.warnings)

    def test_data_duplicate_name_warns(self, tmp_path):
        data_src = tmp_path / "mydata"
        data_src.mkdir()
        spec = {**MINIMAL_SPEC, "data": [
            {"name": "mydata", "source": str(data_src), "destination": "$GLACIER_DIR/data/mydata"},
            {"name": "mydata", "source": str(data_src), "destination": "$GLACIER_DIR/data/mydata2"},
        ]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=True)
        assert any("duplicate" in w for w in result.warnings)

    def test_valid_platforms(self, tmp_path):
        spec = {**MINIMAL_SPEC, "platforms": ["linux/amd64", "linux/arm64"]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok

    def test_invalid_platform(self, tmp_path):
        spec = {**MINIMAL_SPEC, "platforms": ["linux/amd64", "windows/amd64"]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok

    def test_unresolved_containers_fails(self, tmp_path):
        spec = {**MINIMAL_SPEC, "unresolved_containers": ["image:${params.unknown}"]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert not result.ok
        assert any("unresolved" in e for e in result.errors)

    def test_invalid_yaml(self, tmp_path):
        p = tmp_path / "shard.yml"
        p.write_text("{invalid: yaml: content:")
        result = validate_spec(p, check_paths=False)
        assert not result.ok

    def test_not_a_mapping(self, tmp_path):
        p = tmp_path / "shard.yml"
        p.write_text("- item1\n- item2\n")
        result = validate_spec(p, check_paths=False)
        assert not result.ok

    def test_no_check_paths_skips_source(self, tmp_path):
        spec = {**MINIMAL_SPEC, "data": [{
            "name": "missing",
            "source": "/nonexistent/path",
            "destination": "$GLACIER_DIR/data/missing",
        }]}
        p = _write_spec(tmp_path, spec)
        result = validate_spec(p, check_paths=False)
        assert result.ok
