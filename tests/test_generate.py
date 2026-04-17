import subprocess
from pathlib import Path

import pytest
import yaml

from shard.generate import (
    _detect_containers,
    _has_registry,
    _is_docker_image,
    _resolve_interpolation,
    generate,
)
from shard.nfconfig import NextflowConfigParser


class TestIsDockerImage:
    def test_valid_images(self):
        valid = [
            "ubuntu:22.04",
            "artic/fieldbioinformatics:1.10.0",
            "biocontainers/multiqc:1.30--pyhdfd78af_1",
            "ghcr.io/owner/repo:latest",
            "image",
            "image:tag",
        ]
        for img in valid:
            assert _is_docker_image(img), f"expected valid: {img}"

    def test_invalid_images(self):
        invalid = [
            "https://example.com/image",
            "oras://registry/image:tag",
            "shub://container",
            "docker://image:tag",
            "",
        ]
        for img in invalid:
            assert not _is_docker_image(img), f"expected invalid: {img}"


class TestHasRegistry:
    def test_no_slash_no_registry(self):
        assert not _has_registry("ubuntu:22.04")

    def test_org_image_no_registry(self):
        assert not _has_registry("artic/fieldbioinformatics:1.10.0")

    def test_hostname_registry(self):
        assert _has_registry("ghcr.io/owner/repo:tag")

    def test_wave_registry(self):
        assert _has_registry("community.wave.seqera.io/library/tool:abc123")

    def test_quay_registry(self):
        assert _has_registry("quay.io/biocontainers/samtools:1.21")

    def test_localhost_registry(self):
        assert _has_registry("localhost:5000/myimage:tag")


class TestResolveInterpolation:
    def test_resolves_params_prefix(self):
        result, ok = _resolve_interpolation("image:${params.tag}", {"tag": "1.0.0"})
        assert ok
        assert result == "image:1.0.0"

    def test_resolves_bare_key(self):
        result, ok = _resolve_interpolation("image:${tag}", {"tag": "2.0"})
        assert ok
        assert result == "image:2.0"

    def test_unresolvable_returns_false(self):
        result, ok = _resolve_interpolation("image:${params.unknown}", {"tag": "1.0"})
        assert not ok

    def test_multiple_vars_all_resolved(self):
        result, ok = _resolve_interpolation(
            "${org}/${name}:${params.ver}",
            {"org": "myorg", "name": "myapp", "ver": "3.0"},
        )
        assert ok
        assert result == "myorg/myapp:3.0"


class TestDetectContainers:
    def test_literal_container_in_nf(self, tmp_path):
        nf = tmp_path / "main.nf"
        nf.write_text("container 'ubuntu:22.04'\n")
        parser = NextflowConfigParser()
        containers, unresolved = _detect_containers(tmp_path, parser)
        assert "ubuntu:22.04" in containers
        assert unresolved == []

    def test_literal_container_double_quoted(self, tmp_path):
        nf = tmp_path / "main.nf"
        nf.write_text('container "ubuntu:22.04"\n')
        parser = NextflowConfigParser()
        containers, unresolved = _detect_containers(tmp_path, parser)
        assert "ubuntu:22.04" in containers

    def test_interpolated_container_resolved(self, tmp_path):
        config = tmp_path / "nextflow.config"
        config.write_text(
            "params {\n    tag = '1.0.0'\n}\n"
            "process {\n    container \"image:${params.tag}\"\n}\n"
        )
        parser = NextflowConfigParser()
        containers, unresolved = _detect_containers(tmp_path, parser)
        assert "image:1.0.0" in containers

    def test_interpolated_container_unresolved(self, tmp_path):
        config = tmp_path / "nextflow.config"
        config.write_text(
            "process {\n    container \"image:${params.unknown_var}\"\n}\n"
        )
        parser = NextflowConfigParser()
        containers, unresolved = _detect_containers(tmp_path, parser)
        assert len(unresolved) == 1
        assert "${params.unknown_var}" in unresolved[0]

    def test_deduplication(self, tmp_path):
        nf1 = tmp_path / "a.nf"
        nf2 = tmp_path / "b.nf"
        nf1.write_text("container 'ubuntu:22.04'\n")
        nf2.write_text("container 'ubuntu:22.04'\n")
        parser = NextflowConfigParser()
        containers, _ = _detect_containers(tmp_path, parser)
        assert containers.count("ubuntu:22.04") == 1

    def test_skips_non_docker_schemes(self, tmp_path):
        nf = tmp_path / "main.nf"
        nf.write_text("container 'https://example.com/image'\n")
        parser = NextflowConfigParser()
        containers, unresolved = _detect_containers(tmp_path, parser)
        assert containers == []

    def test_with_name_block_in_config(self, tmp_path):
        config = tmp_path / "nextflow.config"
        config.write_text(
            "process {\n"
            "    withName: 'BWA_MEM' {\n"
            "        container = 'biocontainers/bwa:0.7.17'\n"
            "    }\n"
            "    withLabel: 'gpu' {\n"
            "        container = 'nvcr.io/nvidia/cuda:12.0'\n"
            "    }\n"
            "}\n"
        )
        parser = NextflowConfigParser()
        containers, unresolved = _detect_containers(tmp_path, parser)
        assert "biocontainers/bwa:0.7.17" in containers
        assert "nvcr.io/nvidia/cuda:12.0" in containers
        assert unresolved == []


class TestGenerate:
    def _make_git_repo(self, path: Path) -> None:
        subprocess.run(["git", "init", str(path)], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(path), "config", "user.email", "test@test.com"],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(path), "config", "user.name", "Test"],
                       check=True, capture_output=True)

    def test_metadata_from_nextflow_config(self, tmp_path):
        self._make_git_repo(tmp_path)
        config = tmp_path / "nextflow.config"
        config.write_text(
            "manifest {\n    name = 'my-wf'\n    version = '2.0.0'\n    description = 'My workflow'\n}\n"
        )
        spec, warnings = generate(tmp_path)
        assert spec["name"] == "my-wf"
        assert spec["version"] == "2.0.0"
        assert spec["description"] == "My workflow"

    def test_fallback_name_from_directory(self, tmp_path):
        self._make_git_repo(tmp_path)
        spec, warnings = generate(tmp_path)
        assert spec["name"] == tmp_path.name
        assert any("directory name" in w for w in warnings)

    def test_fallback_version(self, tmp_path):
        self._make_git_repo(tmp_path)
        spec, warnings = generate(tmp_path)
        assert spec["version"] == "0.1.0"
        assert any("version" in w for w in warnings)

    def test_workflow_key_present(self, tmp_path):
        self._make_git_repo(tmp_path)
        spec, _ = generate(tmp_path)
        assert "workflow" in spec
        assert spec["workflow"]["path"] == str(tmp_path.resolve())

    def test_workflow_path_dot_when_cwd(self, tmp_path, monkeypatch):
        self._make_git_repo(tmp_path)
        monkeypatch.chdir(tmp_path)
        spec, _ = generate(tmp_path)
        assert spec["workflow"]["path"] == "."

    def test_data_entries(self, tmp_path):
        self._make_git_repo(tmp_path)
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "models").mkdir()
        (data_dir / "primers").mkdir()
        spec, _ = generate(tmp_path, data_dir)
        assert len(spec["data"]) == 2
        names = {e["name"] for e in spec["data"]}
        assert names == {"models", "primers"}
        for entry in spec["data"]:
            assert entry["destination"].startswith("$GLACIER_DIR")

    def test_containers_detected(self, tmp_path):
        self._make_git_repo(tmp_path)
        nf = tmp_path / "main.nf"
        nf.write_text("container 'ubuntu:22.04'\n")
        spec, _ = generate(tmp_path)
        assert "ubuntu:22.04" in spec["containers"]

    def test_unresolved_containers_key(self, tmp_path):
        self._make_git_repo(tmp_path)
        config = tmp_path / "nextflow.config"
        config.write_text(
            "process {\n    container \"image:${params.unknown}\"\n}\n"
        )
        spec, warnings = generate(tmp_path)
        assert "unresolved_containers" in spec
        assert any("unresolved" in w for w in warnings)

    def test_docker_registry_applied(self, tmp_path):
        self._make_git_repo(tmp_path)
        config = tmp_path / "nextflow.config"
        config.write_text(
            "docker.registry = 'quay.io'\n"
            "process {\n"
            "    withName: 'BWA' {\n"
            "        container = 'biocontainers/bwa:0.7.17'\n"
            "    }\n"
            "}\n"
        )
        spec, _ = generate(tmp_path)
        assert "quay.io/biocontainers/bwa:0.7.17" in spec["containers"]

    def test_docker_registry_not_doubled(self, tmp_path):
        self._make_git_repo(tmp_path)
        config = tmp_path / "nextflow.config"
        config.write_text(
            "docker.registry = 'quay.io'\n"
            "process {\n"
            "    withName: 'WAVE' {\n"
            "        container = 'community.wave.seqera.io/library/tool:abc'\n"
            "    }\n"
            "}\n"
        )
        spec, _ = generate(tmp_path)
        assert "community.wave.seqera.io/library/tool:abc" in spec["containers"]
        assert not any(c.startswith("quay.io/community") for c in spec["containers"])

    def test_output_is_valid_yaml(self, tmp_path):
        self._make_git_repo(tmp_path)
        spec, _ = generate(tmp_path)
        dumped = yaml.dump(spec, sort_keys=False)
        reloaded = yaml.safe_load(dumped)
        assert reloaded["name"] == spec["name"]
