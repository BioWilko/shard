# shard

A distribution format and tooling for packaging Nextflow workflows as single self-contained `.shard` files. A `.shard` archive bundles the workflow git repository, Docker container images (multi-arch), and runtime data dependencies so workflows can be installed and run fully offline.

Designed to work with [Glacier](https://github.com/artic-network/glacier), a Nextflow executor wrapper that consumes `.shard` archives.

## Quick start

### Install

```bash
pip install .
```

### 1. Generate a spec from an existing Nextflow repo

```bash
shard init --repo /path/to/my-workflow --data /path/to/data-dir
```

This scans the repo for container declarations, reads metadata from `nextflow.config`, and writes `shard.yml` in the current directory. Review and edit the output — container detection is heuristic.

### 2. Validate the spec

```bash
shard check shard.yml
```

Checks required fields, image reference formats, and (by default) that source paths exist on disk. Use `--no-check-paths` to skip filesystem checks.

### 3. Pack the archive

```bash
shard pack --spec shard.yml --out dist/
```

This will:
- Bundle the workflow git repository
- Pull and save Docker images for `linux/amd64` and `linux/arm64`
- Copy data directories and compute per-file SHA-256 checksums
- Write `dist/<name>-<version>.shard`

Requires Docker to be running. Multi-arch images are pulled by digest to avoid host-architecture fallback issues.

### 4. Validate the archive

```bash
shard validate dist/my-workflow-1.0.0.shard
```

Verifies every declared file is present and its SHA-256 matches the manifest.

### 5. Inspect an archive

```bash
shard inspect dist/my-workflow-1.0.0.shard
```

Prints the manifest contents: name, version, workflow ref, containers (with per-platform paths), and data entries.

## shard.yml reference

```yaml
name: my-workflow
version: 1.0.0
description: Optional human-readable description

workflow:
  path: .           # path to local git repo (default: .)
  ref: v1.0.0       # tag, branch, or commit to bundle

# Optional: restrict target platforms (default: linux/amd64 and linux/arm64)
platforms:
  - linux/amd64
  - linux/arm64

containers:
  - artic/fieldbioinformatics:1.10.0
  - biocontainers/multiqc:1.30--pyhdfd78af_1

data:
  - name: clair3-models
    source: /local/path/to/clair3-models
    destination: $GLACIER_DIR/data/clair3-models
```

## Format

The `.shard` file is a gzipped tar archive. See [docs/spec.md](docs/spec.md) for the full format specification.

## Development

```bash
pip install -e ".[dev]"
python -m pytest tests/ -v
```
