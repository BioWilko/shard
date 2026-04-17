# shard Archive Format Specification

Version: 1  
Status: Draft

## Overview

A `.shard` file is a gzipped tar archive that bundles a Nextflow workflow, its Docker container images, and any required runtime data into a single distributable file. It is designed to be fully self-contained so that a workflow can be installed and run offline.

## File Naming

```
<name>-<version>.shard
```

- `name`: the workflow name (alphanumeric, hyphens, underscores)
- `version`: a semantic version string (e.g. `1.0.0`)

The file is a valid gzip-compressed tar archive. The `.shard` extension is a rename convention only.

## Archive Layout

```
manifest.json
workflow/
    <name>.bundle
containers/
    <image-safe-name>-amd64.tar
    <image-safe-name>-arm64.tar
    ...
data/
    <data-name>/
        ...
```

All paths inside the archive are relative (no leading `/`).

## manifest.json

`manifest.json` must be present at the root of the archive. It is a UTF-8 encoded JSON object with the following structure:

### Top-level fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `shard_spec_version` | integer | yes | Format version. Currently `1`. |
| `name` | string | yes | Workflow name. |
| `version` | string | yes | Workflow version. |
| `created_at` | string | yes | ISO 8601 UTC timestamp of archive creation. |
| `description` | string | no | Human-readable description. |
| `workflow` | object | yes | See below. |
| `containers` | array | no | List of container entries. May be empty. |
| `data` | array | no | List of data entries. May be empty. |

### `workflow` object

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | yes | Archive-relative path to the git bundle. |
| `sha256` | string | yes | Lowercase hex SHA-256 of the bundle file. |
| `git_commit` | string | yes | Full 40-character git commit SHA. |
| `git_ref` | string | yes | Tag, branch, or `HEAD` used when bundling. |

### `containers[]` objects

Each container entry records one or more platform-specific image tarballs:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `image` | string | yes | Full Docker image reference (e.g. `artic/fieldbioinformatics:1.10.0`). |
| `platforms` | object | yes | Map from platform key to platform entry (see below). |

Platform keys are of the form `linux/amd64` or `linux/arm64`.

#### `containers[].platforms[<platform>]` objects

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | yes | Archive-relative path to the Docker image tar for this platform. |
| `sha256` | string | yes | Lowercase hex SHA-256 of the image tar. |

### `data[]` objects

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | yes | Human-readable data entry name. |
| `path` | string | yes | Archive-relative path to the data directory. |
| `destination` | string | yes | Install-time destination path. May contain `$GLACIER_DIR`. |
| `files` | array | yes | Per-file integrity records (see below). |

### `data[].files[]` objects

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | yes | Path relative to the data entry's `path`. |
| `sha256` | string | yes | Lowercase hex SHA-256 of the file. |

### Example manifest.json

```json
{
  "shard_spec_version": 1,
  "name": "amplicon-nf",
  "version": "1.0.0",
  "description": "ARTIC amplicon sequencing pipeline",
  "created_at": "2026-04-17T12:00:00+00:00",
  "workflow": {
    "path": "workflow/amplicon-nf.bundle",
    "sha256": "a3f2...",
    "git_commit": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    "git_ref": "v1.0.0"
  },
  "containers": [
    {
      "image": "artic/fieldbioinformatics:1.10.0",
      "platforms": {
        "linux/amd64": {
          "path": "containers/artic-fieldbioinformatics-1.10.0-amd64.tar",
          "sha256": "b1c2..."
        },
        "linux/arm64": {
          "path": "containers/artic-fieldbioinformatics-1.10.0-arm64.tar",
          "sha256": "d3e4..."
        }
      }
    }
  ],
  "data": [
    {
      "name": "clair3-models",
      "path": "data/clair3-models",
      "destination": "$GLACIER_DIR/data/clair3-models",
      "files": [
        {
          "path": "r941_min_hac_g507/config.json",
          "sha256": "c3d4..."
        }
      ]
    }
  ]
}
```

## Integrity Model

- All SHA-256 values are lowercase hexadecimal strings (64 characters).
- The git bundle is hashed as a single file.
- Each Docker image tar is hashed as a single file per platform.
- Data entries are verified per-file, so corruption can be localised to individual files within a directory.
- No hash is stored for `manifest.json` itself; it is the root of trust.

## Platform Resolution

When installing a `.shard` archive, the installer selects container tarballs as follows:

1. Prefer an exact platform match (e.g. `linux/arm64` on an arm64 host).
2. On `linux/arm64` hosts with no arm64 variant, fall back to `linux/amd64` (emulation via Rosetta 2 / QEMU is available on all common arm64 platforms).
3. On `linux/amd64` hosts with no amd64 variant, fail with an informative error.

A shard containing only `linux/amd64` variants is valid and usable on all common targets.

## Workflow Source Spec: shard.yml

To build a `.shard` archive, the workflow author provides a `shard.yml` file. This file is **not** included in the archive; it is only consumed at pack time.

```yaml
name: amplicon-nf
version: 1.0.0
description: ARTIC amplicon sequencing pipeline

workflow:
  path: .            # path to local git repository
  ref: v1.0.0        # tag, branch, or commit to bundle

# Optional: override default target platforms (default: linux/amd64 and linux/arm64)
platforms:
  - linux/amd64
  - linux/arm64

containers:
  - artic/fieldbioinformatics:1.10.0
  - biocontainers/multiqc:1.30--pyhdfd78af_1

data:
  - name: clair3-models
    source: /path/to/clair3-models   # local directory to include
    destination: $GLACIER_DIR/data/clair3-models
```

## Container Image Filename Convention

Docker image references are normalised to filesystem-safe filenames by replacing `/`, `:`, and `@` with `-` and appending the architecture suffix:

```
artic/fieldbioinformatics:1.10.0  →  artic-fieldbioinformatics-1.10.0-amd64.tar
                                     artic-fieldbioinformatics-1.10.0-arm64.tar
```

## Validation

A conforming validator must:

1. Confirm the file can be opened as a gzip-compressed tar archive.
2. Confirm `manifest.json` exists at the root.
3. Parse `manifest.json` and validate all required fields are present and of the correct type.
4. Confirm `shard_spec_version` is a positive integer no greater than the validator's supported version.
5. For the workflow: confirm the member exists in the archive and its SHA-256 matches.
6. For each container: for each platform entry in `platforms`, confirm the member exists and its SHA-256 matches.
7. For each data entry: for each file in `files`, confirm the member exists and its SHA-256 matches.
8. Report per-component pass/fail and exit non-zero if any check fails.

## Versioning

The `shard_spec_version` integer increments when the manifest schema changes in a backwards-incompatible way. Validators should reject archives with a `shard_spec_version` greater than their supported version.
