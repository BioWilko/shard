"""shard CLI — pack, validate, inspect, init, and check."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def cmd_pack(args: argparse.Namespace) -> int:
    from .pack import PackError, pack

    spec = Path(args.spec)
    out = Path(args.out)
    if not spec.exists():
        print(f"error: spec file not found: {spec}", file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)
    try:
        archive = pack(spec, out)
    except PackError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"packed: {archive}")
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    from .validate import validate

    archive = Path(args.archive)
    if not archive.exists():
        print(f"error: archive not found: {archive}", file=sys.stderr)
        return 1
    result = validate(archive)
    if result.warnings:
        for w in result.warnings:
            print(f"warning: {w}")
    if result.ok:
        print(f"ok: {archive.name}")
        return 0
    for err in result.errors:
        print(f"error: {err}", file=sys.stderr)
    return 1


def cmd_inspect(args: argparse.Namespace) -> int:
    import json
    import tarfile

    from .manifest import Manifest, ManifestError

    archive = Path(args.archive)
    if not archive.exists():
        print(f"error: archive not found: {archive}", file=sys.stderr)
        return 1
    try:
        text = None
        with tarfile.open(archive, "r|gz") as tf:
            for member in tf:
                if member.name == "manifest.json":
                    fh = tf.extractfile(member)
                    if fh is None:
                        print("error: manifest.json is empty", file=sys.stderr)
                        return 1
                    text = fh.read().decode()
                    break
        if text is None:
            print("error: manifest.json not found in archive", file=sys.stderr)
            return 1
    except tarfile.TarError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        manifest = Manifest.from_json(text)
    except ManifestError as exc:
        print(f"error: invalid manifest: {exc}", file=sys.stderr)
        return 1

    print(f"name:        {manifest.name}")
    print(f"version:     {manifest.version}")
    print(f"created:     {manifest.created_at}")
    if manifest.description:
        print(f"description: {manifest.description}")
    print(f"spec:        v{manifest.shard_spec_version}")
    print()
    print("workflow:")
    print(f"  path:   {manifest.workflow.path}")
    print(f"  commit: {manifest.workflow.git_commit}")
    print(f"  ref:    {manifest.workflow.git_ref}")
    if manifest.containers:
        print()
        print("containers:")
        for c in manifest.containers:
            print(f"  {c.image}")
            for platform, pe in c.platforms.items():
                print(f"    [{platform}]")
                print(f"      path:   {pe.path}")
                print(f"      sha256: {pe.sha256}")
    if manifest.data:
        print()
        print("data:")
        for d in manifest.data:
            print(f"  {d.name}")
            print(f"    path:        {d.path}")
            print(f"    destination: {d.destination}")
            print(f"    files:       {len(d.files)}")
    return 0


def cmd_init(args: argparse.Namespace) -> int:
    import yaml
    from .generate import generate

    repo_path = Path(args.repo).resolve()
    data_path = Path(args.data).resolve() if args.data else None
    out_path = Path(args.out)

    if not repo_path.exists():
        print(f"error: repo path does not exist: {repo_path}", file=sys.stderr)
        return 1
    if data_path and not data_path.exists():
        print(f"error: data path does not exist: {data_path}", file=sys.stderr)
        return 1
    if out_path.exists() and not args.force:
        print(f"error: {out_path} already exists (use --force to overwrite)", file=sys.stderr)
        return 1

    spec, warnings = generate(repo_path, data_path)

    out_path.write_text(yaml.dump(spec, sort_keys=False, default_flow_style=False))

    if spec.get("containers"):
        print(f"detected containers ({len(spec['containers'])}):")
        for c in spec["containers"]:
            print(f"  {c}")
    if spec.get("unresolved_containers"):
        print(f"unresolved containers ({len(spec['unresolved_containers'])}):")
        for c in spec["unresolved_containers"]:
            print(f"  {c}")
    for w in warnings:
        print(f"warning: {w}")
    print(f"wrote: {out_path}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    from .shardspec import validate_spec

    spec_path = Path(args.file)
    if not spec_path.exists():
        print(f"error: file not found: {spec_path}", file=sys.stderr)
        return 1

    result = validate_spec(spec_path, check_paths=not args.no_check_paths)

    for w in result.warnings:
        print(f"warning: {w}")
    if result.ok:
        print(f"ok: {spec_path.name}")
        return 0
    for err in result.errors:
        print(f"error: {err}", file=sys.stderr)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shard",
        description="Nextflow workflow distribution format tooling",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_pack = sub.add_parser("pack", help="Build a .shard archive from a shard.yml spec")
    p_pack.add_argument("--spec", default="shard.yml", help="Path to shard.yml (default: shard.yml)")
    p_pack.add_argument("--out", default=".", help="Output directory (default: .)")
    p_pack.set_defaults(func=cmd_pack)

    p_val = sub.add_parser("validate", help="Verify integrity of a .shard archive")
    p_val.add_argument("archive", help="Path to .shard file")
    p_val.set_defaults(func=cmd_validate)

    p_ins = sub.add_parser("inspect", help="Print manifest contents of a .shard archive")
    p_ins.add_argument("archive", help="Path to .shard file")
    p_ins.set_defaults(func=cmd_inspect)

    p_init = sub.add_parser("init", help="Generate a shard.yml from a Nextflow repo")
    p_init.add_argument("--repo", default=".", help="Path to Nextflow repo (default: .)")
    p_init.add_argument("--data", default=None, help="Path to data directory")
    p_init.add_argument("--out", default="shard.yml", help="Output file (default: shard.yml)")
    p_init.add_argument("--force", action="store_true", help="Overwrite existing output file")
    p_init.set_defaults(func=cmd_init)

    p_check = sub.add_parser("check", help="Validate a shard.yml spec file")
    p_check.add_argument("file", nargs="?", default="shard.yml", help="Path to shard.yml (default: shard.yml)")
    p_check.add_argument("--no-check-paths", action="store_true", help="Skip path existence checks")
    p_check.set_defaults(func=cmd_check)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    sys.exit(args.func(args))
