"""The `mia-buffers` command line: `plan` (offline planner), `serve` (buffer-api) and `register` (plan → buffer)."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from .plan import build_plan, load_records
from .spec import SpecError, load_spec, spec_hash


def catalog_head(catalog_dir: Path) -> str | None:
    """The checkout's HEAD commit, or None when the directory is not a git checkout."""
    result = subprocess.run(["git", "-C", str(catalog_dir), "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else None


def _plan(args: argparse.Namespace) -> int:
    try:
        spec = load_spec(args.spec)
    except SpecError as error:
        print(f"spec refused: {error}", file=sys.stderr)
        return 1
    head = catalog_head(args.catalog_dir)
    if head != spec.catalog.ref and not args.allow_ref_mismatch:
        print(
            f"catalog checkout is at {head or 'no commit'} but the spec pins {spec.catalog.ref}; "
            "check out that commit or pass --allow-ref-mismatch",
            file=sys.stderr,
        )
        return 1
    plan = build_plan(
        spec, spec_hash(args.spec), head if head == spec.catalog.ref else None, load_records(args.catalog_dir)
    )
    if args.json:
        args.json.write_text(json.dumps(plan.to_dict(), indent=1))
    print(plan.render_markdown(), end="")
    return 0 if plan.valid else 2


def _serve(args: argparse.Namespace) -> int:
    from .api import serve

    serve(args.state_dir.expanduser(), args.host, args.port)
    return 0


def _register(args: argparse.Namespace) -> int:
    from .register import register

    try:
        spec = load_spec(args.spec)
    except SpecError as error:
        print(f"spec refused: {error}", file=sys.stderr)
        return 1
    out = register(
        args.url,
        args.buffer,
        json.loads(args.plan.read_text()),
        spec,
        args.staged_root,
        tensorizer=args.tensorizer,
        split_sizes={"val": args.val_size, "test": args.test_size},
    )
    print(json.dumps(out, indent=1))
    return 0 if out["registered"] else 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mia-buffers")
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("plan", help="turn a buffer spec and a catalog checkout into a plan (markdown on stdout)")
    plan.add_argument("spec", type=Path)
    plan.add_argument(
        "--catalog-dir", type=Path, required=True, help="a checkout of the catalog repo at the spec's ref"
    )
    plan.add_argument("--json", type=Path, help="also write plan.json here")
    plan.add_argument(
        "--allow-ref-mismatch", action="store_true", help="plan even when the checkout is not at catalog.ref"
    )
    plan.set_defaults(func=_plan)
    serve = sub.add_parser("serve", help="run buffer-api, the sampler and ledger for buffers (spec §5)")
    serve.add_argument("--state-dir", type=Path, default=Path("~/.local/state/mia-buffers"))
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    serve.set_defaults(func=_serve)
    reg = sub.add_parser("register", help="register a plan's staged volumes with a running buffer-api")
    reg.add_argument("plan", type=Path, help="plan.json from `mia-buffers plan --json`")
    reg.add_argument("spec", type=Path)
    reg.add_argument("--staged-root", type=Path, required=True, help="<root>/<record id>/stage.json per record")
    reg.add_argument("--url", required=True, help="buffer-api base URL")
    reg.add_argument("--buffer", required=True, help="buffer id")
    reg.add_argument("--tensorizer", default="muvit-2d")
    reg.add_argument("--val-size", type=int, default=256, help="frozen val list length")
    reg.add_argument("--test-size", type=int, default=256, help="frozen test list length")
    reg.set_defaults(func=_register)
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
