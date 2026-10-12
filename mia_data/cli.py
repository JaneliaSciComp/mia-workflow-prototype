"""mia-data command line. Only `seed` exists so far; a later task fills in the rest."""

import argparse
import asyncio
import sys

from autodev.auth import token_fn
from autodev.github import GitHub

from .config import load_config
from .seed import seed


async def cmd_seed(cfg, args) -> int:
    tokens, g = token_fn(cfg), cfg.github
    gh_data, gh_catalog = GitHub(g.data_repo, tokens, api_url=g.api_url), GitHub(g.catalog_repo, tokens, api_url=g.api_url)
    try:
        made = await seed(cfg, gh_data, gh_catalog, args.dry_run)
    finally:
        await gh_data.aclose()
        await gh_catalog.aclose()
    print(f"{'would create' if args.dry_run else 'created'} {len(made)} issue(s)")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(prog="mia-data")
    sp = ap.add_subparsers(dest="cmd", required=True).add_parser("seed", help="create one issue per downloaded dataset")
    sp.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    sys.exit(asyncio.run(cmd_seed(load_config(), args)))
