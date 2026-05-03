"""
offer_intel.cli
~~~~~~~~~~~~~~~
Command-line interface for the offer_intel package.

Commands
--------
run         Start the full pipeline (default: continuous scheduling)
run --once  Scrape every platform once and exit
normalise   Normalise all pending offers (post-extraction step)
"""

from __future__ import annotations

import argparse
import logging
import sys


def _cmd_run(args: argparse.Namespace) -> None:
    from offer_intel.pipeline import PipelineScheduler
    scheduler = PipelineScheduler()
    if args.once:
        scheduler.run_once()
    else:
        scheduler.start(run_immediately=True)


def _cmd_normalise(args: argparse.Namespace) -> None:
    from offer_intel.normalization.offer_normalizer import OfferNormalizer
    normalizer = OfferNormalizer()
    if args.platform:
        results = normalizer.normalise_by_platform(args.platform, limit=args.limit)
    else:
        results = normalizer.normalise_all_pending(limit=args.limit)

    print(f"\n✅  Normalised : {results.get('normalized', results.get('normalised', 0))}")
    print(f"❌  Failed     : {results['failed']}")
    print(f"📊  Total      : {results.get('total', 'n/a')}")
    sys.exit(1 if results["failed"] else 0)


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        prog="offer-intel",
        description="Offer Intel — social media offer extraction pipeline",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── run ──────────────────────────────────────────────────────────────
    run_parser = sub.add_parser("run", help="Start the full scrape → extract → publish pipeline")
    run_parser.add_argument(
        "--once",
        action="store_true",
        help="Scrape all platforms once and exit (no scheduler loop)",
    )
    run_parser.set_defaults(func=_cmd_run)

    # ── normalise ────────────────────────────────────────────────────────
    norm_parser = sub.add_parser("normalise", help="Normalise pending extracted offers")
    norm_parser.add_argument("--platform", default=None, help="Limit to one platform")
    norm_parser.add_argument("--limit", type=int, default=None, help="Max offers to process")
    norm_parser.set_defaults(func=_cmd_normalise)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
