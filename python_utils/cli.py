"""Inspect affiliate dbt runtime configuration.

This module provides a small command-line interface for viewing the static
affiliate feed configuration resolved by ``python_utils.feed_config``. It
prints the selected feed specs as formatted JSON and supports filtering by
``network_name``.

Usage:
    python -m python_utils.cli
    python -m python_utils.cli --network-name awin_transactions

If the project is installed as a package, the same interface is exposed via
the ``affiliate-api`` console script.
"""

from __future__ import annotations

import argparse
import json

from python_utils.feed_config import list_feed_specs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect the affiliate dbt pipeline configuration."
    )
    parser.add_argument("--network-name", required=False, help="Filter output to one network name.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    payload = [
        spec.config
        for spec in list_feed_specs()
        if not args.network_name or spec.network_name == args.network_name
    ]
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
