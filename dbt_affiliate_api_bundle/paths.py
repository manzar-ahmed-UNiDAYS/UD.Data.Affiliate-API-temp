"""Helpers for locating the installed dbt affiliate package paths."""

from __future__ import annotations

import argparse
import json

from dbt_affiliate_api_bundle import get_project_dir


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Print the installed dbt_affiliate_api_bundle project paths."
    )
    parser.add_argument(
        "--format",
        choices=("json", "shell"),
        default="json",
        help="Output format.",
    )
    return parser


def main() -> None:
    project_dir = get_project_dir()
    payload = {
        "project_dir": str(project_dir),
        "profiles_dir": str(project_dir),
        "module_path": str(project_dir),
    }

    args = build_parser().parse_args()
    if args.format == "shell":
        print(f"DBT_AFFILIATE_PROJECT_DIR={payload['project_dir']}")
        print(f"DBT_AFFILIATE_PROFILES_DIR={payload['profiles_dir']}")
        print(f"DBT_AFFILIATE_MODULE_PATH={payload['module_path']}")
        return

    print(json.dumps(payload, indent=2))
