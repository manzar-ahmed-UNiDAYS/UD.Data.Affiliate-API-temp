"""Bundled dbt project assets for the affiliate API pipeline."""

from __future__ import annotations

from pathlib import Path


def get_project_dir() -> Path:
    """Return the dbt project root for source checkouts and installed wheels."""
    package_dir = Path(__file__).resolve().parent
    if (package_dir / "dbt_project.yml").is_file():
        return package_dir

    repo_root = package_dir.parent
    if (repo_root / "dbt_project.yml").is_file():
        return repo_root

    return package_dir


__all__ = ["get_project_dir"]
