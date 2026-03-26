"""Resolve environment-backed runtime settings for the affiliate project.

This module centralizes process-level configuration used by the dbt affiliate
pipeline. It determines the project root, resolves the location of
``affiliate_config.yml``, and exposes request timeout defaults from the
environment. The cached ``Settings`` object provides a stable view of those
values during a single process run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the dbt-driven affiliate pipeline."""

    project_root: Path
    affiliate_config_path: Path
    request_timeout: int


def get_project_root() -> Path:
    """Resolve the project root from dbt env when available, else from the package location."""
    raw_root = os.getenv("DBT_PROJECT_DIR")
    if raw_root:
        return Path(raw_root).expanduser().resolve()
    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached settings object for the current process."""
    project_root = get_project_root()
    return Settings(
        project_root=project_root,
        affiliate_config_path=Path(
            os.getenv(
                "AFFILIATE_CONFIG_PATH",
                str(project_root / "affiliate_config.yml"),
            )
        ),
        request_timeout=int(os.getenv("REQUEST_TIMEOUT", 600)),
    )
