"""Provide shared date and window helpers for the dbt extraction models.

This module contains the small time-related utilities used by ``step1_http``
and related runtime code. It normalizes user-supplied dates to UTC midnight,
parses ``YYYY-MM-DD`` strings from dbt vars, and constructs the inclusive
lookback window used for affiliate API extraction runs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class Window:
    """Inclusive pipeline window."""

    start_date: datetime
    end_date: datetime


def parse_date_string(date_str: str) -> datetime:
    """Parse a YYYY-MM-DD string into a UTC datetime."""
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def ensure_utc_midnight(value: datetime | None) -> datetime | None:
    """Normalise a datetime into UTC midnight."""
    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def resolve_window(lookback_days: int = 7, end_date: datetime | None = None) -> Window:
    """Return the default rolling ingestion window."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be a positive integer")

    resolved_end_date = ensure_utc_midnight(end_date)
    if resolved_end_date is None:
        resolved_end_date = datetime.now(timezone.utc).replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    return Window(
        start_date=resolved_end_date - timedelta(days=lookback_days),
        end_date=resolved_end_date,
    )
