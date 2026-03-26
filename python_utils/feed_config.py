"""Load and resolve affiliate feed configuration for the dbt pipeline.

This module is the main adapter between ``affiliate_config.yml`` and the
runtime code used by the dbt models. It reads the static YAML configuration,
merges pipeline-level defaults with feed-level overrides, and exposes the
result as normalized ``FeedSpec`` objects and plain dictionaries.

The helpers in this module are used to:

- list all enabled network/feed combinations
- resolve one selected feed from a unique ``affiliate_network_name``
- normalize names so matching is stable across minor formatting differences
- surface the credential environment variables required by the configured feeds

The returned config payload includes request templates, pagination settings,
response paths, credential variable mappings, and child JSON extraction rules
that downstream code in ``step1_http`` and related utilities relies on.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from python_utils.settings import get_settings


def _slugify_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


@dataclass(frozen=True)
class FeedSpec:
    """Static description of one configured affiliate feed pipeline."""

    network_id: int
    network_name: str
    feed_id: str
    feed_name: str
    publisher_id: str | None
    child_json_items: tuple[str, ...]
    config: dict[str, Any]


def _config_path(config_path: str | Path | None = None) -> Path:
    if config_path is None:
        return get_settings().affiliate_config_path
    return Path(str(config_path)).expanduser().resolve()


def _load_affiliate_meta(config_path: str | Path | None = None) -> dict[str, Any]:
    raw = yaml.safe_load(_config_path(config_path).read_text(encoding="utf-8")) or {}
    if "affiliate_defaults" in raw or "affiliate_pipelines" in raw:
        return dict(raw)

    for model in raw.get("models") or []:
        if model.get("name") == "step1_http":
            return dict((model.get("config") or {}).get("meta") or {})

    raise ValueError("Could not find affiliate config.")


def _merged_feed_config(
    pipeline: dict[str, Any],
    feed: dict[str, Any],
    defaults: dict[str, Any],
) -> dict[str, Any]:
    request = dict(feed.get("request") or {})
    pagination = dict(feed.get("pagination") or {})
    response = dict(feed.get("response") or {})
    credential_variables = dict(pipeline.get("credentials") or {})
    credential_variables.update(feed.get("credentials") or {})

    return {
        "id": f"{pipeline['network_id']}:{feed['feed_id']}",
        "feed_id": feed["feed_id"],
        "feed_name": feed.get("feed_name", feed["feed_id"]),
        "network_id": int(pipeline["network_id"]),
        "network_name": str(pipeline["network_name"]),
        "account_name": pipeline.get("account_name"),
        "publisher_id": feed.get("program_id"),
        "program_id": feed.get("program_id"),
        "credential_variables": credential_variables,
        "child_json_items": list(feed.get("child_json_items") or []),
        "api_method": (request.get("method") or "GET").upper(),
        "api_url_template": request.get("url_template"),
        "query_params_template": request.get("query_params_template", ""),
        "api_body_template": request.get("body_template", ""),
        "api_headers_template": request.get("headers_template", "{}"),
        "api_max_retry": int(feed.get("api_max_retry", pipeline.get("api_max_retry", defaults.get("api_max_retry", 5)))),
        "api_retry_delay": int(feed.get("api_retry_delay", pipeline.get("api_retry_delay", defaults.get("api_retry_delay", 60)))),
        "api_timeout": int(feed.get("api_timeout", pipeline.get("api_timeout", defaults.get("api_timeout", get_settings().request_timeout)))),
        "records_path": response.get("records_path", "$"),
        "pagination_mode": pagination.get("mode", "none"),
        "next_url_path": pagination.get("next_url_path"),
        "next_url_relative": bool(pagination.get("next_url_relative", False)),
        "current_page_path": pagination.get("current_page_path"),
        "total_pages_path": pagination.get("total_pages_path"),
        "page_param_name": pagination.get("page_param_name", "page"),
        "offset_path": pagination.get("offset_path"),
        "limit_path": pagination.get("limit_path"),
        "offset_param_name": pagination.get("offset_param_name", "offset"),
        "limit_param_name": pagination.get("limit_param_name", "limit"),
        "cursor_path": pagination.get("cursor_path"),
        "cursor_complete_path": pagination.get("cursor_complete_path"),
    }


def list_feed_specs(config_path: str | Path | None = None) -> list[FeedSpec]:
    """Return all enabled feed definitions from the static affiliate config."""

    meta = _load_affiliate_meta(config_path=config_path)
    defaults = dict(meta.get("affiliate_defaults") or {})
    specs: list[FeedSpec] = []

    for pipeline in meta.get("affiliate_pipelines") or []:
        if not pipeline.get("enabled", True):
            continue
        for feed in pipeline.get("feeds") or []:
            if not feed.get("enabled", True):
                continue
            merged_config = _merged_feed_config(pipeline, feed, defaults)
            specs.append(
                FeedSpec(
                    network_id=int(pipeline["network_id"]),
                    network_name=str(pipeline["network_name"]),
                    feed_id=str(feed["feed_id"]),
                    feed_name=str(feed.get("feed_name", feed["feed_id"])),
                    publisher_id=feed.get("program_id"),
                    child_json_items=tuple(merged_config.get("child_json_items") or []),
                    config=merged_config,
                )
            )
    return specs


def get_feed_spec(
    network_name: str,
    feed_id: str,
    config_path: str | Path | None = None,
) -> FeedSpec:
    """Return the matching feed spec by static network and feed names."""

    normalized_network_name = _slugify_name(network_name)
    normalized_feed_id = _slugify_name(feed_id)

    for spec in list_feed_specs(config_path=config_path):
        if (
            _slugify_name(spec.network_name) == normalized_network_name
            and _slugify_name(spec.feed_id) == normalized_feed_id
        ):
            return spec

    raise ValueError(f"Unknown feed spec for network_name={network_name!r}, feed_id={feed_id!r}.")


def resolve_feed_spec(
    network_name: str | None = None,
    feed_id: str | None = None,
    config_path: str | Path | None = None,
) -> FeedSpec:
    """Resolve the selected feed from the static affiliate config."""

    candidates = list_feed_specs(config_path=config_path)

    if network_name:
        normalized_network_name = _slugify_name(network_name)
        candidates = [
            spec
            for spec in candidates
            if _slugify_name(spec.network_name) == normalized_network_name
        ]

    if feed_id:
        normalized_feed_id = _slugify_name(feed_id)
        candidates = [
            spec
            for spec in candidates
            if _slugify_name(spec.feed_id) == normalized_feed_id
        ]

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise ValueError("No feed configuration matched the selected affiliate variables.")

    raise ValueError(
        "Multiple feed configurations matched. affiliate_network_name must identify a single configured feed."
    )


def list_required_credential_variables(config_path: str | Path | None = None) -> list[str]:
    """Return the unique environment variable names referenced by feed credentials."""

    required = {
        env_var_name
        for spec in list_feed_specs(config_path=config_path)
        for env_var_name in (spec.config.get("credential_variables") or {}).values()
        if env_var_name
    }
    return sorted(required)
