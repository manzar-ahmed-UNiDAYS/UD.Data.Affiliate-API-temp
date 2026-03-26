"""Provide the generic HTTP runtime used by the affiliate dbt pipeline.

This module contains the config-driven request handler used by ``step1_http``
to call each affiliate network API. It is responsible for rendering request
templates, injecting environment-backed credentials, sending HTTP requests,
handling pagination, normalizing response records, and capturing sanitized
request/response audit metadata for downstream logging and header tables.

The handler is intentionally network-agnostic. Feed-specific behavior comes
from the merged configuration loaded from ``affiliate_config.yml`` rather than
from hard-coded per-network classes.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional
from urllib.parse import parse_qs, urljoin

import requests
from jinja2 import BaseLoader, Environment

from python_utils.settings import get_settings

logger = logging.getLogger(__name__)

SENSITIVE_FIELD_TOKENS = (
    "authorization",
    "token",
    "secret",
    "password",
    "cookie",
    "api_key",
    "apikey",
    "x-api-key",
)

RETRYABLE_STATUS_CODES = {429}
NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404}
SERVER_ERROR_STATUS_CODES = {500, 502, 503, 504}


@dataclass
class PaginationToken:
    next_url: Optional[str] = None
    params: Optional[Dict[str, Any]] = None
    cursor: Optional[str] = None


class BaseNetworkHandler:
    """Generic request/pagination/serialization handler driven by model metadata."""

    def __init__(self, config=None):
        self.config = config or {}
        self.request_sender = None
        self.request_history: list[dict[str, Any]] = []
        self._last_request_audit = None

        self._base_url = None
        self._base_headers = None
        self._base_params = None
        self._base_body = None
        self._base_http_method = None
        self._start_date = None
        self._end_date = None
        self._base_request_context: dict[str, Any] = {}

    def _sanitize_mapping(self, values: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        sanitized = {}
        for key, value in (values or {}).items():
            key_str = str(key)
            key_lower = key_str.lower()
            if any(token in key_lower for token in SENSITIVE_FIELD_TOKENS):
                sanitized[key_str] = "[REDACTED]"
            else:
                sanitized[key_str] = value
        return sanitized

    def _capture_request_audit(
        self,
        method,
        url,
        headers,
        params,
        body,
        response,
        request_sent_at: str,
        response_received_at: str,
    ) -> None:
        self._last_request_audit = {
            "request_sent_at": request_sent_at,
            "response_received_at": response_received_at,
            "request": {
                "method": method.upper(),
                "url": url,
                "headers": self._sanitize_mapping(headers),
                "params": self._sanitize_mapping(params),
                "body_present": body is not None,
            },
            "response": {
                "status_code": getattr(response, "status_code", None),
                "headers": self._sanitize_mapping(dict(getattr(response, "headers", {}))),
            },
        }

    def _store_last_request_audit(self) -> None:
        if self._last_request_audit is not None:
            self.request_history.append(self._last_request_audit)
            self._last_request_audit = None

    def _extract_path(self, payload: Any, path: str | None, default: Any = None) -> Any:
        if path in (None, "", "$"):
            return payload

        current = payload
        for segment in str(path).split("."):
            if current is None:
                return default
            if isinstance(current, list):
                try:
                    current = current[int(segment)]
                except (ValueError, IndexError):
                    return default
                continue
            if isinstance(current, dict):
                current = current.get(segment)
                continue
            return default
        return default if current is None else current

    def _records_from_payload(self, payload: Any) -> list[Any]:
        records = self._extract_path(payload, (self.config or {}).get("records_path"), default=[])
        if records is None:
            return []
        if isinstance(records, list):
            return records
        if isinstance(records, dict):
            return [records]
        return [records]

    def _normalize_record(self, record: Any) -> Any:
        if not isinstance(record, dict):
            return record
        return dict(record)

    def _resolve_credentials(self, config: dict[str, Any]) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for field_name, env_var_name in (config.get("credential_variables") or {}).items():
            if not env_var_name:
                continue
            value = os.getenv(str(env_var_name))
            if value in (None, ""):
                raise ValueError(
                    f"Missing required environment variable {env_var_name} for credential field {field_name}."
                )
            resolved[field_name] = value
        return resolved

    def _build_base_request_context(self, config, start_date, end_date) -> dict[str, Any]:
        return {
            "start_date": start_date,
            "end_date": end_date,
            "start_date_str": start_date.strftime("%Y-%m-%d"),
            "end_date_str": end_date.strftime("%Y-%m-%d"),
            "start_datetime_utc": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_datetime_utc": end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
            **config,
            **self._resolve_credentials(config),
        }

    def fetch(self, config: Dict[str, Any], start_date, end_date) -> Iterator[Dict[str, Any] | list[Any]]:
        self.config = config
        self._start_date = start_date
        self._end_date = end_date
        self._base_request_context = self._build_base_request_context(config, start_date, end_date)
        self.request_history = []

        response_json = self._fetch_initial_page(config, start_date, end_date)
        yield response_json

        token = self.extract_pagination_token(response_json)
        while token:
            time.sleep(2)
            page_json = self._fetch_next_page(config, token)
            yield page_json
            token = self.extract_pagination_token(page_json)

    def _send_request(self, method, url, headers=None, params=None, body=None):
        request_sent_at = datetime.now(timezone.utc).isoformat()

        if self.request_sender is not None:
            response = self.request_sender(
                method=method,
                url=url,
                headers=headers,
                params=params,
                body=body,
            )
            response_received_at = datetime.now(timezone.utc).isoformat()
            self._capture_request_audit(
                method,
                url,
                headers,
                params,
                body,
                response,
                request_sent_at=request_sent_at,
                response_received_at=response_received_at,
            )
            return response

        timeout = int((self.config or {}).get("api_timeout", get_settings().request_timeout))
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            data=body,
            timeout=timeout,
        )
        response_received_at = datetime.now(timezone.utc).isoformat()
        self._capture_request_audit(
            method,
            url,
            headers,
            params,
            body,
            response,
            request_sent_at=request_sent_at,
            response_received_at=response_received_at,
        )
        return response

    def _response_to_json(self, response):
        try:
            return response.json()
        except Exception as exc:
            raise ValueError(f"Failed to decode JSON response: {exc}") from exc

    def _response_error_message(self, response) -> str:
        try:
            resp_json = self._response_to_json(response)
            if isinstance(resp_json, dict):
                return str(resp_json.get("message") or resp_json.get("errors") or resp_json)
            return str(resp_json)
        except Exception:
            return getattr(response, "text", "")[:500]

    def _request_json_with_retry(
        self,
        *,
        method,
        url,
        headers=None,
        params=None,
        body=None,
        config: dict[str, Any] | None = None,
        request_label: str,
    ):
        request_config = config or self.config or {}
        network_name = request_config.get("account_name", request_config.get("network_name", "unknown"))
        max_retry = int(request_config.get("api_max_retry", 3))
        retry_delay = int(request_config.get("api_retry_delay", 60))

        logger.info("Max retries: %s, Retry delay: %ss", max_retry, retry_delay)
        for attempt in range(1, max_retry + 1):
            try:
                logger.info("Attempt %s for URL: %s", attempt, url)
                resp = self._send_request(
                    method=method,
                    url=url,
                    headers=headers,
                    params=params,
                    body=body,
                )
                api_error_message = self._response_error_message(resp)

                if resp.status_code == 200:
                    self._store_last_request_audit()
                    return self._response_to_json(resp)

                if resp.status_code in RETRYABLE_STATUS_CODES:
                    logger.warning(
                        "Attempt %s/%s failed for %s during %s -> HTTP %s: %s. %s",
                        attempt,
                        max_retry,
                        network_name,
                        request_label,
                        resp.status_code,
                        api_error_message,
                        "Retrying..." if attempt < max_retry else "No more retries.",
                    )
                    if attempt < max_retry:
                        time.sleep(retry_delay)
                        continue
                    break

                if resp.status_code in NON_RETRYABLE_STATUS_CODES:
                    raise Exception(f"Non-retryable HTTP {resp.status_code}: {api_error_message}")

                if resp.status_code in SERVER_ERROR_STATUS_CODES:
                    raise Exception(f"Server error HTTP {resp.status_code}: {api_error_message}")

                raise Exception(f"Unexpected HTTP {resp.status_code}: {api_error_message}")
            except requests.exceptions.RequestException as req_err:
                logger.warning(
                    "Attempt %s/%s failed during %s due to request error: %s\n%s\n%s",
                    attempt,
                    max_retry,
                    request_label,
                    req_err,
                    traceback.format_exc(),
                    "Retrying..." if attempt < max_retry else "No more retries.",
                )
            except Exception as exc:
                if "Non-retryable" in str(exc):
                    logger.error(str(exc))
                    raise
                logger.warning(
                    "Attempt %s/%s failed during %s: %s\n%s",
                    attempt,
                    max_retry,
                    request_label,
                    exc,
                    "Retrying..." if attempt < max_retry else "No more retries.",
                )
        raise Exception(f"Failed after {max_retry} attempts for network: {network_name} during {request_label}")

    def _prepare_request(self, config, start_date, end_date, extra_context: dict[str, Any] | None = None):
        http_method = (config.get("api_method") or "GET").upper()
        context = {**self._base_request_context, **(extra_context or {})}

        url = self.render_template(config.get("api_url_template"), context)

        params_raw = self.render_template(config.get("query_params_template", ""), context).strip()
        params = {}
        if params_raw:
            normalized_params_raw = "&".join(
                fragment.strip()
                for fragment in params_raw.split("&")
                if fragment.strip()
            )
            parsed = parse_qs(normalized_params_raw, keep_blank_values=True)
            params = {
                key.strip(): (
                    value[0].strip()
                    if len(value) == 1 and isinstance(value[0], str)
                    else [item.strip() if isinstance(item, str) else item for item in value]
                )
                for key, value in parsed.items()
            }

        body_raw = self.render_template(config.get("api_body_template", ""), context).strip()
        body = None
        if body_raw:
            try:
                json.loads(body_raw)
                body = body_raw
            except Exception:
                body = json.dumps({"query": body_raw})

        headers_raw = self.render_template(config.get("api_headers_template", "{}"), context)
        try:
            headers = json.loads(headers_raw or "{}")
        except Exception:
            headers = {}

        return url, headers, params, body, http_method

    def _fetch_initial_page(self, config, start_date, end_date):
        url, headers, params, body, http_method = self._prepare_request(config, start_date, end_date)

        self._base_url = url
        self._base_headers = headers
        self._base_params = params
        self._base_body = body
        self._base_http_method = http_method

        return self._request_json_with_retry(
            method=http_method,
            url=url,
            headers=headers,
            params=params,
            body=body,
            config=config,
            request_label="initial page",
        )

    def extract_pagination_token(self, response_json) -> Optional[PaginationToken]:
        mode = (self.config or {}).get("pagination_mode", "none")
        if mode == "none":
            return None

        if mode == "next_url":
            next_url = self._extract_path(response_json, self.config.get("next_url_path"))
            if not next_url:
                return None
            if self.config.get("next_url_relative"):
                next_url = urljoin(self._base_url or "", str(next_url))
            return PaginationToken(next_url=str(next_url))

        if mode == "page_count":
            current_page = self._extract_path(response_json, self.config.get("current_page_path"))
            total_pages = self._extract_path(response_json, self.config.get("total_pages_path"))
            if current_page is None or total_pages is None:
                return None
            current_page = int(current_page)
            total_pages = int(total_pages)
            if current_page < total_pages:
                return PaginationToken(params={self.config.get("page_param_name", "page"): current_page + 1})
            return None

        if mode == "offset_limit":
            offset = self._extract_path(response_json, self.config.get("offset_path"))
            limit = self._extract_path(response_json, self.config.get("limit_path"))
            if offset is None or limit is None:
                return None
            offset = int(offset)
            limit = int(limit)
            if len(self._records_from_payload(response_json)) < limit:
                return None
            return PaginationToken(
                params={
                    self.config.get("offset_param_name", "offset"): offset + limit,
                    self.config.get("limit_param_name", "limit"): limit,
                }
            )

        if mode == "cursor":
            payload_complete = self._extract_path(response_json, self.config.get("cursor_complete_path"))
            if payload_complete in (True, "true", "True", 1, "1"):
                return None
            cursor = self._extract_path(response_json, self.config.get("cursor_path"))
            return PaginationToken(cursor=str(cursor)) if cursor else None

        raise ValueError(f"Unsupported pagination mode: {mode}")

    def _fetch_next_page(self, config, token: PaginationToken):
        headers = self._base_headers or {}
        base_url = self._base_url

        if token.next_url:
            return self._request_json_with_retry(
                method="GET",
                url=token.next_url,
                headers=headers,
                config=config,
                request_label=f"pagination next_url {token.next_url}",
            )

        if token.params:
            merged = {**(self._base_params or {}), **token.params}
            return self._request_json_with_retry(
                method=self._base_http_method or "GET",
                url=base_url,
                headers=headers,
                params=merged,
                body=self._base_body,
                config=config,
                request_label=f"pagination params {token.params}",
            )

        if token.cursor:
            return self._fetch_next_page_cursor(token.cursor)

        raise ValueError("Invalid pagination token")

    def _fetch_next_page_cursor(self, cursor: str):
        context = {"since_cursor": cursor}
        url, headers, params, body, http_method = self._prepare_request(
            self.config,
            self._start_date,
            self._end_date,
            extra_context=context,
        )
        return self._request_json_with_retry(
            method=http_method,
            url=url,
            headers=headers,
            params=params,
            body=body,
            config=self.config,
            request_label=f"pagination cursor {cursor}",
        )

    def render_template(self, template_str: str, context: Dict[str, Any]) -> str:
        if not template_str:
            return ""

        env = Environment(
            loader=BaseLoader(),
            trim_blocks=True,
            lstrip_blocks=True,
            variable_start_string="[[",
            variable_end_string="]]",
            block_start_string="[%",
            block_end_string="%]",
        )

        def format_datetime(value, fmt="%Y-%m-%dT%H:%M:%SZ", unix=False):
            if hasattr(value, "astimezone"):
                dt = value.astimezone(timezone.utc)
            elif hasattr(value, "strftime"):
                dt = value
            else:
                return str(value)
            if unix:
                return str(int(dt.timestamp()))
            return dt.strftime(fmt)

        def b64encode(value: Any) -> str:
            return base64.b64encode(str(value).encode("utf-8")).decode("utf-8")

        env.filters["format_datetime"] = format_datetime
        env.filters["b64encode"] = b64encode
        return env.from_string(template_str).render(context)
