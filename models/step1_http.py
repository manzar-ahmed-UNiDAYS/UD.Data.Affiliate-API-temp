#*******************************************************************************
# project:      dbt_affiliate_api
# model name:   step1_http.py
#*******************************************************************************
# Generic affiliate step1 HTTP extract model.
#*******************************************************************************
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from python_utils.feed_config import resolve_feed_spec
from python_utils.handler import BaseNetworkHandler
from python_utils.modeling import parse_date_string, resolve_window

logger = logging.getLogger(__name__)

def _log(message: str, label: str = "ℹ️") -> None:
    # Print keeps the message visible in dbt Python model execution output.
    print(f"{datetime.now().strftime('%H:%M:%S')}  {label}: {message}")
    log_method = getattr(logger, label.lower(), logger.info)
    log_method(message)


def _log_info(message: str) -> None:
    _log(message, label="ℹ️")

def _log_error(message: str) -> None:
    _log(message, label="❌")


def _dbt_meta_value(dbt, key: str) -> str | None:
    raw_meta = dbt.config.get("meta") or {}
    if not isinstance(raw_meta, dict):
        return None

    raw_value = raw_meta.get(key)
    if raw_value is None:
        return None

    value = str(raw_value).strip()
    return value or None


def _json_dumps(value: Any, *, sort_keys: bool = True) -> str:
    return json.dumps(value, default=str, sort_keys=sort_keys)


def _log_pretty_json(label: str, value: Any) -> None:
    pretty_json = json.dumps(value, default=str, sort_keys=True, indent=2)
    _log_info(f"{label}:")
    for line in pretty_json.splitlines():
        _log_info(line)


def _records_from_page(handler: BaseNetworkHandler, page_json: Any) -> list[Any]:
    records = handler._records_from_payload(page_json)  # noqa: SLF001 - shared runtime helper
    return records if isinstance(records, list) else list(records)


def _metadata_value(value: Any) -> Any:
    if value is None:
        return None

    value_str = str(value).strip()
    if value_str.isdigit():
        return int(value_str)
    return value


def _metadata_payload(spec, start_date: str, end_date: str) -> dict[str, Any]:
    return {
        "network_id": int(spec.network_id),
        "network_name": spec.network_name,
        "feed_id": spec.feed_id,
        "feed_name": spec.feed_name,
        "publisher_id": _metadata_value(spec.publisher_id),
        "start_date": start_date,
        "end_date": end_date,
    }


def _header_id(request_payload: dict[str, Any]) -> str:
    return hashlib.md5(_json_dumps(request_payload).encode("utf-8")).hexdigest()


http_transaction_columns = [
    "header_id",
    "metadata",
    "page_number",
    "page_record_index",
    "raw_record_json",
]

http_header_columns = [
    "header_id",
    "metadata",
    "page_number",
    "request_sent_at",
    "response_received_at",
    "request_method",
    "request_url",
    "request_json",
    "response_json",
    "page_record_count",
    "export_path",
    "is_success",
]


def _header_relation_name(dbt) -> str:
    relation_name = str(dbt.this)
    header_identifier = dbt.this.identifier.replace("step1_http", "step1_http_header")
    return relation_name.replace(dbt.this.identifier, header_identifier, 1)


def _materialize_header_table(session, dbt, header_df) -> None:
    temp_relation = f"__{dbt.this.identifier}_header_df"
    session.register(temp_relation, header_df)
    try:
        session.execute(
            f"""
            create or replace table {_header_relation_name(dbt)} as
            select
                header_id,
                cast(metadata as json) as metadata,
                cast(page_number as bigint) as page_number,
                cast(request_sent_at as timestamp with time zone) as request_sent_at,
                cast(response_received_at as timestamp with time zone) as response_received_at,
                request_method,
                request_url,
                cast(request_json as json) as request_json,
                cast(response_json as json) as response_json,
                cast(page_record_count as bigint) as page_record_count,
                export_path,
                cast(is_success as boolean) as is_success
            from "{temp_relation}"
            """
        )
    finally:
        try:
            session.unregister(temp_relation)
        except Exception:
            pass


def model(dbt, session):
    dbt.config(materialized="table")

    import pandas as pd

    spec = resolve_feed_spec(
        network_name=_dbt_meta_value(dbt, "affiliate_network_name"),
    )

    raw_end_date = _dbt_meta_value(dbt, "end_date")
    lookback_days = int(_dbt_meta_value(dbt, "lookback_days") or 7)
    parsed_end_date = parse_date_string(raw_end_date) if raw_end_date else None
    window = resolve_window(lookback_days=lookback_days, end_date=parsed_end_date)

    start_date = window.start_date.isoformat()
    end_date = window.end_date.isoformat()
    metadata_json = _json_dumps(_metadata_payload(spec, start_date, end_date), sort_keys=False)

    handler = BaseNetworkHandler(config=spec.config)
    
    _log_info(f"network_id: {spec.network_id}")
    _log_info(f"network_name: {spec.network_name}")
    _log_info(f"feed_id: {spec.feed_id}")
    _log_info(f"feed_name: {spec.feed_name}")

    rows: list[dict[str, Any]] = []
    header_rows: list[dict[str, Any]] = []
    batch_header_id: str | None = None
    for page_number, page_json in enumerate(
        handler.fetch(
            spec.config,
            window.start_date,
            window.end_date,
        ),
        start=1,
    ):
        request_audit = (
            handler.request_history[page_number - 1]
            if len(handler.request_history) >= page_number
            else {}
        )
        request_payload = request_audit.get("request") or {}
        response_payload = request_audit.get("response") or {}
        request_method = request_payload.get("method")
        request_url = request_payload.get("url")
        response_status_code = response_payload.get("status_code")
        request_sent_at = request_audit.get("request_sent_at")
        response_received_at = request_audit.get("response_received_at")

        if request_url:
            _log_info(f"request_url: {request_url}")
        if request_payload:
            _log_pretty_json("request_json", request_payload)
        if request_method and response_status_code is not None:
            if int(response_status_code) >= 400:
                _log_error(f"❌ {request_method} response code: {response_status_code}")
            else:
                _log_info(f"ℹ️ {request_method} response code: {response_status_code}")

        if batch_header_id is None:
            batch_header_id = _header_id(request_payload)
        header_id = batch_header_id

        page_record_count = 0
        for page_record_index, record in enumerate(_records_from_page(handler, page_json), start=1):
            normalized_record = handler._normalize_record(record)  # noqa: SLF001 - shared runtime helper
            raw_record_json = _json_dumps(normalized_record)
            rows.append(
                {
                    "header_id": header_id,
                    "metadata": metadata_json,
                    "page_number": page_number,
                    "page_record_index": page_record_index,
                    "raw_record_json": raw_record_json,
                }
            )
            page_record_count += 1

        _log_info(f"page_record_count: {page_record_count}")

        if page_record_count > 0:
            header_rows.append(
                {
                    "header_id": header_id,
                    "metadata": metadata_json,
                    "page_number": page_number,
                    "request_sent_at": request_sent_at,
                    "response_received_at": response_received_at,
                    "request_method": request_payload.get("method"),
                    "request_url": request_url,
                    "request_json": _json_dumps(request_payload),
                    "response_json": _json_dumps(response_payload),
                    "page_record_count": page_record_count,
                    "export_path": "",
                    "is_success": False,
                }
            )

    df = pd.DataFrame(rows, columns=http_transaction_columns)
    header_df = pd.DataFrame(header_rows, columns=http_header_columns)
    _materialize_header_table(session, dbt, header_df)
    temp_relation = f"__{dbt.this.identifier}_df"
    session.register(temp_relation, df)
    return session.sql(
        f"""
        select
            header_id,
            cast(metadata as json) as metadata,
            cast(page_number as bigint) as page_number,
            cast(page_record_index as bigint) as page_record_index,
            raw_record_json
        from "{temp_relation}"
        """
    )
