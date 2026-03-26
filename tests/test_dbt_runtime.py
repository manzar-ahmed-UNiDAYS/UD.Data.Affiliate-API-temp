from __future__ import annotations

import os
import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from python_utils.feed_config import (
    get_feed_spec,
    list_feed_specs,
    list_required_credential_variables,
    resolve_feed_spec,
)
from python_utils.handler import BaseNetworkHandler
from python_utils.settings import get_settings


class _DummyResponse:
    def __init__(self, status_code: int, payload=None, text: str | None = None, headers=None) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = text if text is not None else ("" if payload is None else repr(payload))
        self.headers = headers or {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


def _write_affiliate_config_yaml(config_path: Path) -> None:
    config_path.write_text(
        "affiliate_defaults:\n"
        "  api_retry_delay: 10\n"
        "affiliate_pipelines:\n"
        "  - network_id: 7\n"
        "    network_name: awin_transactions\n"
        "    credentials:\n"
        "      api_token: AFFILIATE_AWIN_API_TOKEN\n"
        "    feeds:\n"
        "      - feed_id: transactions\n"
        "        feed_name: transactions\n"
        "        program_id: '533915'\n"
        "        child_json_items:\n"
        "          - basketProducts\n"
        "        request:\n"
        "          method: GET\n"
        "          url_template: https://api.example.com\n"
        "        response:\n"
        "          records_path: $\n",
        encoding="utf-8",
    )


class DbtRuntimeHelpersTest(unittest.TestCase):
    def test_list_feed_specs_reads_default_affiliate_config(self) -> None:
        specs = list_feed_specs()
        self.assertTrue(specs)
        awin_transactions = get_feed_spec("awin_transactions", "transactions")
        self.assertEqual(awin_transactions.network_id, 7)

    def test_resolve_feed_spec_reads_affiliate_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "affiliate_config.yml"
            _write_affiliate_config_yaml(config_path)
            spec = resolve_feed_spec(config_path=config_path)

        self.assertEqual(spec.network_name, "awin_transactions")
        self.assertEqual(spec.feed_id, "transactions")
        self.assertEqual(spec.child_json_items, ("basketProducts",))

    def test_list_required_credential_variables_reads_affiliate_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "affiliate_config.yml"
            _write_affiliate_config_yaml(config_path)

            credential_vars = list_required_credential_variables(config_path=config_path)

        self.assertEqual(credential_vars, ["AFFILIATE_AWIN_API_TOKEN"])

    def test_settings_default_to_affiliate_config(self) -> None:
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            get_settings.cache_clear()
            try:
                with mock.patch.dict(
                    os.environ,
                    {"DBT_PROJECT_DIR": temp_dir},
                    clear=False,
                ):
                    settings = get_settings()
            finally:
                get_settings.cache_clear()

        self.assertEqual(settings.project_root, temp_path.resolve())
        self.assertEqual(
            settings.affiliate_config_path,
            temp_path / "affiliate_config.yml",
        )

    def test_prepare_request_strips_folded_yaml_whitespace_from_query_params(self) -> None:
        handler = BaseNetworkHandler()
        start_date = datetime(2026, 3, 12)
        end_date = datetime(2026, 3, 19)
        handler._base_request_context = {
            "start_date": start_date,
            "end_date": end_date,
        }

        _, _, params, _, _ = handler._prepare_request(
            {
                "api_method": "GET",
                "api_url_template": "https://api.example.com/report",
                "query_params_template": (
                    'endDate=[[ end_date | format_datetime("%Y-%m-%dT%H:%M:%SZ") ]]&\n'
                    '  timezone=UTC&\n'
                    '  dateType=transaction&\n'
                    '  startDate=[[ start_date | format_datetime("%Y-%m-%dT%H:%M:%SZ") ]]'
                ),
                "api_headers_template": "{}",
            },
            start_date,
            end_date,
        )

        self.assertEqual(
            params,
            {
                "endDate": "2026-03-19T00:00:00Z",
                "timezone": "UTC",
                "dateType": "transaction",
                "startDate": "2026-03-12T00:00:00Z",
            },
        )

    def test_build_base_request_context_uses_env_backed_credentials(self) -> None:
        handler = BaseNetworkHandler()
        start_date = datetime(2026, 3, 12)
        end_date = datetime(2026, 3, 19)

        with mock.patch.dict(
            os.environ,
            {"AFFILIATE_AWIN_API_TOKEN": "secret-token"},
            clear=False,
        ):
            context = handler._build_base_request_context(
                {
                    "credential_variables": {
                        "api_token": "AFFILIATE_AWIN_API_TOKEN",
                    }
                },
                start_date,
                end_date,
            )

        self.assertEqual(context["api_token"], "secret-token")

    def test_build_base_request_context_requires_missing_credentials(self) -> None:
        handler = BaseNetworkHandler()
        start_date = datetime(2026, 3, 12)
        end_date = datetime(2026, 3, 19)

        with self.assertRaisesRegex(ValueError, "AFFILIATE_AWIN_API_TOKEN"):
            handler._build_base_request_context(
                {
                    "credential_variables": {
                        "api_token": "AFFILIATE_AWIN_API_TOKEN",
                    }
                },
                start_date,
                end_date,
            )

    def test_fetch_retries_paginated_page_count_requests(self) -> None:
        handler = BaseNetworkHandler()
        start_date = datetime(2026, 3, 12)
        end_date = datetime(2026, 3, 19)
        calls: list[dict[str, object]] = []
        responses = iter(
            [
                _DummyResponse(200, {"page": 1, "total_pages": 2, "records": [{"id": 1}]}),
                _DummyResponse(429, {"message": "rate limited"}),
                _DummyResponse(200, {"page": 2, "total_pages": 2, "records": [{"id": 2}]}),
            ]
        )

        def request_sender(**kwargs):
            calls.append(kwargs)
            return next(responses)

        handler.request_sender = request_sender
        config = {
            "network_name": "awin_transactions",
            "api_method": "GET",
            "api_url_template": "https://api.example.com/report",
            "api_headers_template": "{}",
            "query_params_template": "",
            "records_path": "records",
            "pagination_mode": "page_count",
            "current_page_path": "page",
            "total_pages_path": "total_pages",
            "api_max_retry": 3,
            "api_retry_delay": 7,
        }

        with self.assertLogs("python_utils.handler", level="INFO") as log_capture:
            with mock.patch("python_utils.handler.time.sleep") as sleep_mock:
                pages = list(handler.fetch(config, start_date, end_date))

        self.assertEqual(
            pages,
            [
                {"page": 1, "total_pages": 2, "records": [{"id": 1}]},
                {"page": 2, "total_pages": 2, "records": [{"id": 2}]},
            ],
        )
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[1]["params"], {"page": 2})
        self.assertEqual(calls[2]["params"], {"page": 2})
        self.assertEqual(len(handler.request_history), 2)
        sleep_mock.assert_any_call(2)
        sleep_mock.assert_any_call(7)
        self.assertIn("INFO:python_utils.handler:Max retries: 3, Retry delay: 7s", log_capture.output)
        self.assertIn("INFO:python_utils.handler:Attempt 1 for URL: https://api.example.com/report", log_capture.output)
        self.assertIn("INFO:python_utils.handler:Attempt 2 for URL: https://api.example.com/report", log_capture.output)

    def test_fetch_raises_for_non_retryable_paginated_next_url_requests(self) -> None:
        handler = BaseNetworkHandler()
        start_date = datetime(2026, 3, 12)
        end_date = datetime(2026, 3, 19)
        calls: list[dict[str, object]] = []
        responses = iter(
            [
                _DummyResponse(
                    200,
                    {
                        "records": [{"id": 1}],
                        "next": "https://api.example.com/report?page=2",
                    },
                ),
                _DummyResponse(404, {"message": "missing page"}),
            ]
        )

        def request_sender(**kwargs):
            calls.append(kwargs)
            return next(responses)

        handler.request_sender = request_sender
        config = {
            "network_name": "awin_transactions",
            "api_method": "GET",
            "api_url_template": "https://api.example.com/report",
            "api_headers_template": "{}",
            "query_params_template": "",
            "records_path": "records",
            "pagination_mode": "next_url",
            "next_url_path": "next",
            "api_max_retry": 3,
            "api_retry_delay": 7,
        }

        with mock.patch("python_utils.handler.time.sleep") as sleep_mock:
            with self.assertRaisesRegex(Exception, "Non-retryable HTTP 404"):
                list(handler.fetch(config, start_date, end_date))

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["url"], "https://api.example.com/report?page=2")
        self.assertEqual(len(handler.request_history), 1)
        self.assertEqual([call.args[0] for call in sleep_mock.call_args_list], [2])


if __name__ == "__main__":
    unittest.main()
