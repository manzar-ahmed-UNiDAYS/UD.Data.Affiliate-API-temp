# Affiliate Pipeline dbt Project

This project is now a minimal dbt-duckdb affiliate extract with three active models:
   ```mermaid
   flowchart LR
      s1[step1_http] --> s2[step2_transactions]--> s3[step3_upload_to_s3]
   ```

- `step1_http`
- `step2_transactions`
- `step3_upload_to_s3`



`step1_http` calls the affiliate API for one selected network, writes the transaction rows to DuckDB, and creates a separate header table from Python on the same DuckDB connection. `step2_transactions` extracts configured child JSON fields into separate columns. `step3_upload_to_s3` is a terminal dummy model that exports the final transaction table to S3 as parquet. Redshift load logic has been removed from this project.

## Flow

1. `step1_http`
   Python model. Fetches one configured affiliate feed and writes one row per record into DuckDB.
   The Python model also creates `<network>_step1_http_header_<feed>` with one row per request/page header.
2. `step2_transactions`
   SQL model. Adds the final metadata columns and extracts configured child JSON arrays/objects into separate columns.
3. `step3_upload_to_s3`
   SQL model. Dummy one-row transfer marker model. It exports `step2_transactions` to S3 with DuckDB `COPY ... TO` parquet.

The exported parquet path is deterministic:

`<AFFILIATE_S3_EXPORT_ROOT>/<network_name>/<feed_id>/<YYYY>/<MM>/<DD>/<run_id>/step2_transactions.parquet`

## Config Location

The static feed config now lives in [affiliate_config.yml](/home/mahmed/UD.Data.Affiliate-API-temp/affiliate_config.yml). That file contains:

- feed definitions
- request templates
- pagination rules
- masked credential variable names
- per-feed `child_json_items`

`affiliate_config.yml` is the source of truth for the generic pipeline.

## Supported Networks

The project currently supports the following affiliate network definitions from
[affiliate_config.yml](/home/mahmed/UD.Data.Affiliate-API-temp/affiliate_config.yml):

| Network Name | Network ID |
| --- | ---: |
| `awin_transactions` | 7 |
| `awin_validations` | 7 |
| `commission_factory` | 13 |
| `commission_junction` | 17 |
| `hasoffers` | 67 |
| `impact_radius` | 21 |
| `partnerize` | 30 |
| `pepperjam` | 37 |
| `rakuten` | 25 |
| `tradedoubler` | 52 |
| `webgains` | 42 |

## Local Usage

Source the local env file first:

```bash
cd /home/mahmed/UD.Data.Affiliate-API-temp
set -a
source .env
set +a
```

Set the S3 export root and the required credential env vars for the selected network:

```bash
export AFFILIATE_DUCKDB_PATH=/tmp/affiliate_pipeline
export AFFILIATE_S3_EXPORT_ROOT=s3://my-bucket/affiliate_api
export AFFILIATE_AWIN_API_TOKEN=...
```

`AFFILIATE_DUCKDB_PATH` should be the parent directory only. The selected target appends its own DuckDB filename, for example `affiliate_awin_transactions.duckdb`.

Run one feed:

```bash
/home/mahmed/dbt_venv/bin/dbt run \
  --project-dir "$DBT_PROJECT_DIR" \
  --profiles-dir "$DBT_PROFILES_DIR" \
  --target awin_transactions \
  --select +step3_upload_to_s3 \
  --vars '{affiliate_network_name: "awin_transactions", lookback_days: 7, end_date: "2026-03-24"}'
```

## Airflow Variables

Global Airflow Variables:

- `AFFILIATE_DBT_PROJECT_DIR`
- `AFFILIATE_DBT_PROFILES_DIR`
- `AFFILIATE_S3_EXPORT_ROOT`
- `AFFILIATE_DBT_BIN`
- `AFFILIATE_DBT_TARGET`
- `AWS_DEFAULT_REGION`
- `DEPLOY_ENV`

Masked Airflow Variables for affiliate credentials:

- `AFFILIATE_AWIN_API_TOKEN`
- `AFFILIATE_COMMISSION_FACTORY_API_TOKEN`
- `AFFILIATE_COMMISSION_JUNCTION_API_TOKEN`
- `AFFILIATE_HASOFFERS_API_TOKEN`
- `AFFILIATE_IMPACT_RADIUS_API_USERNAME`
- `AFFILIATE_IMPACT_RADIUS_API_PASSWORD`
- `AFFILIATE_PARTNERIZE_API_USERNAME`
- `AFFILIATE_PARTNERIZE_API_PASSWORD`
- `AFFILIATE_PEPPERJAM_API_KEY`
- `AFFILIATE_RAKUTEN_ACCESS_TOKEN`
- `AFFILIATE_TRADEDOUBLER_ACCESS_TOKEN`
- `AFFILIATE_WEBGAINS_API_TOKEN`

The MWAA DAG looks up env-scoped variables first. For example, with `DEPLOY_ENV=prod`, it will check `PROD_AFFILIATE_AWIN_API_TOKEN` before `AFFILIATE_AWIN_API_TOKEN`.

## MWAA

A new MWAA DAG file was added at [affiliate_dbt_pipeline_daily.py](/home/mahmed/ud-data-mwaa/UD.Data.Affiliate-API/dags/affiliate_dbt_pipeline_daily.py). It creates one daily DAG per configured network/feed and runs:

- `dbt run --select +step3_upload_to_s3`

The DAG reads [affiliate_config.yml](/home/mahmed/UD.Data.Affiliate-API-temp/affiliate_config.yml) at runtime to determine which masked Airflow Variables must be injected into the dbt task environment.

## Notes

- The project no longer queries Redshift for runtime config.
- The project no longer loads parquet into Redshift.
- `step1_http` keeps request/response audit fields in DuckDB and moves them into the separate header table.
- `step3_upload_to_s3` exports only the final transaction table.
