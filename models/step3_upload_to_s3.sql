/*
  ******************************************************************************
  * project:      dbt_affiliate_api
  * model name:   step3_upload_to_s3.sql
  ******************************************************************************
  * Generic affiliate step3 export model.
  * Purpose:
  *   - Exports step2_transactions output to parquet.
  *   - Updates step1_http_header with export_path and is_success.
  ******************************************************************************
*/

{% set local_export_root    = var('affiliate_s3_export_root', none) %}
{% set aws_s3_bucket        = var('aws_s3_bucket') %}
{% set selected_network     = var('affiliate_network_name', target.name)  %}
{% set export_end_date      = var('end_date', run_started_at.strftime('%Y-%m-%d')) %}
{% set date_parts           = export_end_date.split('-') %}
{% set export_date_yyyymmdd = date_parts[0] ~ date_parts[1] ~ date_parts[2] %}
{% set export_time_hhmmss   = run_started_at.strftime('%H%M%S') %}
{% set export_filename      = selected_network ~ '_' ~ export_date_yyyymmdd ~ '_' ~ export_time_hhmmss ~ '.parquet' %}
{% if local_export_root %}
  {% set export_path        = local_export_root 
                              ~ '/' 
                              ~ export_filename 
  %}
{% else %}
  {% set export_path        = aws_s3_bucket 
                              ~ '/' 
                              ~ selected_network 
                              ~ '/' 
                              ~ date_parts[0] 
                              ~ '/' 
                              ~ date_parts[1] 
                              ~ '/' 
                              ~ date_parts[2] 
                              ~ '/' 
                              ~ export_filename 
  %}
{% endif %}

{{ log(" ", info=True) }}
{{ log("ℹ️ aws_s3_bucket: " ~ aws_s3_bucket, info=True) }}
{{ log("ℹ️ export_path: " ~ export_path, info=True) }}
{{ log(" ", info=True) }}

{% set hook_database           = this.database %}
{% set hook_schema             = this.schema %}
{% set step2_relation_name     = '"' ~ hook_database ~ '"."' ~ hook_schema ~ '"."step2_transactions"' %}
{% set step1_header_relation   = '"' ~ hook_database ~ '"."' ~ hook_schema ~ '"."step1_http_header"' %}

{% set upload_to_s3 %}
    copy (
        select *
        from {{ step2_relation_name }}
    ) to '{{ export_path }}'
    (format parquet, compression zstd);
{% endset %}

{% set update_header_export_path %}
    update {{ step1_header_relation }}
    set
        export_path = '{{ export_path }}',
        is_success = case
            when cast(json_extract_string(response_json, '$.status_code') as integer) = 200
                and page_record_count > 0
                and '{{ export_path }}' <> ''
            then true
            else false
        end
    ;
{% endset %}

{{ config(
    materialized="table",
    post_hook=[
        {"sql": upload_to_s3, "transaction": false},
        {"sql": update_header_export_path, "transaction": false},
    ]
) }}


select 
  current_timestamp as transfer_ts
from {{ref('step2_transactions') }}
limit 1
