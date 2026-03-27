/*
  ******************************************************************************
  * project:      dbt_affiliate_api
  * model name:   step4_copy_to_redshift.sql
  ******************************************************************************
  * Redshift landing load model.
  * Purpose:
  *   - Uses a Redshift COPY pre-hook to load Step 3 parquet into a temporary table.
  *   - Dynamically projects child JSON columns based on affiliate network config.
  *   - Incrementally merges into landing.affiliate__<network_name> using hash_key.
  ******************************************************************************
*/

{% set selected_network  = var('affiliate_network_name', 'awin_transactions') %}
{% set child_json_items  = affiliate_child_json_items(selected_network) %}
{% set aws_iam_role      = var('aws_iam_role', env_var('AWS_IAM_ROLE', '')) %}
{% set s3_path           = var('s3_path', '') %}
{% set stage_table_name  = 'tmp_' ~ selected_network  %}

{% if execute and target.type == 'redshift' and not aws_iam_role %}
  {% do exceptions.raise_compiler_error("❌ missing aws_iam_role var or AWS_IAM_ROLE env var.") %}
{% endif %}

{% if execute and target.type == 'redshift' and not s3_path %}
  {% do exceptions.raise_compiler_error("❌ missing s3_path var.") %}
{% endif %}

{% if execute %}
  {{ log(" ", info=True) }}
  {{ log("ℹ️ selected_network: " ~ selected_network, info=True) }}
  {{ log("ℹ️ target_name: " ~ target.name, info=True) }}
  {{ log("ℹ️ target_type: " ~ target.type, info=True) }}
  {{ log("ℹ️ stage_table_name: " ~ stage_table_name, info=True) }}
  {{ log("ℹ️ s3_path: " ~ s3_path, info=True) }}
  {{ log("ℹ️ aws_iam_role_present: " ~ (aws_iam_role != '') | string, info=True) }}
  {{ log("ℹ️ child_json_items: " ~ (child_json_items | tojson), info=True) }}
  {{ log(" ", info=True) }}
{% endif %}

{% set stage_columns = [
    {'name': 'header_id', 'type': 'varchar(64)'},
    {'name': 'metadata', 'type': 'super'},
    {'name': 'data', 'type': 'super'},
    {'name': 'hash_key', 'type': 'varchar(32)'}
] %}
{% for item_name, item_path in child_json_items | dictsort %}
  {% do stage_columns.append({'name': item_name, 'type': 'super'}) %}
  {% do stage_columns.append({'name': 'has_' ~ item_name, 'type': 'boolean'}) %}
  {% do stage_columns.append({'name': 'hashkey_' ~ item_name, 'type': 'varchar(32)'}) %}
{% endfor %}
{% do stage_columns.append({'name': 'el_loaded_at', 'type': 'timestamp'}) %}

{% set create_copy_stage_sql %}
create temp table {{ stage_table_name }} (
  {%- for column in stage_columns %}
  {{ column.name }} {{ column.type }}{% if not loop.last %},{% endif %}
  {%- endfor %}
)
{% endset %}

{% set copy_into_stage_sql %}

  copy {{ stage_table_name }}
  from '{{ s3_path }}'
  iam_role '{{ aws_iam_role }}'
  format as parquet
  serializetojson

{% endset %}

{{ config(
    enabled              = (target.type == 'redshift'),
    materialized         = 'incremental',
    schema               = 'landing',
    alias                = 'affiliate__' ~ selected_network,
    unique_key           = 'hash_key',
    incremental_strategy = 'merge',
    pre_hook             = [
        {"sql": "drop table if exists " ~ stage_table_name, "transaction": false},
        {"sql": create_copy_stage_sql, "transaction": false},
        {"sql": copy_into_stage_sql, "transaction": false},
    ]
) }}

select
    header_id,
    metadata,
    data,
    hash_key
    {% for item_name, item_path in child_json_items | dictsort %}
    ,
    {{ item_name }},
    has_{{ item_name }},
    hashkey_{{ item_name }}
    {% endfor %}
    ,
    el_loaded_at
from {{ stage_table_name }}
