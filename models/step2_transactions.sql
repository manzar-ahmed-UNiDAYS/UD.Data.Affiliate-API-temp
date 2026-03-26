/*
  ******************************************************************************
  * project:      dbt_affiliate_api
  * model name:   step2_trahsactions.sql
  ******************************************************************************
  * Generic affiliate step2 transform model.
  * Purpose:
  *   - Reads step1_http raw transaction payloads.
  *   - Splits child JSON arrays/objects into separate columns for safer
  *     downstream loading into Redshift SUPER.
  ******************************************************************************
*/

{{ config(materialized="table") }}

{% set selected_network = var('affiliate_network_name', target.name) %}
{% set step1_relation = ref('step1_http') %}

/**** Defensive Coding ***
  Downstream Redshift SUPER columns can only handle varchar 65536, so split 
  nested child JSON arrays/objects into separate columns before load/export.
*/
{% set network_child_json_items = {
  'awin_transactions': {'transaction_parts': '$.transactionParts',
           'basket_products': '$.basketProducts'},

  'awin_validations': {'transaction_parts': '$.transactionParts',
           'basket_products': '$.basketProducts'},

  'commission_factory': {'items': '$.Items'},

  'commission_junction': {'items': '$.items'},

  'hasoffers': {},

  'impact_radius': {},

  'partnerize': {'conversion_items': '$.conversion_data.conversion_items'},

  'pepperjam': {},

  'rakuten': {},

  'tradedoubler': {'product_info': '$.productInfo'},

  'webgains': {'items': '$.items'}
} %}

{% set child_json_items = network_child_json_items.get(selected_network, {}) %}

{% if execute %}
  {{ log(" ", info=True) }}
  {{ log("INFO: child_json_items: " ~ (child_json_items | tojson), info=true) }}
  {{ log(" ", info=True) }}
{% endif %}

with step1 as (
    select
        header_id,
        metadata,
        raw_record_json
    from {{ step1_relation }}
),

base as (
    select
        step1.header_id,
        step1.metadata,
        step1.raw_record_json as data,
        md5(step1.raw_record_json) as hash_key
        {% for item_name, item_path in child_json_items | dictsort %}
        ,
        case
            when json_extract(cast(step1.raw_record_json as json), '{{ item_path }}') is null then null
            when cast(json_extract(cast(step1.raw_record_json as json), '{{ item_path }}') as varchar) = 'null' then null
            else json_extract(cast(step1.raw_record_json as json), '{{ item_path }}')
        end as {{ item_name }}
        {% endfor %}
        ,
        current_timestamp as el_loaded_at
    from step1
)

select
    header_id,
    metadata,
    data,
    hash_key
    {% for item_name, item_path in child_json_items | dictsort %}
    ,
    {{ item_name }},
    case
        when {{ item_name }} is null then false
        when left(cast({{ item_name }} as varchar), 1) = '[' then coalesce(json_array_length({{ item_name }}), 0) > 0
        else true
    end as has_{{ item_name }},
    case
        when {{ item_name }} is null then null
        else md5(cast({{ item_name }} as varchar))
    end as hashkey_{{ item_name }}
    {% endfor %}
    ,
    el_loaded_at
from base
