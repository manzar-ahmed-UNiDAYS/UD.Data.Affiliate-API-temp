{# Read the shared network -> child JSON path mapping from dbt_project.yml vars. #}
{% macro affiliate_child_json_items_map() %}
  {% set mapping = var('affiliate_child_json_items_map', {}) %}
  {{ return(mapping) }}
{% endmacro %}


{# Return only the child JSON items configured for the selected affiliate network. #}
{% macro affiliate_child_json_items(network_name) %}
  {% set mapping = affiliate_child_json_items_map() %}
  {{ return(mapping.get(network_name, {})) }}
{% endmacro %}
