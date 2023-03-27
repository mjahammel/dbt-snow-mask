{% macro unapply_row_access_policy(resource_type="models", meta_key="row_access_policy", operation_type="unapply") %}
  {% if execute %}
    {% if resource_type == "sources" %}
        {{ dbt_snow_mask.apply_row_access_policy_sources(meta_key, operation_type) }}
    {% elif resource_type|lower in ["models", "snapshots"] %}
        {{ dbt_snow_mask.apply_row_access_policy_model(meta_key, operation_type) }}
    {% endif %}
  {% endif %}
{% endmacro %}
