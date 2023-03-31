{% macro apply_row_access_policy(resource_type="models", meta_key="row_access_policy") %}
  {% if execute %}
    {% if resource_type == "sources" %}
      {{ dbt_snow_mask.apply_row_access_policy_sources(meta_key) }}
    {% elif resource_type|lower in ["models", "snapshots"] %}
      {{ dbt_snow_mask.apply_row_access_policy_model(meta_key) }}
    {% endif %}
  {% endif %}
{% endmacro %}