{% macro apply_row_access_policy(resource_type="models", meta_key="row_access_policy") %}

    {% if execute %}
        {% do log('APPLY_ROW_ACCESS_POLICY: ' ~ resource_type ~ ' :: ' ~ meta_key, info=true) %}

        {% if resource_type == "sources" %}
            {{ dbt_snow_mask.apply_masking_policy_list_for_sources(meta_key) }}
        {% elif resource_type|lower in ["models", "snapshots"] %}
            {{ dbt_snow_mask.apply_masking_policy_list_for_models(meta_key) }}
        {% endif %}

    {% endif %}

{% endmacro %}