{% macro create_row_access_policy(resource_type="sources", meta_key="row_access_policy") %}

{% if execute %}

    {% set row_access_policies = [] %}

    {% if resource_type == "sources" %}
        {% set row_access_policies = dbt_snow_mask.get_masking_policy_list_for_sources(meta_key) %}
    {% else %}
        {% set row_access_policies = dbt_snow_mask.get_masking_policy_list_for_models(meta_key) %}
    {% endif %}

    {% for row_access_policy in row_access_policies | unique -%}

        {% set row_access_policy_db = row_access_policy[0] | string  %}
        {% set row_access_policy_schema = row_access_policy[1] | string  %}
        {% set row_access_policy_name = row_access_policy[2] | string %}

        {% if row_access_policy_name is not none %}
            {# Override the database and schema name when use_common_masking_policy_db flag is set #}
            {%- if (var('use_common_masking_policy_db', 'False')|upper in ['TRUE','YES']) -%}
                {% if (var('common_masking_policy_db') and var('common_masking_policy_schema')) %}
                    {% set row_access_policy_db = var('common_masking_policy_db') | string  %}
                    {% set row_access_policy_schema = var('common_masking_policy_schema') | string  %}
                {% endif %}
            {% endif %}

            {# Override the schema name (in the row_access_policy_db) when use_common_masking_policy_schema_only flag is set #}
            {%- if (var('use_common_masking_policy_schema_only', 'False')|upper in ['TRUE','YES']) and (var('use_common_masking_policy_db', 'False')|upper in ['FALSE','NO']) -%}
                {% if var('common_masking_policy_schema') %}
                    {% set row_access_policy_schema = var('common_masking_policy_schema') | string  %}
                {% endif %}
            {% endif %}

            {% set current_policy_name = row_access_policy_name | string  %}

            {%- if (var('create_masking_policy_schema', 'True')|upper in ['TRUE','YES']) -%}
                {% do adapter.create_schema(api.Relation.create(database=row_access_policy_db, schema=row_access_policy_schema)) %}
            {% endif %}

            {% set call_masking_policy_macro = context["create_row_access_policy_" | string ~ current_policy_name | string]  %}
            {% set result = run_query(call_masking_policy_macro(row_access_policy_db, row_access_policy_schema)) %}
        {% endif %}
    {% endfor %}

{% endif %}

{% endmacro %}
