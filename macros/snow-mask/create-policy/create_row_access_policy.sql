{% macro create_row_access_policy(resource_type="sources", meta_key="row_access_policy") %}
  {% if execute %}
    {% set row_access_policies = [] %}
    {% do log('CREATE_ROW_ACCESS_POLICY a: ' ~ resource_type ~ ' :: ' ~ meta_key, info=true) %}

    {% set row_access_policies = dbt_snow_mask.get_row_access_meta_objects(resource_type, meta_key) %}
    {% do log('CREATE_ROW_ACCESS_POLICY d: ' ~ row_access_policies, info=true) %}

    {% for row_access_policy in row_access_policies | unique %}
      {% set row_access_policy_db = row_access_policy[0] %}
      {% set row_access_policy_schema = row_access_policy[1] %}
      {% set row_access_policy_name = row_access_policy[2] %}

      {% do log('CREATE_ROW_ACCESS_POLICY b: ' ~ row_access_policy, info=true) %}

      {% set (row_access_policy_db, row_access_policy_schema) = dbt_snow_mask.calculate_database_and_schema_name(row_access_policy_db, row_access_policy_schema)  %}
      {% do log('CREATE_ROW_ACCESS_POLICY e: ' ~ row_access_policy_db ~ ' :: ' ~ row_access_policy_schema, info=true) %}

      {%- if (var('create_masking_policy_schema', 'True')|upper in ['TRUE','YES']) -%}
        {% do adapter.create_schema(api.Relation.create(database=row_access_policy_db, schema=row_access_policy_schema)) %}
      {% endif %}

      {% set call_row_access_policy_macro = context["create_row_access_policy_" | string ~ row_access_policy_name | string]  %}
      {% do log('CREATE_ROW_ACCESS_POLICY c: ' ~ row_access_policy_db ~ ' :: ' ~ row_access_policy_schema ~ ' :: ' ~ row_access_policy_name ~ ' :: ' ~ call_row_access_policy_macro, info=true) %}
      {% set result = run_query(call_row_access_policy_macro(row_access_policy_db, row_access_policy_schema)) %}
    {% endfor %}
  {% endif %}
{% endmacro %}
