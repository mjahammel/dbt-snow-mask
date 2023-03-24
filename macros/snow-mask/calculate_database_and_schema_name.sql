{% macro calculate_database_and_schema_name(database_name, schema_name) %}
  {# Override the database and schema name when use_common_masking_policy_db flag is set #}
  {%- if (var('use_common_masking_policy_db', 'False')|upper in ['TRUE','YES']) -%}
    {% if (var('common_masking_policy_db') and var('common_masking_policy_schema')) %}
      {% set database_name = var('common_masking_policy_db') | string  %}
      {% set schema_name = var('common_masking_policy_schema') | string  %}
    {% endif %}
  {% endif %}

  {# Override the schema name (in the masking_policy_db) when use_common_masking_policy_schema_only flag is set #}
  {%- if (var('use_common_masking_policy_schema_only', 'False')|upper in ['TRUE','YES']) and (var('use_common_masking_policy_db', 'False')|upper in ['FALSE','NO']) -%}
    {% if var('common_masking_policy_schema') %}
      {% set schema_name = var('common_masking_policy_schema') | string  %}
    {% endif %}
  {% endif %}

  {% set return_tuple = (database_name, schema_name) %}
  {{ return(return_tuple) }}
{% endmacro %}