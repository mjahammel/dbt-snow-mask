{% macro apply_row_access_policy_sources(meta_key, operation_type="apply") %}
  {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES b: ' ~ meta_key ~ ' :: ' ~ operation_type, info=true) %}

  {% if execute %}
    {% for node in graph.sources.values() -%}
      {% set database = node.database | string %}
      {% set schema = node.schema | string %}
      {% set name = node.name | string %}
      {% set identifier = (node.identifier | default(name, True)) | string %}
      {% set unique_id = node.unique_id | string %}
      {% set resource_type = node.resource_type | string %}
      {% set materialization = "table" %}

      {% set relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
      {% if relation.is_view %}
        {% set materialization = "view" %}
      {% endif %}

      {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES a: ' ~ database ~ ' :: ' ~
                                                        schema ~ ' :: ' ~
                                                        name ~ ' :: ' ~
                                                        identifier ~ ' :: ' ~
                                                        unique_id ~ ' :: ' ~
                                                        resource_type ~ ' :: ' ~
                                                        materialization ~ ' :: ' ~
                                                        relation, info=true) %}

      {% set meta_columns = dbt_snow_mask.get_meta_objects(unique_id, meta_key, resource_type) %}
      {% set row_access_policies = dbt_snow_mask.get_row_access_meta_objects(resource_type, meta_key, true, unique_id) %}
      {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES c: ' ~ meta_columns, info=true) %}
      {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES d: ' ~ row_access_policies, info=true) %}

      {# Use the database and schema for the source node: #}
      {% set row_access_policy_db = node.database %}
      {% set row_access_policy_schema = node.schema %}
		
      {% set (row_access_policy_db, row_access_policy_schema) = dbt_snow_mask.calculate_database_and_schema_name(row_access_policy_db, row_access_policy_schema) %}

      {% set masking_policy_list_sql %}
        show row access policies in {{row_access_policy_db}}.{{row_access_policy_schema}};
        select $3||'.'||$4||'.'||$2 as masking_policy from table(result_scan(last_query_id()));
      {% endset %}

      {# If there are some masking policies to be applied in this model, we should show the masking policies in the schema #}
      {% if meta_columns | length > 0 %}
        {% set masking_policy_list = dbt_utils.get_query_results_as_dict(masking_policy_list_sql) %}
      {% endif %}

      {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
        {% set column   = meta_tuple[0] %}
        {% set masking_policy_name  = meta_tuple[1] %}

        {% if masking_policy_name is not none %}
          {% for masking_policy_in_db in masking_policy_list['MASKING_POLICY'] %}
            {% if row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ masking_policy_name|upper == masking_policy_in_db %}
              {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing masking policy to source : " ~ row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ masking_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ identifier ~ '.' ~ column ~ ' [force = ' ~ var('use_force_applying_masking_policy','False') ~ ']', info=True) }}
              {% set query %}
                {% if operation_type == "apply" %}
                  alter {{materialization}}  {{database}}.{{schema}}.{{identifier}} modify column  {{column}} set masking policy  {{row_access_policy_db}}.{{row_access_policy_schema}}.{{masking_policy_name}} {% if var('use_force_applying_masking_policy','False')|upper in ['TRUE','YES'] %} force {% endif %}
                {% elif operation_type == "unapply" %}
                  alter {{materialization}}  {{database}}.{{schema}}.{{identifier}} modify column  {{column}} unset masking policy
                {% endif %}
              {% endset %}
              {% do run_query(query) %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endif %}
{% endmacro %}
