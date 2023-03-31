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

      {% set row_access_policies = dbt_snow_mask.get_row_access_meta_objects(resource_type, meta_key, true, unique_id) %}
      {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES c: ' ~ row_access_policies, info=true) %}

      {# Use the database and schema for the source node: #}
      {% set row_access_policy_db = node.database %}
      {% set row_access_policy_schema = node.schema %}
		
      {% set (row_access_policy_db, row_access_policy_schema) = dbt_snow_mask.calculate_database_and_schema_name(row_access_policy_db, row_access_policy_schema) %}

      {% set row_access_policy_list_sql %}
        show row access policies in {{row_access_policy_db}}.{{row_access_policy_schema}};
        select $3||'.'||$4||'.'||$2 as row_access_policy from table(result_scan(last_query_id()));
      {% endset %}
      {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES l: ' ~ row_access_policy_list_sql, info=true) %}

      {# If there are some masking policies to be applied in this model, we should show the masking policies in the schema #}
      {% if row_access_policies | length > 0 %}
        {% set row_access_policy_list = dbt_utils.get_query_results_as_dict(row_access_policy_list_sql) %}
      {% endif %}

      {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES d: ' ~ row_access_policy_list, info=true) %}

      {%- for meta_tuple in row_access_policies if row_access_policies | length > 0 %}
        {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES f: ' ~ meta_tuple, info=true) %}
        {% set columns = meta_tuple[3] %}
        {% set row_access_policy_name = meta_tuple[2] %}
        {% if row_access_policy_name is not none %}
          {% for row_access_policy_in_db in row_access_policy_list['ROW_ACCESS_POLICY'] %}
            {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES h: ' ~ columns ~ ' :: ' ~ row_access_policy_name ~ ' :: ' ~ row_access_policy_in_db ~ ' :: ' ~ row_access_policy_list['ROW_ACCESS_POLICY'], info=true) %}
            {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES i: ' ~ row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper ~ ' :: ' ~ row_access_policy_in_db, info=true) %}
            {% if row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper == row_access_policy_in_db %}
              {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing row access policy to model  : " ~ 
                                                                                    row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ 
                                                                                    row_access_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ name ~ '.' ~ 
                                                                                    columns ~ ' [force = ' ~ var('use_force_applying_masking_policy','False') ~ ']', info=True) }}
              {# if force is in place, find if there is an existing row access policy on the object that must be replaced #}
              {% if var('use_force_applying_masking_policy','False')|upper in ['TRUE','YES'] %}
                {% set existing_policy_sql %}
                  select policy_db || '.' || policy_schema || '.' || policy_name as row_access_policy
                  from table({{database}}.information_schema.policy_references(ref_entity_name => '{{ database }}.{{ schema }}.{{ name }}', ref_entity_domain => '{{ materialization }}'))
                  where policy_kind = 'ROW_ACCESS_POLICY';
                {% endset %}
                {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES j: ' ~ existing_policy_sql, info=true) %}
                {% set results = run_query(existing_policy_sql) %}
                {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES m: ' ~ results, info=true) %}
                {% if results %}
                  {% set existing_policy = results.rows[0][0] %}
                {% endif %}
                {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES k: ' ~ existing_policy, info=true) %}
              {% endif %}

              {% set query %}
                alter {{materialization}} {{database}}.{{schema}}.{{name}} 
                {% if operation_type == "apply" %}
                  {%- if var('use_force_applying_masking_policy','False')|upper in ['TRUE','YES'] and existing_policy is defined %} drop row access policy {{ existing_policy }}, {%- endif %}
                  add row access policy {{row_access_policy_db}}.{{row_access_policy_schema}}.{{row_access_policy_name}} on (
                    {% for column in columns %} {{ column }} {% if not loop.last %},{% endif %}{% endfor %}
                  );
                {% elif operation_type == "unapply" %}
                  drop row access policy {{ existing_policy }};
                {% endif %}
              {% endset %}
              {% do log('APPLY_ROW_ACCESS_POLICY_SOURCES g: ' ~ query, info=true) %}
              {% do run_query(query) %}
            {% endif %}
          {% endfor %}        
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endif %}
{% endmacro %}
