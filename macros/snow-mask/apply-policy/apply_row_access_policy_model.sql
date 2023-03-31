{% macro apply_row_access_policy_model(meta_key, operation_type="apply") %}
  {% if execute %}
    {% if operation_type == "apply" %}
      {% set model_id = model.unique_id | string %}
      {% set alias    = model.alias %}    
      {% set database = model.database %}
      {% set schema   = model.schema %}
      {% set model_resource_type = model.resource_type | string %}
      {% do log('APPLY_ROW_ACCESS_POLICY_MODEL a: ' ~ model_id ~ ' :: ' ~ 
                                                      alias ~ ' :: ' ~ 
                                                      meta_key ~ ' :: ' ~ 
                                                      database ~ ' :: ' ~ 
                                                      schema ~ ' :: ' ~ 
                                                      model_resource_type, info=true) %}

      {% if model_resource_type|lower in ["model", "snapshot"] %}

        {# This dictionary stores a mapping between materializations in dbt and the objects they will generate in Snowflake  #}
        {% set materialization_map = {"table": "table", "view": "view", "incremental": "table", "snapshot": "table"} %}

        {# Append custom materializations to the list of standard materializations  #}
        {% do materialization_map.update(fromjson(var('custom_materializations_map', '{}'))) %}

        {% set materialization = materialization_map[model.config.get("materialized")] %}
        {% set meta_objects = dbt_snow_mask.get_row_access_meta_objects(model_resource_type, meta_key, true, model_id) %}
        {% do log('APPLY_ROW_ACCESS_POLICY_MODEL c: ' ~ materialization ~ ' :: ' ~ model_id ~ ' :: ' ~ meta_key ~ ' :: ' ~ meta_objects, info=true) %}

        {% set (row_access_policy_db, row_access_policy_schema) = dbt_snow_mask.calculate_database_and_schema_name(database, schema) %}
        {% do log('APPLY_ROW_ACCESS_POLICY_MODEL b: ' ~ row_access_policy_db ~ ' :: ' ~ row_access_policy_schema, info=true) %}

        {% set row_access_policy_list_sql %}     
          show row access policies in {{row_access_policy_db}}.{{row_access_policy_schema}};
          select $3 || '.' || $4 || '.' || $2 as row_access_policy from table(result_scan(last_query_id()));
        {% endset %}

        {# If there are is a row access policy to be applied in this model, we should show the row access policies in the schema #}
        {% if meta_objects | length > 0 %}
          {% do log('APPLY_ROW_ACCESS_POLICY_MODEL d: ' ~ row_access_policy_list_sql, info=true) %}
          {% set row_access_policy_list = dbt_utils.get_query_results_as_dict(row_access_policy_list_sql) %}
          {% do log('APPLY_ROW_ACCESS_POLICY_MODEL e: ' ~ row_access_policy_list, info=true) %}
        {% endif %}

        {%- for meta_tuple in meta_objects if meta_objects | length > 0 %}
          {% do log('APPLY_ROW_ACCESS_POLICY_MODEL f: ' ~ meta_tuple, info=true) %}
          {% set columns = meta_tuple[3] %}
          {% set row_access_policy_name = meta_tuple[2] %}
          {% if row_access_policy_name is not none %}
            {% for row_access_policy_in_db in row_access_policy_list['ROW_ACCESS_POLICY'] %}
              {% do log('APPLY_ROW_ACCESS_POLICY_MODEL h: ' ~ columns ~ ' :: ' ~ row_access_policy_name ~ ' :: ' ~ row_access_policy_in_db ~ ' :: ' ~ row_access_policy_list['ROW_ACCESS_POLICY'], info=true) %}
              {% do log('APPLY_ROW_ACCESS_POLICY_MODEL i: ' ~ row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper ~ ' :: ' ~ row_access_policy_in_db, info=true) %}
              {% if row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ row_access_policy_name|upper == row_access_policy_in_db %}
                {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing row access policy to model  : " ~ 
                                                                                      row_access_policy_db|upper ~ '.' ~ row_access_policy_schema|upper ~ '.' ~ 
                                                                                      row_access_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ alias ~ '.' ~ 
                                                                                      columns ~ ' [force = ' ~ var('use_force_applying_masking_policy','False') ~ ']', info=True) }}
                {# if force is in place, find if there is an existing row access policy on the object that must be replaced #}
                {% if var('use_force_applying_masking_policy','False')|upper in ['TRUE','YES'] %}
                {% set existing_policy_sql %}
                  select policy_db || '.' || policy_schema || '.' || policy_name as row_access_policy
                  from table({{database}}.information_schema.policy_references(ref_entity_name => '{{ database }}.{{ schema }}.{{ alias }}', ref_entity_domain => '{{ materialization }}'))
                  where policy_kind = 'ROW_ACCESS_POLICY';
                {% endset %}
                {% do log('APPLY_ROW_ACCESS_POLICY_MODEL j: ' ~ existing_policy_sql, info=true) %}
                {% set results = run_query(existing_policy_sql) %}
                {% if results %}
                  {% set existing_policy = results.rows[0][0] %}
                {% endif %}
                {% do log('APPLY_ROW_ACCESS_POLICY_MODEL k: ' ~ existing_policy, info=true) %}
                {% endif %}

                {% set query %}
                alter {{materialization}} {{database}}.{{schema}}.{{alias}} 
                  {%- if var('use_force_applying_masking_policy','False')|upper in ['TRUE','YES'] and existing_policy is defined %} drop row access policy {{ existing_policy }}, {%- endif %}
                  add row access policy {{row_access_policy_db}}.{{row_access_policy_schema}}.{{row_access_policy_name}} on (
                    {% for column in columns %} {{ column }} {% if not loop.last %},{% endif %}{% endfor %}
                  );
                {% endset %}
                {% do log('APPLY_ROW_ACCESS_POLICY_MODEL g: ' ~ query, info=true) %}
                {% do run_query(query) %}
              {% endif %}
            {% endfor %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% elif operation_type == "unapply" %}
      {% for node in graph.nodes.values() -%}
        {% set database = node.database | string %}
        {% set schema   = node.schema | string %}
        {% set node_unique_id = node.unique_id | string %}
        {% set node_resource_type = node.resource_type | string %}
        {% set materialization_map = {"table": "table", "view": "view", "incremental": "table", "snapshot": "table"} %}

        {% do log('APPLY_ROW_ACCESS_POLICY_MODEL ua: ' ~ node_unique_id ~ ' :: ' ~ 
                                                         node_resource_type ~ ' :: ' ~ 
                                                         meta_key ~ ' :: ' ~ 
                                                         database ~ ' :: ' ~ 
                                                         schema ~ ' :: ', info=true) %}

        {% if node_resource_type|lower in ["model", "snapshot"] %}
          {# Append custom materializations to the list of standard materializations  #}
          {% do materialization_map.update(fromjson(var('custom_materializations_map', '{}'))) %}

          {% set materialization = materialization_map[node.config.get("materialized")] %}
          {% set alias = node.alias %}

          {% set meta_objects = dbt_snow_mask.get_row_access_meta_objects(node_resource_type, meta_key, true, node_unique_id) %}
          {% do log('APPLY_ROW_ACCESS_POLICY_MODEL uc: ' ~ materialization ~ ' :: ' ~ node_unique_id ~ ' :: ' ~ meta_key ~ ' :: ' ~ meta_objects, info=true) %}
          {%- for meta_tuple in meta_objects if meta_objects | length > 0 %}
            {% set current_policy = meta_tuple[2] %}

            {% do log('APPLY_ROW_ACCESS_POLICY_MODEL ud: ' ~ current_policy ~ ' :: ' ~ meta_tuple, info=true) %}
            {% if current_policy is not none %}
              {% set existing_policy_sql %}
                select policy_db || '.' || policy_schema || '.' || policy_name as row_access_policy
                from table({{database}}.information_schema.policy_references(ref_entity_name => '{{ database }}.{{ schema }}.{{ alias }}', ref_entity_domain => '{{ materialization }}'))
                where policy_kind = 'ROW_ACCESS_POLICY';
              {% endset %}
              {% do log('APPLY_ROW_ACCESS_POLICY_MODEL uj: ' ~ existing_policy_sql, info=true) %}
              {% set results = run_query(existing_policy_sql) %}
              {% if results %}
                {% set existing_policy = results.rows[0][0] %}
              {% endif %}
              {% do log('APPLY_ROW_ACCESS_POLICY_MODEL uk: ' ~ existing_policy, info=true) %}

              {% if existing_policy %}
                {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " ~ operation_type ~ "ing row access policy to model  : " ~ database|upper ~ '.' ~ schema|upper ~ '.' ~ masking_policy_name|upper ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ alias, info=True) }}
                {% set query %}
                    alter {{materialization}} {{database}}.{{schema}}.{{alias}} drop row access policy {{ existing_policy }}
                {% endset %}
                {% do log('APPLY_ROW_ACCESS_POLICY_MODEL ub: ' ~ query, info=true) %}
                {% do run_query(query) %}
              {% endif %}
            {% endif %}
          {% endfor %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endmacro %}
