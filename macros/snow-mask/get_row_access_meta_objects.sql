{% macro get_row_access_meta_objects(resource_type, meta_key, include_optional=false, object_unique_id=none) %}

  {% set row_access_policies = [] %}
  {% do log('GET_ROW_ACCESS_META_OBJECTS h: ' ~ resource_type ~ ' :: ' ~ 
                                                meta_key ~ ' :: ' ~ 
                                                include_optional ~ ' :: ' ~ 
                                                object_unique_id, info=true) %} 

  {% if resource_type.startswith("source") %}
    {% set graph_nodes = graph.sources.values() %}
    {% do log('GET_ROW_ACCESS_META_OBJECTS g:' ~ graph_nodes, info=true) %} 
  {% else %}
    {% set graph_nodes = graph.nodes.values() %}
    {% do log('GET_ROW_ACCESS_META_OBJECTS i:' ~ graph_nodes, info=true) %} 
  {% endif %}

  {% do log('GET_ROW_ACCESS_META_OBJECTS e: ' ~ object_unique_id, info=true) %}
  {% if object_unique_id is not none %}
    {% set include_optional = true %}
  {% endif %}
  
  {% for node in graph_nodes -%}
    {% do log('GET_ROW_ACCESS_META_OBJECTS f: ' ~ object_unique_id ~ ' :: ' ~ node.unique_id, info=true) %}
    {% if object_unique_id is none or object_unique_id == node.unique_id %}
      {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | macro - now processing            : " ~ node.unique_id | string , info=False) }}

      {% set node_database = node.database | string %}
      {% set node_schema = node.schema | string %}
      {% set node_unique_id = node.unique_id | string %}
      {% set node_resource_type = node.resource_type | string %}
      {% do log('GET_ROW_ACCESS_META_OBJECTS c: ' ~ node_database ~ ' :: '
                                                  ~ node_schema ~ ' :: '
                                                  ~ node_unique_id ~ ' :: '
                                                  ~ node_resource_type ~ ' !! '
                                                  ~ node['meta'] ~ ' !! '
                                                  ~ node['config']['meta'], info=true) %}
      {% if node['meta'] | length > 0 %}
        {% do log('GET_ROW_ACCESS_META_OBJECTS a: ' ~ node['meta'], info=true) %}
        {% if include_optional %}
          {% set tuple = (node_database, node_schema, node['meta']['row_access_policy'], node['meta']['row_access_policy_columns'], node_unique_id) %}
        {% else %}
          {% set tuple = (node_database, node_schema, node['meta']['row_access_policy']) %}
        {% endif %}
        {% do row_access_policies.append(tuple) %}
      {% elif node['config']['meta'] | length > 0 %}
        {% do log('GET_ROW_ACCESS_META_OBJECTS b: ' ~ node['config']['meta'], info=true) %}
        {% if include_optional %}
          {% set tuple =  (node_database, node_schema, node['config']['meta']['row_access_policy'], node['config']['meta']['row_access_policy_columns'], node_unique_id) %}
        {% else %}
          {% set tuple =  (node_database, node_schema, node['config']['meta']['row_access_policy']) %}
        {% endif %}
        {% do row_access_policies.append(tuple) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% do log('GET_ROW_ACCESS_META_OBJECTS d: ' ~ row_access_policies, info=true) %}

  {{ return(row_access_policies) }}
{% endmacro %}