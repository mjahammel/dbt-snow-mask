{% macro get_meta_objects(node_unique_id, meta_key, node_resource_type="model") %}
	{% if execute %}

        {% set meta_columns = [] %}
        {% if node_resource_type == "source" %}
            {% if meta_key == "row_access_policy" %}
                {% set columns = graph.sources[node_unique_id]['source_meta'] %}
            {% else %}
                {% set columns = graph.sources[node_unique_id]['columns']  %}
            {% endif %}
        {% else %}
            {% if meta_key == "row_access_policy" %}
                {% set columns = graph.nodes[node_unique_id]['config']['meta'] %}
            {% else %}
                {% set columns = graph.nodes[node_unique_id]['columns']  %}
            {% endif %}
        {% endif %}

        {% if meta_key == "row_access_policy" %}
        {% do log('GET_META_OBJECTS: ' ~ columns, info=true) %}
        {% endif %}

        {% if meta_key is not none %}
            {% if node_resource_type == "source" %}
                {% if meta_key == "row_access_policy" %}
                  {% set row_access_policy = columns['row_access_policy'] %}
                  {% set meta_tuple = (meta_key, row_access_policy, columns['row_access_policy_columns']) %}
                  {% do meta_columns.append(meta_tuple) %}
                  {% do log('GET_META_OBJECTS <ra mt> ' ~ meta_tuple, info=true) %}
                {% else %}
                    {% for column in columns if graph.sources[node_unique_id]['columns'][column]['meta'][meta_key] | length > 0 %}
                        {% set meta_dict = graph.sources[node_unique_id]['columns'][column]['meta'] %}
                        {% for key, value in meta_dict.items() if key == meta_key %}
                            {% set meta_tuple = (column ,value ) %}
                            {% do meta_columns.append(meta_tuple) %}
                        {% endfor %}
                    {% endfor %}
                {% endif %}
            {% else %}
                {% if meta_key == "row_access_policy" %}
                  {% set row_access_policy = columns['row_access_policy'] | string %}
                  {% if row_access_policy | length > 0 %}
                      {% set meta_tuple = (meta_key, row_access_policy, columns[row_access_policy_columns]) %}
                      {% do meta_columns.append(meta_tuple) %}
                  {% endif %}
                {% else %}
                    {% for column in columns if graph.nodes[node_unique_id]['columns'][column]['meta'][meta_key] | length > 0 %}
                        {% set meta_dict = graph.nodes[node_unique_id]['columns'][column]['meta'] %}
                        {% for key, value in meta_dict.items() if key == meta_key %}
                            {% set meta_tuple = (column, value) %}
                            {% do meta_columns.append(meta_tuple) %}
                        {% endfor %}
                    {% endfor %}
                {% endif %}
            {% endif %}
        {% else %}
            {% do meta_columns.append(column|upper) %}
        {% endif %}

        {{ return(meta_columns) }}

    {% endif %}
{% endmacro %}