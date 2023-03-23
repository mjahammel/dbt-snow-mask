{% macro get_masking_policy_list_for_sources(meta_key) %}

    {% set masking_policies = [] %}

    {% for node in graph.sources.values() -%}

        {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | macro - now processing            : " ~ node.unique_id | string , info=False) }}
        
        {% set node_database = node.database | string %}
        {% set node_schema   = node.schema | string %}
        {% set node_unique_id = node.unique_id | string %}
        {% set node_resource_type = node.resource_type | string %}

        {% do log('GET_MASKING_POLICY_LIST_FOR_SOURCES: ' ~ meta_key ~ ' ' ~ node_database ~ ' ' ~ node_schema ~ ' ' ~ node_unique_id ~ ' ' ~ node_resource_type, info=true) %}
        {% set meta_columns = dbt_snow_mask.get_meta_objects(node_unique_id, meta_key, node_resource_type) %}
    
        {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
            {{ log(modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | macro - meta_columns               : " ~ node_unique_id ~ " has " ~ meta_columns | string ~ " masking tags set", info=False) }}
            
            {% if meta_key == 'masking_policy' %}
                {% set column  = meta_tuple[0] %}
                {% set masking_policy_name = meta_tuple[1] %}
                
                {% if masking_policy_name is not none %}
                    {% set masking_policy_tuple = (node_database, node_schema, masking_policy_name) %}
                    {% do masking_policies.append(masking_policy_tuple) %}
                {% endif %}
            {% else %}
                {% set row_access_policy_name = meta_tuple[1] %}

                {% if row_access_policy_name is not none %}
                    {% set row_access_policy_tuple = (node_database, node_schema, row_access_policy_name) %}
                    {% do masking_policies.append(row_access_policy_tuple) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    
    {% endfor %}

    {% do log('GET_MASKING_POLICY_LIST_FOR_SOURCES <mp> ' ~ masking_policies, info=true) %}
    {{ return(masking_policies) }}

{% endmacro %}