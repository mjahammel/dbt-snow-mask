{% macro create_row_access_policy_ra_hidden_customer(node_database, node_schema) %}

CREATE ROW ACCESS POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.ra_hidden_customer AS (val1 number, val2 number)
  RETURNS boolean ->
    CASE
      WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN true
      WHEN CURRENT_ROLE() NOT IN ('SYSADMIN') AND UNIFORM(1, 10, RANDOM()) != 1 THEN true
      ELSE false
    END
{% endmacro %}