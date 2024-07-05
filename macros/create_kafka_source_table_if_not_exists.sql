{% macro create_kafka_source_table_if_not_exists(table_name, topic_name) %}

{% do log(env_var("KAFKA_BOOTSTRAP_SERVERS", "localhost:9193"), info=True) %}
CREATE TABLE IF NOT EXISTS {{ table_name }}
USING kafka
OPTIONS (
  'kafka.bootstrap.servers' = '{{ env_var("KAFKA_BOOTSTRAP_SERVERS", "localhost:9193") }}',
  'subscribe' = '{{ topic_name }}',
  'startingOffsets' = 'earliest'
);

{% set query %}
DESCRIBE TABLE kafka_source_table;
{% endset %}

{% if execute %}
  {% set results = run_query(query) %}
  {% do log(results.columns, info=True) %}
  {% do log(results.rows, info=True) %}
{% endif %}
{% endmacro %}
