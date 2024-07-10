{% macro retrieve_max_value(table, column, fallback) %}
  {% set result = run_query("SELECT COALESCE(MAX(" ~ column ~ "), '" ~ fallback ~ "') AS max_value FROM " ~ table) %}
  {{ result.columns[0].values()[0] }}
{% endmacro %}
