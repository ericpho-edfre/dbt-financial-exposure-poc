{% macro get_database() %}
  {{ return(env_var('DBT_DATABASE')) }}
{% endmacro %}