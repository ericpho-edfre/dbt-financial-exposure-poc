{% macro generate_schema_name(custom_schema_name, node) %}
    {% set is_pr = env_var('DBT_CLOUD_PR_ID', 'none') != 'none' %}
    {% set env = env_var('DBT_ENV_NAME', 'dev') | lower %}
    {% set dev_user = env_var('DBT_DEV_USER', '') | lower %}

    {% if is_pr %}
        {{ custom_schema_name }}  {# Let dbt Cloud set the dynamic schema like dbt_cloud_pr_xxx #}

    {% elif env == 'dev' %}
        {% if dev_user in ['', 'default'] %}
            {{ exceptions.raise_compiler_error("Missing DBT_DEV_USER in DEV environment.") }}
        {% endif %}
        {{ 'DEV_' ~ dev_user ~ '_' ~ custom_schema_name }}

    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}
