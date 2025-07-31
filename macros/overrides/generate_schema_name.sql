{% macro generate_schema_name(custom_schema_name, node) %}
    {% set env_name = env_var('DBT_ENV_NAME', 'DEV') | lower %}
    {% set dev_user = env_var('DBT_DEV_USER', '') | lower %}

    {% if env_name not in ['PROD', 'QUAL'] %}
        {% if dev_user in ['', 'default'] %}
            {{ exceptions.raise_compiler_error("Missing required environment variable: DBT_DEV_USER. Each developer must set this in their dbt Cloud profile.") }}
        {% endif %}
        {{ 'DEV_' ~ dev_user ~ '_' ~ custom_schema_name }}
    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}