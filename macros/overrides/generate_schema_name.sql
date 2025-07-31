{% macro generate_schema_name(custom_schema_name, node) %}
    {% set env = env_var('DBT_ENV_NAME', 'dev') | lower %}
    {% set user = env_var('DBT_DEV_USER', '') | lower %}

    {% if env == 'dev' %}
        {% if user in ['', 'default'] %}
            {{ exceptions.raise_compiler_error("Missing DBT_DEV_USER in DEV environment. Please set it in your developer credentials.") }}
        {% endif %}
        {{ 'DEV_' ~ user ~ '_' ~ custom_schema_name }}
    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}