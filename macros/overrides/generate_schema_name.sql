{% macro generate_schema_name(custom_schema_name, node) %}
    {% set env = env_var('DBT_ENV_NAME', 'dev') | lower %}
    {% set dev_user = env_var('DBT_DEV_USER', '') | lower %}
    {% set schema_override = target.schema %}

    {% if schema_override.startswith('dbt_cloud_pr_') %}
        {{ log("✅ Using PR schema from target.schema: " ~ schema_override, info=True) }}
        {{ log("🔍 Target database: " ~ target.database, info=True) }}
        {{ schema_override }}

    {% elif env == 'dev' %}
        {% if dev_user in ['', 'default'] %}
            {{ exceptions.raise_compiler_error("❌ Missing required DBT_DEV_USER in DEV environment.") }}
        {% endif %}
        {% set dev_schema = 'DEV_' ~ dev_user ~ '_' ~ custom_schema_name %}
        {{ log("✅ Using DEV schema: " ~ dev_schema, info=True) }}
        {{ dev_schema }}

    {% else %}
        {{ log("✅ Using standard schema: " ~ custom_schema_name, info=True) }}
        {{ custom_schema_name }}
    {% endif %}
{% endmacro %}
