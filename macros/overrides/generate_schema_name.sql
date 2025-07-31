{% macro generate_schema_name(custom_schema_name, node) %}
    {% set dbt_pr_schema = env_var('DBT_CLOUD_PR_SCHEMA', 'none') %}
    {% set env = env_var('DBT_ENV_NAME', 'dev') | lower %}
    {% set dev_user = env_var('DBT_DEV_USER', '') | lower %}

    {{ log("🔍 DBT_ENV_NAME        = " ~ env, info=True) }}
    {{ log("🔍 DBT_DEV_USER       = " ~ dev_user, info=True) }}
    {{ log("🔍 DBT_CLOUD_PR_SCHEMA = " ~ dbt_pr_schema, info=True) }}

    {% if dbt_pr_schema != 'none' %}
        {{ log("✅ Using PR schema: " ~ dbt_pr_schema, info=True) }}
        {{ dbt_pr_schema }}

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
