{{ config(
    materialized = 'incremental',
    database=get_database(),
    unique_key = 'hub_hk'
) }}

{{ generate_hub(
    source_relation=ref('stg_project_tracker'),
    business_keys=["project_id"],
    source_name="Project tracker"
) }}
