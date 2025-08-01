{{ config(
    materialized = 'incremental',
    database=get_database(),
    unique_key = 'hub_hk'
) }}

{{ generate_hub(
    source_relation=ref('stg_sap_project'),
    business_keys=["sap_client_id","sap_project_id"],
    source_name="SAP PROJ"
) }}
