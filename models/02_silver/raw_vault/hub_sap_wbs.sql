{{ config(
    materialized = 'incremental',
    database=get_database(),
    unique_key = 'sap_wbs_hk'
) }}

{{ generate_hub(
    source_relation=ref('stg_sap_wbs'),
    business_keys=["sap_client_id","wbs_id","sap_proj_id"],
    source_name="SAP PRPS"
) }}
