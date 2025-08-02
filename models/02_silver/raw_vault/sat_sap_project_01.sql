{{ config(
    materialized = 'incremental',
    unique_key = ['sap_project_hk', 'load_date']
) }}

{{ generate_satellite_incremental(
    source_model='stg_sap_project',
    business_keys=['sap_client_id','sap_project_id'],
    attributes=[
        'project_definition',
        'project_short_definition_line1'        
    ],
    record_source='"SAP PROJ"',
    load_date_expr='current_timestamp',
    key_alias='sap_project_hk',
    hashdiff_alias='hashdiff'
) }}

