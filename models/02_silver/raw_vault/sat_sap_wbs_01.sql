{{ config(
    materialized = 'incremental',
    unique_key = ['sap_wbs_hk', 'load_date']
) }}

{{ generate_satellite_incremental(
    source_model='stg_sap_wbs',
    business_keys=['sap_client_id','wbs_id'],
    attributes=[
        'wbs_other_id',
        'wbs_short_definition_line1',
        'sap_proj_id',
        'wbs_element_short_identification',
        'project_tracker_project_id',
        'project_hierarchy_level'
    ],
    record_source='"SAP PRPS"',
    load_date_expr='current_timestamp',
    key_alias='sap_wbs_hk',
    hashdiff_alias='hashdiff'
) }}

