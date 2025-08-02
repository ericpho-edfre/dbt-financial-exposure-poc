{{ config(
    materialized = 'incremental',
    unique_key = ['project_tracker_project_hk', 'load_date']
) }}

{{ generate_satellite_incremental(
    source_model='stg_project_tracker',
    business_keys=['project_id'],
    attributes=[
        'project_name',
        'latitude',
        'longitude',
        'is_project_edf_owned',
        'development_region',
        'current_project_status',
        'technology_category',
        'contracted_capacity_mw_dc',
        'contracted_capacity_mw_ac',
        'poi_capacity_mw_ac',
        'cod_date',
        'earliest_cod_date'
    ],
    record_source='"project_tracker_raw"',
    load_date_expr='current_timestamp',
    key_alias='project_tracker_project_hk',
    hashdiff_alias='hashdiff'
) }}
