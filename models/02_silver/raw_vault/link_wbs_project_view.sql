{{ config(
    materialized = 'view',
    database=get_database(),
) }}

{{ generate_link_decipher_view(
    model_name = 'link_wbs_project',
    link_hashkeys = ['sap_wbs_hk', 'sap_project_hk', 'project_tracker_project_hk']
) }}