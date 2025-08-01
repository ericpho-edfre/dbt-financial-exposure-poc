{{ config(
    materialized = 'view',
    database=get_database(),
) }}

-- depends_on: {{ ref('hub_sap_wbs') }}
-- depends_on: {{ ref('hub_sap_project') }}
-- depends_on: {{ ref('hub_project_tracker_project') }}

{{ generate_link_decipher_view('link_wbs_project') }}