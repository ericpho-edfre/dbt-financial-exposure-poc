-- models/01_bronze/stg_sap_wbs.sql
{{ config(
    materialized='view',
    database=get_database() 
) }}

-- Bronze layer for SAP WBS data
-- This model processes raw SAP WBS data, cleans it, and removes duplicates based on the latest load date.
-- It ensures that only the most recent record for each WBS is retained.
-- Source: raw.SAP_PRPS_RAW


-- TODO : some renaming might not be accurate - to be confirmed which one is being used
with raw_data as (
SELECT *
FROM {{ source('raw', 'SAP_PRPS_RAW') }}
WHERE LOAD_DATE IS NOT NULL
),        

ranked as (
        SELECT
        MANDT as sap_client_id,
        PSPNR as wbs_id,
        POSID as wbs_other_id,
        POST1 as project_short_definition_line1,
        PSPHI as sap_proj_id,
        POSKI as wbs_element_short_identification,
        USR03 as project_tracker_project_id,
        LOAD_DATE,
        ROW_NUMBER() OVER (PARTITION BY MANDT, PSPNR ORDER BY LOAD_DATE DESC NULLS LAST) AS row_num
    FROM raw_data
)

SELECT
    sap_client_id,
    wbs_id,
    wbs_other_id,
    project_short_definition_line1,
    sap_proj_id,
    wbs_element_short_identification,
    project_tracker_project_id,
    LOAD_DATE
FROM ranked
where row_num = 1