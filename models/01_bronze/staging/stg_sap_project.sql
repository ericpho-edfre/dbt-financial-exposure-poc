-- models/01_bronze/stg_sap_project.sql
{{ config(
    materialized='view',
    database=get_database() 
) }}

-- Bronze layer for SAP Project data
-- This model processes raw SAP project data, cleans it, and removes duplicates based on the latest load date.
-- It ensures that only the most recent record for each project is retained.
-- Source: raw.SAP_PROJ_RAW

with raw_data as (
SELECT *
FROM {{ source('raw', 'SAP_PROJ_RAW') }}
WHERE LOAD_DATE IS NOT NULL
),

clean_data as (
        SELECT
        MANDT as sap_client_id,
        PSPNR as sap_project_id,
        PSPID as project_definition,
        POST1 as project_short_definition_line1,
        LOAD_DATE,
        ROW_NUMBER() OVER (PARTITION BY MANDT, PSPNR ORDER BY LOAD_DATE DESC NULLS LAST) AS row_num
    FROM raw_data
)

SELECT
    sap_client_id,
    sap_project_id,
    project_definition,
    project_short_definition_line1,
    LOAD_DATE
FROM clean_data
where row_num = 1