-- models/01_bronze/stg_project_tracker.sql
{{ config(
    materialized='view',
    database=get_database() 
) }}


-- Bronze layer for Project Tracker data
-- This model processes raw project tracker data, cleans it, and removes duplicates based on the latest load date.
-- It ensures that only the most recent record for each project is retained.
-- The model handles NULL values and converts date fields appropriately.
-- Source: raw.project_tracker_raw

with raw_data as (
SELECT *
FROM {{ source('raw', 'project_tracker_raw') }}
WHERE LOAD_DATE IS NOT NULL
),

clean_data as (
        SELECT
        PROJECT_ID,
        NULLIF(PROJECT_NAME,'NULL') as PROJECT_NAME,
        LATITUDE,
        LONGITUDE,
        IS_PROJECT_EDF_OWNED,
        NULLIF(DEVELOPMENT_REGION,'NULL') as DEVELOPMENT_REGION,
        NULLIF(CURRENT_PROJECT_STATUS,'NULL') as CURRENT_PROJECT_STATUS,
        NULLIF(TECHNOLOGY_CATEGORY,'NULL') as TECHNOLOGY_CATEGORY,
        NULLIF(CONTRACTEDCAPACITYMWDC, 0) as CONTRACTEDCAPACITYMWDC,
        NULLIF(CONTRACTEDCAPACITYMWAC, 0) as CONTRACTEDCAPACITYMWAC,
        NULLIF(POICAPACITYMWAC,0) as POICAPACITYMWAC,
        CASE
            WHEN UPPER(COD_DATE) = 'NULL' THEN NULL
            ELSE TRY_CAST(COD_DATE AS DATE)
        END AS COD_DATE,
        CASE
            WHEN UPPER(EARLIEST_COD_DATE) = 'NULL' THEN NULL
            ELSE TRY_CAST(EARLIEST_COD_DATE AS DATE)
        END AS EARLIEST_COD_DATE,
        LOAD_DATE,
        ROW_NUMBER() OVER (PARTITION BY PROJECT_ID ORDER BY LOAD_DATE DESC NULLS LAST) AS row_num
    FROM raw_data
)

SELECT PROJECT_ID as project_id,
    PROJECT_NAME as project_name,
    LATITUDE as latitude,
    LONGITUDE as longitude,
    IS_PROJECT_EDF_OWNED as is_project_edf_owned,
    DEVELOPMENT_REGION as development_region,
    CURRENT_PROJECT_STATUS as current_project_status,
    TECHNOLOGY_CATEGORY as technology_category,
    CONTRACTEDCAPACITYMWDC as contracted_capacity_mw_dc,
    CONTRACTEDCAPACITYMWAC as contracted_capacity_mw_ac,
    POICAPACITYMWAC as poi_capacity_mw_ac,
    COD_DATE as cod_date,
    EARLIEST_COD_DATE as earliest_cod_date,
    LOAD_DATE as load_date
    from clean_data
    where row_num = 1
