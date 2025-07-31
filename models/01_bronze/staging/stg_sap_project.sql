-- models/01_bronze/bronze_project_tracker.sql
{{ config(
    materialized='view',
    database=get_database() 
) }}

with raw_data as (
SELECT *
FROM {{ source('raw', 'SAP_PROJ_RAW') }}
WHERE LOAD_DATE IS NOT NULL
),

clean_data as (
        SELECT
        MANDT as client_id,
        PSPNR as project_definition_internal,
        PSPID as project_definition,
        POST1 as project_short_definition_line1,
        LOAD_DATE,
        ROW_NUMBER() OVER (PARTITION BY MANDT, PSPNR ORDER BY LOAD_DATE DESC NULLS LAST) AS row_num
    FROM raw_data
)

SELECT
client_id,
project_definition_internal,
project_definition,
project_short_definition_line1,
LOAD_DATE
FROM clean_data
where row_num = 1