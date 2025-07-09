{% snapshot project_tracker_snapshot %}

{{
  config(
    unique_key='PROJECT_ID',
    strategy='check',
    check_cols=[
      'PROJECT_NAME',
      'LATITUDE',
      'LONGITUDE',
      'IS_PROJECT_EDF_OWNED',
      'DEVELOPMENT_REGION',
      'CURRENT_PROJECT_STATUS',
      'TECHNOLOGY_CATEGORY',
      'CONTRACTEDCAPACITYMWDC',
      'CONTRACTEDCAPACITYMWAC',
      'POICAPACITYMWAC',
      'COD_DATE',
      'EARLIEST_COD_DATE'
      ]
  )
}}

{# List of unknown columns : 
      -- 'PROJECT_STAGE' - Not found
      -- 'PROJECT_TIER' - Not found
      -- 'Stage Gate' - Not found
      -- 'Project Tier' - Not found
      -- 'BESSCapacity',
      -- 'BESSEnergy',
      -- 'LatestCOD' - Not found
      -- 'FID' - Not found
#}

WITH base_data AS (
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
        LOAD_DATETIME,
        ROW_NUMBER() OVER (PARTITION BY PROJECT_ID ORDER BY LOAD_DATETIME DESC) AS row_num
    FROM {{ source('raw', 'project_tracker_raw') }}
)

SELECT *
FROM base_data
WHERE row_num = 1

{% endsnapshot %}