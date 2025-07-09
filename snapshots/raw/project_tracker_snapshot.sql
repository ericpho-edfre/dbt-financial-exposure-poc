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

select PROJECT_ID,
        PROJECT_NAME,
        LATITUDE,
        LONGITUDE,
        IS_PROJECT_EDF_OWNED,
        DEVELOPMENT_REGION,
        CURRENT_PROJECT_STATUS,
        TECHNOLOGY_CATEGORY,
        CONTRACTEDCAPACITYMWDC,
        CONTRACTEDCAPACITYMWAC,
        POICAPACITYMWAC,
        COD_DATE,
        EARLIEST_COD_DATE
from {{ source('raw', 'project_tracker') }}

{% endsnapshot %}