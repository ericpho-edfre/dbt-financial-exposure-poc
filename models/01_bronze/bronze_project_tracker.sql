-- models/01_bronze/bronze_project_tracker.sql

WITH base_data AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY PROJECT_ID
           ORDER BY LOAD_DATETIME DESC
         ) AS row_num
  FROM {{ source('raw', 'project_tracker_raw') }}
  WHERE LOAD_DATETIME IS NOT NULL
)

SELECT *
FROM base_data
WHERE row_num = 1