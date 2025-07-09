-- models/01_bronze/bronze_project_tracker.sql

SELECT *
FROM {{ source('raw', 'project_tracker_raw') }}
WHERE LOAD_DATETIME IS NOT NULL
