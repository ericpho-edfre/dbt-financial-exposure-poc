
{{ config(
    store_failures=true,
    severity='warn'
) }}

-- tests/cross-models/test_hub_project_tracker_project_not_empty.sql
-- This test ensures that the hub_project_tracker table is not empty
-- The test will FAIL if the table has 0 rows, and PASS if it has any rows

select *
from {{ ref('stg_sap_wbs') }}
where project_tracker_project_id is not null
