{% snapshot project_tracker_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='project_id',
    strategy='check',
    check_cols='all'  -- or list the columns explicitly
  )
}}

select *
from {{ source('raw', 'project_tracker') }}

{% endsnapshot %}