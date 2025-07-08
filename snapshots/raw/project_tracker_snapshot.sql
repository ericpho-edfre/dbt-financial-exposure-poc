{% snapshot project_tracker_snapshot %}

{{
  config(
    unique_key='project_id',
    strategy='check',
    check_cols='all'
  )
}}

select *
from {{ source('raw', 'project_tracker') }}

{% endsnapshot %}