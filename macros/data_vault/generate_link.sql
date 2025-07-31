{% macro generate_link(
    source_model,
    left_key,
    right_key,
    left_hub,
    right_hub,
    link_name,
    record_source='"manual"',
    load_date_expr='current_timestamp'
) %}

{#
  source_model:     staging model with business keys 
  left_key:         e.g., 'customer_id'
  right_key:        e.g., 'order_id'
  left_hub:         e.g., 'hub_customer'
  right_hub:        e.g., 'hub_order'
  link_name:        e.g., 'link_customer_order'
  record_source:    optional, defaults to 'manual'
  load_date_expr:   optional, defaults to current_timestamp
#}

{% set left_hk = left_key | replace('_id', '') ~ '_hk' %}
{% set right_hk = right_key | replace('_id', '') ~ '_hk' %}

with staged as (
    select
        {{ dbt_utils.generate_surrogate_key([left_key]) }} as {{ left_hk }},
        {{ dbt_utils.generate_surrogate_key([right_key]) }} as {{ right_hk }},
        {{ dbt_utils.generate_surrogate_key([left_key, right_key]) }} as {{ link_name }}_hk,
        {{ load_date_expr }} as load_date,
        {{ record_source }} as record_source
    from {{ ref(source_model) }}
    where {{ left_key }} is not null and {{ right_key }} is not null
),

validated as (
    select s.*
    from staged s
    inner join {{ ref(left_hub) }} lh on s.{{ left_hk }} = lh.{{ left_hk }}
    inner join {{ ref(right_hub) }} rh on s.{{ right_hk }} = rh.{{ right_hk }}
)

select * from validated

{% endmacro %}
