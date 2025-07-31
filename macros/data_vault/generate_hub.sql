{% macro generate_hub(source_relation, business_keys, source_name, add_unknown_record=True) %}

{%- set unknown_list = [] -%}
{%- for col in business_keys -%}
    {%- set _ = unknown_list.append("'UNKNOWN'") -%}
{%- endfor -%}

with source as (

    select
        {{ dbt_utils.generate_surrogate_key(business_keys) }} as hub_hk,
        {% for col in business_keys %}
        cast({{ col }} as varchar) as {{ col }},
        {% endfor %}
        current_timestamp as load_date,
        '{{ source_name }}' as record_source

    from {{ source_relation }}
    where {% for col in business_keys %}
        {{ col }} is not null {% if not loop.last %} and {% endif %}
    {% endfor %}

),

deduplicated as (

    select distinct 
        hub_hk,
        {% for col in business_keys %}
        {{ col }},
        {% endfor %}
        load_date,
        record_source
    from source

    {% if is_incremental() %}
    where hub_hk not in (select hub_hk from {{ this }})
    {% endif %}

)

{% if add_unknown_record %}
,
unknown_record as (
    select 
        {{ dbt_utils.generate_surrogate_key(unknown_list) }} as hub_hk,
        {% for col in business_keys %}
        'UNKNOWN' as {{ col }},
        {% endfor %}
        current_timestamp as load_date,
        'SYSTEM' as record_source
)
{% endif %}

select * from deduplicated

{% if add_unknown_record %}
union all
select * from unknown_record
{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key(unknown_list) }} not in (
    select hub_hk from {{ this }}
)
{% endif %}
{% endif %}

{% endmacro %}
