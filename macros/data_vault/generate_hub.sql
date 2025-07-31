{% macro generate_hub(source_relation, business_keys, source_name) %}

with source as (

    select
        {{ dbt_utils.generate_surrogate_key(business_keys) }} as hub_hk,
        {% for col in business_keys %}
        {{ col }},
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

select * from deduplicated

{% endmacro %}
