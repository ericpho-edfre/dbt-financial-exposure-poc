{% macro generate_satellite_incremental(
    source_model,
    business_key,
    attributes,
    record_source='"manual"',
    load_date_expr='current_timestamp',
    key_alias='business_hk',
    hashdiff_alias='hashdiff'
) %}
{# Create hashdiff from descriptive attributes #}
{% set hash_expr = dbt_utils.generate_surrogate_key(attributes) %}

-- CTE: Source records with generated keys and hashdiff
with source as (
    select
        {{ dbt_utils.generate_surrogate_key([business_key]) }} as {{ key_alias }},
        {% for attr in attributes %}
        {{ attr }},
        {% endfor %}
        {{ hash_expr }} as {{ hashdiff_alias }},
        {{ load_date_expr }} as load_date,
        '{{ record_source }}' as record_source
    from {{ ref(source_model) }}
    where {{ business_key }} is not null
),

{% if is_incremental() %}

-- CTE: Latest existing records per business key
latest_existing as (
    select
        {{ key_alias }} as existing_{{ key_alias }},
        {{ hashdiff_alias }}
    from (
        select
            {{ key_alias }},
            {{ hashdiff_alias }},
            row_number() over (partition by {{ key_alias }} order by load_date desc) as rn
        from {{ this }}
    ) le
    where rn = 1
),

-- CTE: Filter only new or changed records
deduplicated as (
    select
        s.{{ key_alias }},
        {% for attr in attributes %}
        s.{{ attr }},
        {% endfor %}
        s.{{ hashdiff_alias }},
        s.load_date,
        s.record_source
    from source s
    left join latest_existing le
      on s.{{ key_alias }} = le.existing_{{ key_alias }}
    where le.{{ hashdiff_alias }} is null or le.{{ hashdiff_alias }} != s.{{ hashdiff_alias }}
)

{% else %}

-- Full load case
deduplicated as (
    select
        {{ key_alias }},
        {% for attr in attributes %}
        {{ attr }},
        {% endfor %}
        {{ hashdiff_alias }},
        load_date,
        record_source
    from source
)

{% endif %}

-- Final output (no ambiguous select *)
select
    {{ key_alias }},
    {% for attr in attributes %}
    {{ attr }},
    {% endfor %}
    {{ hashdiff_alias }},
    load_date,
    record_source
from deduplicated
{% endmacro %}
