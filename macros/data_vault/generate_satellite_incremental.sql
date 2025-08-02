{% macro generate_satellite_incremental(
    hub_model_name,
    source_model,
    business_keys,
    attributes,
    record_source='"manual"',
    load_date_expr='current_timestamp',
    key_alias='business_hk',
    hashdiff_alias='hashdiff'
) %}
{% set key_expr = dbt_utils.generate_surrogate_key(business_keys) %}
{% set hash_expr = dbt_utils.generate_surrogate_key(attributes) %}

{% if hub_model_name is none %}
    {{ exceptions.raise_compiler_error("❌ Missing required hub_model_name for referential integrity.") }}
{% endif %}

{% set hub_ref = ref(hub_model_name) %}


-- CTE: Source with keys
with source as (
    select
        {{ key_expr }} as {{ key_alias }},
        {% for attr in attributes %}
        {{ attr }},
        {% endfor %}
        {{ hash_expr }} as {{ hashdiff_alias }},
        {{ load_date_expr }} as load_date,
        '{{ record_source }}' as record_source
    from {{ ref(source_model) }}
    where {% for bk in business_keys %}
        {{ bk }} is not null{% if not loop.last %} and {% endif %}
    {% endfor %}
),

-- CTE: Validate against hub
valid_source as (
    select s.*
    from source s
    inner join {{ hub_ref }} h
      on s.{{ key_alias }} = h.{{ key_alias }}
),

{% if is_incremental() %}

-- Latest existing
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

deduplicated as (
    select
        v.{{ key_alias }},
        {% for attr in attributes %}
        v.{{ attr }},
        {% endfor %}
        v.{{ hashdiff_alias }},
        v.load_date,
        v.record_source
    from valid_source v
    left join latest_existing le
      on v.{{ key_alias }} = le.existing_{{ key_alias }}
    where le.{{ hashdiff_alias }} is null or le.{{ hashdiff_alias }} != v.{{ hashdiff_alias }}
)

{% else %}

deduplicated as (
    select
        {{ key_alias }},
        {% for attr in attributes %}
        {{ attr }},
        {% endfor %}
        {{ hashdiff_alias }},
        load_date,
        record_source
    from valid_source
)

{% endif %}

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
