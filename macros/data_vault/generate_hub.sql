{% macro generate_hub(source_relation, business_keys, source_name, add_unknown_record=True) %}

{# Infer the hashkey alias from the model name, dropping 'hub_' prefix #}
{% set model_name = this.identifier | lower %}
{% set concept_name = model_name.replace('hub_', '') %}
{% set hashkey_alias = concept_name ~ '_hk' %}

{%- set unknown_list = [] -%}
{%- for col in business_keys -%}
    {%- set _ = unknown_list.append("'UNKNOWN'") -%}
{%- endfor -%}

with source as (

    select
        {{ dbt_utils.generate_surrogate_key(business_keys) }} as {{ hashkey_alias }},
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
        {{ hashkey_alias }},
        {% for col in business_keys %}
        {{ col }},
        {% endfor %}
        load_date,
        record_source
    from source

    {% if is_incremental() %}
    where {{ hashkey_alias }} not in (select {{ hashkey_alias }} from {{ this }})
    {% endif %}

)

{% if add_unknown_record %}
,
unknown_record as (
    select 
        {{ dbt_utils.generate_surrogate_key(unknown_list) }} as {{ hashkey_alias }},
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
    select {{ hashkey_alias }} from {{ this }}
)
{% endif %}
{% endif %}

{% endmacro %}
