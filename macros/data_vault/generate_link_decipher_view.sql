{% macro generate_link_decipher_view(model_name, link_hashkeys) %}
    {%- set link_relation = ref(model_name) -%}

    {%- set join_clauses = [] -%}
    {%- set select_clauses = ['l.link_hk'] -%}

    {%- for hk in link_hashkeys %}
        {%- set hub_prefix = hk[:-3] -%}  {# Remove '_hk' #}
        {%- set hub_model_name = 'hub_' ~ hub_prefix -%}
        {%- set hub_ref = ref(hub_model_name) -%}
        {%- set alias = hub_model_name -%}

        {# JOIN clause #}
        {%- do join_clauses.append("left join " ~ hub_ref ~ " as " ~ alias ~ " on l." ~ hk ~ " = " ~ alias ~ "." ~ hk) -%}

        {# SELECT business keys (exclude hashkey, load_date, record_source) #}
        {%- set excluded_cols = [hk, 'load_date', 'record_source'] -%}
        {%- set hub_cols = adapter.get_columns_in_relation(hub_ref) -%}

        {%- for col in hub_cols %}
            {%- if col.name not in excluded_cols and not col.name.endswith('_hk') -%}
                {%- set alias_col = hub_prefix ~ '__' ~ col.name -%}
                {%- do select_clauses.append(alias ~ "." ~ col.name ~ " as " ~ alias_col) -%}
            {%- endif -%}
        {%- endfor %}
    {%- endfor %}

    with link_base as (
        select * from {{ link_relation }} where is_active = true
    )

    select
        {{ select_clauses | join(',\n        ') }}
    from link_base l
    {% for join in join_clauses %}
        {{ join }}
    {% endfor %}
{% endmacro %}
