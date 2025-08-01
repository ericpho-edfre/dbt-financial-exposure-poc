{% macro generate_link_decipher_view(model_name) %}
    {%- set full_model_name = 'model.' ~ project_name ~ '.' ~ model_name -%}
    {%- set model_node = graph.nodes[full_model_name] -%}
    {%- set link_relation = ref(model_name) -%}

    {%- set join_clauses = [] -%}
    {%- set select_clauses = ['l.link_hk'] -%}

    {%- for column_name in model_node.columns.keys() -%}
        {%- if column_name.endswith('_hub_hk') -%}
            {%- set hub_prefix = column_name[:-7] -%}  {# correct: remove '_hub_hk' (7 chars) #}
            {%- set hub_model_name = 'hub_' ~ hub_prefix -%}
            {%- set hub_node_name = 'model.' ~ project_name ~ '.' ~ hub_model_name -%}
            {%- set hub_node = graph.nodes.get(hub_node_name) -%}
            {%- if hub_node is none -%}
                {% do exceptions.raise_compiler_error("Hub model '" ~ hub_model_name ~ "' not found in graph.") %}
            {%- endif -%}

            {%- set hub_ref = ref(hub_model_name) -%}
            {%- set alias = hub_model_name -%}

            {# JOIN clause #}
            {%- do join_clauses.append("left join " ~ hub_ref ~ " as " ~ alias ~ " on " ~ alias ~ ".hub_hk = l." ~ column_name) -%}

            {# SELECT business keys from hub, excluding standard fields #}
            {%- for hub_col in hub_node.columns.keys() -%}
                {%- if hub_col not in ['hub_hk', 'load_date', 'record_source'] -%}
                    {%- set prefixed_name = hub_model_name ~ '__' ~ hub_col -%}
                    {%- do select_clauses.append(alias ~ "." ~ hub_col ~ " as " ~ prefixed_name) -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}

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
