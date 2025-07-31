{% test stg_sap_wbs_completeness(model) %}
    select *
    from {{ model }}
    where project_tracker_project_id is not null
{% endtest %}