-- models/fact/fact_aggregate.sql

{{ config(
    materialized = 'table',
    on_schema_change = 'append_new_columns',
    tags = ['cadence_hourly'],
    indexes = [
        {'columns': ['metric_id']},
        {'columns': ['period_id']},
        {'columns': ['location_id','period_id']},
        {'columns': ['metric_id','location_id']}
    ]
) }}

WITH all_facts AS (

    SELECT * FROM {{ ref('expected_chps') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_actively_reporting_chps') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_chps_enrolled') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_chps_with_households') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_community_events_participation') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_death_metrics') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_households_registered') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_households_visited_today') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_households_visited_rolling_year') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_households_visited_all_time') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_immunization') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_over_five_metrics_today') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_over_five_metrics_rolling_year') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_over_five_metrics_all_time') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_people_served_today') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_people_served_rolling_year') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_people_served_all_time') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_pnc_newborn') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_population_net') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_pregnancy_metrics') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_referrals_union') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_sha_registration') }}

    UNION ALL
    SELECT * FROM {{ ref('fact_under_five_conditions') }}
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM all_facts
WHERE value IS NOT NULL
