-- models/fact/fact_aggregate.sql
{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_metric_id ON {{ this }} (metric_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_period_id ON {{ this }} (period_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_location_id ON {{ this }} (location_id)"
    ],
    tags = ['cadence_hourly']
) }}

SELECT * FROM {{ ref('expected_chps') }}
union all
SELECT * FROM {{ ref('fact_actively_reporting_chps') }}
union all
SELECT * FROM {{ ref('fact_chps_enrolled') }}
union all
SELECT * FROM {{ ref('fact_chps_with_households') }}
union all
SELECT * FROM {{ ref('fact_community_events_participation') }}
union all
SELECT * FROM {{ ref('fact_death_metrics') }}
union all
SELECT * FROM {{ ref('fact_households_registered') }}
union all
SELECT * FROM {{ ref('fact_households_visited_today') }}
UNION ALL
SELECT * FROM {{ ref('fact_households_visited_rolling_year') }}
UNION ALL
SELECT * FROM {{ ref('fact_households_visited_all_time') }}
union all
SELECT * FROM {{ ref('fact_immunization') }}
union all
select * from {{ ref('fact_over_five_metrics_today') }}
union all
select * from {{ ref('fact_over_five_metrics_rolling_year') }}
union all
select * from {{ ref('fact_over_five_metrics_all_time') }}
union all
select * from {{ ref('fact_people_served_today') }}
union all
select * from {{ ref('fact_people_served_rolling_year') }}
union all
select * from {{ ref('fact_people_served_all_time') }}
union all
SELECT * FROM {{ ref('fact_pnc_newborn') }}
union all
SELECT * FROM {{ ref('fact_population_net') }}
union all
SELECT * FROM {{ ref('fact_pregnancy_metrics') }}
union all
select * from {{ ref('fact_referrals_union') }}
union all
SELECT * FROM {{ ref('fact_sha_registration') }}
union all
SELECT * FROM {{ ref('fact_under_five_conditions') }}