-- models/fact/fact_aggregate.sql

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['location_id','period_id','metric_id'],
    on_schema_change='ignore',
    tags=['cadence_hourly'],
    indexes=[
        {'columns': ['metric_id']},
        {'columns': ['period_id']},
        {'columns': ['location_id','period_id']},
        {'columns': ['metric_id','location_id']}
    ]
) }}

{# -------------------------------------------------------------------------
   1. Identify which periods to refresh
      This ensures incremental runs process ONLY recent changes.
   ------------------------------------------------------------------------- #}

WITH active_periods AS (
    SELECT period_id
    FROM {{ ref('dim_period') }}
    WHERE end_date >= CURRENT_DATE - interval '400 days'
),

{# -------------------------------------------------------------------------
   2. UNION ALL all fact sources, but filtered to active periods only.
      Every source table must have period_id & location_id columns—which they do.

      NOTE: Filtering each ref() to active periods massively reduces I/O.
   ------------------------------------------------------------------------- #}

all_facts AS (

    SELECT * FROM {{ ref('expected_chps') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_actively_reporting_chps') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_chps_enrolled') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_chps_with_households') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_community_events_participation') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_death_metrics') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_households_registered') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_households_visited_today') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_households_visited_rolling_year') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_households_visited_all_time') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_immunization') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_over_five_metrics_today') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_over_five_metrics_rolling_year') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_over_five_metrics_all_time') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_people_served_today') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_people_served_rolling_year') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_people_served_all_time') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_pnc_newborn') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_population_net') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_pregnancy_metrics') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_referrals_union') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_sha_registration') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)

    UNION ALL
    SELECT * FROM {{ ref('fact_under_five_conditions') }}
      WHERE period_id IN (SELECT period_id FROM active_periods)
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM all_facts

{% if is_incremental() %}
  {{ config(
        incremental_predicates=[
          "period_id IN (SELECT period_id FROM " ~ ref('dim_period') ~ " WHERE end_date >= CURRENT_DATE - interval '400 days')"
        ]
     )
  }}
{% endif %}
