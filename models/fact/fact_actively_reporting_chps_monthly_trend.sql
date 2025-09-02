{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_start','metric_id'],
  tags = ['kpi','cadence_weekly'],
  on_schema_change = 'ignore'
) }}

{{
  /*
    Configurable thresholds:
      - active_min_ratio      : fraction e.g. 0.165 (default)
      - active_min_abs_visits : absolute visits threshold e.g. 15 (default)
  */
}}
{% set min_ratio = var('active_min_ratio', 0.165) %}
{% set min_abs = var('active_min_abs_visits', 15) %}
{% set lookback_months = var('hh_lookback_months', 24) %}

WITH denom AS (
  -- monthly registered households (NOT cumulative)
  SELECT location_id, period_start, value AS monthly_registered
  FROM {{ ref('fact_households_registered_monthly_counts') }}
  WHERE metric_id = 'households_registered_monthly'
    AND period_start >= date_trunc('month', CURRENT_DATE) - (interval '{{ lookback_months }} months')
),

num AS (
  -- households visited (monthly)
  SELECT location_id, period_start, value AS monthly_visits
  FROM {{ ref('fact_households_visited_monthly_trend') }}
  WHERE metric_id = 'hh_visited'
    AND period_start >= date_trunc('month', CURRENT_DATE) - (interval '{{ lookback_months }} months')
),

-- Score each CHP-month as active (1) or not (0)
scored AS (
  SELECT
    COALESCE(d.location_id, n.location_id) AS location_id,
    COALESCE(d.period_start, n.period_start)   AS period_start,
    -- Decide active:
    -- 1) if monthly_visits >= absolute threshold -> active
    -- 2) else if monthly_registered > 0 and monthly_visits / monthly_registered >= ratio -> active
    -- 3) else not active
    CASE
      WHEN COALESCE(n.monthly_visits,0) >= {{ min_abs }} THEN 1
      WHEN COALESCE(d.monthly_registered,0) > 0
           AND (COALESCE(n.monthly_visits,0)::float / d.monthly_registered) >= {{ min_ratio }} THEN 1
      ELSE 0
    END AS is_active
  FROM denom d
  FULL JOIN num n
    ON d.location_id = n.location_id
   AND d.period_start = n.period_start
)

SELECT
  location_id,
  period_start,
  'active_chps_new' AS metric_id,
  1 AS value
FROM scored
WHERE is_active = 1
