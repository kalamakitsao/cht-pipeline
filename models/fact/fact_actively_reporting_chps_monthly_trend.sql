{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_start','metric_id'],
  tags = ['kpi','cadence_weekly'],
  on_schema_change = 'ignore'
) }}

{% set min_ratio = var('active_min_ratio', 0.165) %}

WITH denom AS (
  -- monthly registered households (NOT cumulative)
  SELECT location_id, period_start, value AS monthly_registered
  FROM {{ ref('fact_households_registered_monthly_trend') }}
),

num AS (
  -- households visited (monthly)
  SELECT location_id, period_start, value AS monthly_visits
  FROM {{ ref('fact_households_visited_monthly_trend') }}
),

-- Score each CHP-month as active (1) or not (0)
scored AS (
  SELECT
    d.location_id,
    d.period_start,
    -- Decide active:
    -- 1) if monthly_visits >= absolute threshold -> active
    -- 2) else if monthly_registered > 0 and monthly_visits / monthly_registered >= ratio -> active
    -- 3) else not active
    CASE
      WHEN COALESCE(d.monthly_registered,0) > 0
           AND (COALESCE(n.monthly_visits,0)::float / d.monthly_registered) >= {{ min_ratio }} THEN 1
      ELSE 0
    END AS is_active
  FROM denom d
  LEFT JOIN num n
    ON d.location_id = n.location_id
   AND d.period_start = n.period_start
)

SELECT
  location_id,
  period_start,
  'active_chps_new' AS metric_id,
  1 AS value
FROM scored
WHERE is_active = 1 AND period_start='2025-09-01';
