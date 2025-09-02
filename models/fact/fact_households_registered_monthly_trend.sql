-- models/marts/facts/fact_households_registered_monthly_base.sql

{{ config(
    materialized = "table"
) }}

-- Determine min and max months, build full month series from first-ever registration to current month,
-- compute per-location monthly counts, then cumulative across the full history, finally emit last 12 months.
WITH bounds AS (
  SELECT
    COALESCE(date_trunc('month', MIN(reported))::date, date_trunc('month', CURRENT_DATE)::date) AS min_month,
    date_trunc('month', CURRENT_DATE)::date AS max_month
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }}
),

months AS (
  SELECT generate_series(min_month, max_month, interval '1 month')::date AS period_start
  FROM bounds
),

locations AS (
  SELECT DISTINCT chv_area_id AS location_id
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }}
),

-- Raw counts per month/location for the entire history
households AS (
  SELECT
    hv.chv_area_id AS location_id,
    date_trunc('month', hv.reported)::date AS period_start,
    COUNT(hv.uuid) AS households_registered
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} hv
  WHERE hv.reported IS NOT NULL
  GROUP BY 1,2
),

-- full grid: every location x every month from first registration to current month
grid AS (
  SELECT
    l.location_id,
    m.period_start
  FROM locations l
  CROSS JOIN months m
),

-- align counts to the grid (0 when none that month)
aligned AS (
  SELECT
    g.location_id,
    g.period_start,
    COALESCE(h.households_registered, 0) AS households_registered
  FROM grid g
  LEFT JOIN households h
    ON g.location_id = h.location_id
   AND g.period_start = h.period_start
),

-- compute cumulative across the entire history (so carry-forward from earliest month)
cumulative_all AS (
  SELECT
    location_id,
    period_start,
    SUM(households_registered) OVER (
      PARTITION BY location_id
      ORDER BY period_start
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS households_registered_cumulative
  FROM aligned
)

-- Emit only the last 12 months of the cumulative series
SELECT
  location_id,
  period_start,
  'households_registered' as metric_id,
  households_registered_cumulative as value
FROM cumulative_all
WHERE period_start >= date_trunc('month', CURRENT_DATE) - interval '11 months'
ORDER BY location_id, period_start
