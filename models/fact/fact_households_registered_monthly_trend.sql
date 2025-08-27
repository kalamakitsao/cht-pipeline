-- models/fact/metrics/fact_households_registered_monthly_trend.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','chp_compliance'],
  on_schema_change = 'ignore'
) }}

WITH months AS (
    -- All months from Jan 2020 up to the current month
    SELECT generate_series(
        date '2020-01-01',
        date_trunc('month', CURRENT_DATE),
        interval '1 month'
    )::date AS period_start
),
households AS (
    SELECT
        chv_area_id AS location_id,
        date_trunc('month', reported)::date AS period_start,
        COUNT(uuid) AS households_registered
    FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }}
    WHERE reported >= '2020-01-01'
    GROUP BY chv_area_id, date_trunc('month', reported)
),
all_combinations AS (
    -- Cross join all months and locations so we have a complete grid
    SELECT DISTINCT l.location_id, m.period_start
    FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} l
    CROSS JOIN months m
),
filled AS (
    -- Join with actual registrations
    SELECT
        ac.location_id,
        ac.period_start,
        COALESCE(h.households_registered, 0) AS households_registered
    FROM all_combinations ac
    LEFT JOIN households h
      ON ac.location_id = h.location_id
     AND ac.period_start = h.period_start
),
cumulative AS (
    -- Carry forward the cumulative total per location_id
    SELECT
        location_id,
        period_start,
        SUM(households_registered) OVER (
            PARTITION BY location_id ORDER BY period_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS value
    FROM filled
)
SELECT
    location_id,
    period_start,
    'households_registered' AS metric_id,
    value
FROM cumulative
ORDER BY location_id, period_start;
