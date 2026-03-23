-- models/marts/mart_chp_activity_monthly_national.sql

{{ config(
    materialized = 'table',
    indexes = [
      -- Unique constraint: one row per month x metric (no location dimension)
      {"columns": ["period_start", "metric_id"], "unique": true},
      -- National tables are small; metric_group+metric covers the main panel query
      {"columns": ["metric_group", "metric"]},
      -- Period index supports time-series range queries
      {"columns": ["period_start"]}
    ],
    on_schema_change = 'append_new_columns',
    tags = ['cadence_daily']
) }}

SELECT
  'national' AS level,
  'Kenya' AS name,
  fa.period_start,
  TO_CHAR(fa.period_start, 'FMMonth YYYY') AS month_year,
  dm.group_name AS metric_group,
  dm.metric_group_id AS metric_group_id,
  dm.name AS metric,
  SUM(fa.value) AS value,
  fa.metric_id
FROM {{ ref('fact_over_five_metrics_monthly_trend') }} fa
JOIN {{ ref('dim_metric') }} dm ON fa.metric_id = dm.metric_id
GROUP BY
  fa.period_start,
  fa.metric_id,
  dm.group_name,
  dm.metric_group_id,
  dm.name
