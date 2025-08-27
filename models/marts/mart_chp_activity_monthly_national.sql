-- models/marts/mart_chp_activity_monthly_national.sql

{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["period_id", "metric_id"], "unique": true},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["metric_group", "metric"]},
      {"columns": ["period_label"]}
    ],
    tags=['cadence_weekly']
) }}

SELECT
  'national' AS level,
  'Kenya' AS name,
  fa.period_start,
  TO_CHAR(DATE fa.period_start, 'FMMonth YYYY') AS month_year,
  dm.group_name AS metric_group,
  dm.metric_group_id AS metric_group_id,
  dm.name AS metric,
  SUM(fa.value) AS value,
  fa.metric_id,
FROM {{ ref('fact_aggregate') }} fa
JOIN {{ ref('dim_metric') }} dm ON fa.metric_id = dm.metric_id
GROUP BY
  fa.period_start,
  fa.metric_id,
  m.group_name,
  m.metric_group_id,
  m.name
