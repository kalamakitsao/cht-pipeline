-- models/metrics/mv_aggregate_national_metrics_summary.sql

{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["period_id", "metric_id"], "unique": true, "name": "idx_mv_national_unique_period_metric"},
      {"columns": ["period_start", "period_end"], "name": "idx_mv_national_period_dates"},
      {"columns": ["metric_group", "metric"], "name": "idx_mv_national_metric_group"},
      {"columns": ["period_label"], "name": "idx_mv_national_period_label"}
    ]
) }}

SELECT
  'national' AS level,
  'Kenya' AS name,
  p.start_date AS period_start,
  p.end_date AS period_end,
  p.label AS period_label,
  m.group_name AS metric_group,
  m.name AS metric,
  SUM(f.value) AS value,
  f.period_id,
  f.metric_id,
  CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_aggregate') }} f
JOIN {{ ref('dim_period') }} p ON f.period_id = p.period_id
JOIN {{ ref('dim_metric') }} m ON f.metric_id = m.metric_id
GROUP BY
  f.period_id,
  f.metric_id,
  p.start_date,
  p.end_date,
  p.label,
  m.group_name,
  m.name
