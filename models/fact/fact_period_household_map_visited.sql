{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'household_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_visits AS (
  SELECT
    hhvisit.household AS household_id,
    hhvisit.reported_by_parent AS location_id,
    DATE(hhvisit.reported) AS report_date
  FROM {{ ref('household_visit') }} hhvisit
  WHERE hhvisit.household IS NOT NULL
)

SELECT
  p.period_id AS period_id,
  b.location_id,
  'hh_visited' AS metric_id,
  b.household_id
FROM base_visits b
JOIN periods p ON b.report_date BETWEEN p.start_date AND p.end_date
