{{ config(
    materialized = 'table',
    on_schema_change='append_new_columns',
    tags = ['daily_refresh'],
    indexes = [{'columns': ['period_id', 'location_id', 'metric_id', 'snapshot_date']}]
) }}

SELECT
  period_id,
  location_id,
  metric_id,
  COUNT(DISTINCT household_id) AS value,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_period_household_map_sha') }}
GROUP BY 1, 2, 3
