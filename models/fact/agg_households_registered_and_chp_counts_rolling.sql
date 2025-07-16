-- agg_households_registered_and_chp_counts.sql
{{ config(
    materialized = 'table',
    on_schema_change='append_new_columns',
    tags = ['daily_refresh'],
    indexes = [{'columns': ['period_id', 'location_id', 'metric_id', 'snapshot_date']}]
) }}

-- Total registered households
SELECT
  period_id,
  location_id,
  'households_registered' AS metric_id,
  COUNT(DISTINCT household_id) AS value,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_period_household_map_registered') }}
GROUP BY 1, 2

UNION ALL

-- CHPs with at least 1 household
SELECT
  period_id,
  location_id,
  'chps_with_hholds' AS metric_id,
  1 AS value,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS last_updated
FROM (
  SELECT DISTINCT period_id, location_id
  FROM {{ ref('fact_period_household_map_registered') }}
) active_chps
