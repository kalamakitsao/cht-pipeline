{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'chp_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

chps AS (
  SELECT
    location_id,
    location_id AS chp_id  -- Use as proxy for enrolled CHP area
  FROM {{ ref('dim_location') }}
  WHERE level = 'chp area'
)

SELECT
  p.period_id AS period_id,
  c.location_id,
  'chps_enrolled' AS metric_id,
  c.chp_id
FROM chps c
CROSS JOIN periods p
