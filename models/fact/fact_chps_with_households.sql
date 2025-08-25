-- models/fact/metrics/fact_chps_with_households.sql
{{ config(
  materialized = 'table',
  tags = ['kpi','coverage'],
  on_schema_change = 'ignore'
) }}

WITH first_seen AS (
  SELECT
    h.chv_area_id       AS location_id,
    MIN(h.reported::date) AS first_seen_date
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} h
  WHERE h.chv_area_id IS NOT NULL
    AND h.reported IS NOT NULL
  GROUP BY h.chv_area_id
),

matched_periods AS (
  -- cumulative presence: once a CHP has ≥1 household on/ before a date,
  -- they count for all periods whose end_date is on/after that first_seen_date
  SELECT
    fs.location_id,
    p.period_id
  FROM first_seen fs
  JOIN {{ ref('dim_period') }} p
    ON fs.first_seen_date <= p.end_date
)

SELECT
  mp.location_id,
  mp.period_id,
  'chps_with_hholds' AS metric_id,
  1                  AS value,
  CURRENT_TIMESTAMP  AS last_updated
FROM matched_periods mp
