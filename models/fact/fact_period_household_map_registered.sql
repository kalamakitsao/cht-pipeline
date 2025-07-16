-- fact_period_household_map_registered.sql
{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'household_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_households AS (
  SELECT
    hh.uuid AS household_id,
    hh.chv_area_id AS location_id,
    DATE(hh.reported) AS report_date
  FROM {{ ref('household') }} hh
  WHERE hh.chv_area_id IN (SELECT location_id FROM {{ ref('dim_location') }})
)

SELECT
  p.period_id AS period_id,
  b.location_id,
  'households_registered' AS metric_id,
  b.household_id
FROM base_households b
JOIN periods p ON b.report_date <= p.end_date
