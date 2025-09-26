-- models/fact/metrics/households_visited_all_time.sql
{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','households_visited','cadence_daily'],
  on_schema_change='ignore'
) }}

WITH base AS (
  SELECT
    hv.reported_by_parent AS location_id,
    hv.reported::date     AS visit_date,
    hv.household          AS household_id
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household_visit') }} hv
    JOIN {{ ref('household') }} h on hv.household = h.uuid
    JOIN {{ source(env_var('POSTGRES_SCHEMA'), 'mv_location_hierarchy') }} chps on hv.reported_by_parent = chps.chp_area_id
    JOIN {{ ref('contact') }} c ON hv.household = c.uuid and c.contact_type = 'e_household'
  WHERE hv.household IS NOT NULL and c.muted is null
),
dated AS (
  SELECT b.location_id, pd.period_id, b.household_id
  FROM base b
  JOIN {{ ref('dim_period_date_map') }} pd ON pd.date = b.visit_date WHERE pd.period_id_name = 'all_time'
),
agg AS (
  SELECT location_id, period_id, COUNT(DISTINCT household_id) AS value
  FROM dated
  GROUP BY 1,2
)
SELECT location_id, period_id, 'hh_visited' AS metric_id, value, CURRENT_TIMESTAMP AS last_updated
FROM agg
WHERE value > 0
