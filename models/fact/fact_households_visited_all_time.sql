{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  indexes=[
    {'columns': ['location_id','period_id','metric_id'], 'unique': true},
    {'columns': ['period_id']},
    {'columns': ['location_id']},
    {'columns': ['metric_id']}
  ],
  tags=['kpi','households_visited','cadence_daily'],
  on_schema_change='ignore'
) }}

WITH all_time_period AS (
  SELECT period_id
  FROM {{ ref('dim_period') }}
  WHERE period_id_name = 'all_time'
),

agg AS (
  SELECT
    location_id,
    COUNT(*)::bigint AS value,
    MAX(last_updated) AS last_updated
  FROM {{ ref('int_households_visited_pairs_all_time') }}
  GROUP BY 1
)

SELECT
  a.location_id,
  p.period_id,
  'hh_visited' AS metric_id,
  a.value,
  a.last_updated
FROM agg a
CROSS JOIN all_time_period p
WHERE a.value > 0