-- models/fact/metrics/households_visited_today.sql
{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','households_visited','cadence_hourly'],
  on_schema_change='ignore'
) }}

WITH base AS (
  SELECT
    hv.reported_by_parent AS location_id,
    hv.reported::date     AS visit_date,
    hv.household          AS household_id
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household_visit') }} hv
  WHERE hv.household IS NOT NULL
    AND hv.reported::date = CURRENT_DATE
),
dated AS (
  SELECT b.location_id, pd.period_id, b.household_id
  FROM base b
  JOIN {{ ref('dim_period_date_map') }} pd ON pd.date = b.visit_date
),
agg AS (
  SELECT location_id, period_id, COUNT(DISTINCT household_id) AS value
  FROM dated
  GROUP BY 1,2
)
SELECT location_id, period_id, 'hh_visited' AS metric_id, value, CURRENT_TIMESTAMP AS last_updated
FROM agg
WHERE value > 0

{% if is_incremental() %}
  {% set preds = [
    "period_id in (select period_id from " ~ ref('dim_period_date_map') ~ " where date = CURRENT_DATE)"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
