-- models/fact/metrics/fact_households_registered.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','coverage'],
  on_schema_change = 'ignore'
) }}

WITH periods AS (
  SELECT period_id, end_date
  FROM {{ ref('dim_period') }}
),

households AS (
  SELECT
    hh.chv_area_id      AS location_id,
    (hh.reported)::date AS reported_date
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} hh
  WHERE hh.chv_area_id IS NOT NULL
    AND hh.reported IS NOT NULL
),

-- Cumulative: each household counts for every period whose end_date >= its reported_date
agg AS (
  SELECT
    h.location_id,
    p.period_id,
    COUNT(*)::bigint AS value
  FROM households h
  JOIN periods   p
    ON h.reported_date <= p.end_date
  GROUP BY h.location_id, p.period_id
)

SELECT
  location_id,
  period_id,
  'households_registered' AS metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM agg
WHERE value > 0

{% if is_incremental() %}
  {# Recompute only periods that could change with late/new households.
     Adjust window based on your lateness profile (e.g., 30/60/90 days). #}
  {% set preds = [
    "period_id IN (SELECT period_id
                   FROM " ~ ref('dim_period') ~ "
                   WHERE end_date >= CURRENT_DATE - interval '30 days')"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
