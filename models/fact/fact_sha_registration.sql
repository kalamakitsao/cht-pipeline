-- models/fact/metrics/fact_sha_registration.sql

-- depends_on: {{ ref('dim_period') }}
-- depends_on: {{ ref('dim_period_date_map') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','sha','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    e.location_id,
    e.reported_date,
    e.has_sha_registration
  FROM {{ ref('sha_registration_enriched') }} e
),

-- Map day → period via equality join (hits index on date)
with_period AS (
  SELECT
    b.location_id,
    pd.period_id,
    b.has_sha_registration
  FROM base b
  JOIN {{ ref('dim_period_date_map') }} pd
    ON pd.date = b.reported_date
),

-- Single aggregate with FILTERs (no row explosion)
agg AS (
  SELECT
    wp.location_id,
    wp.period_id,
    COUNT(*)::bigint                                           AS households_assessed_sha,
    COUNT(*) FILTER (WHERE wp.has_sha_registration IS TRUE)::bigint AS households_with_sha
  FROM with_period wp
  GROUP BY wp.location_id, wp.period_id
),

-- Tiny unpivot to metric rows
metrics AS (
  SELECT
    a.location_id,
    a.period_id,
    m.metric_id,
    m.value
  FROM agg a
  CROSS JOIN LATERAL (
    VALUES
      ('households_assessed_sha', a.households_assessed_sha),
      ('households_with_sha',     a.households_with_sha)
  ) AS m(metric_id, value)
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM metrics
WHERE value > 0

{% if is_incremental() %}
  {# Only rewrite periods that end recently; tune window to your lateness profile #}
  {% set preds = [
    "period_id IN (SELECT period_id FROM " ~ dp ~
    " WHERE end_date >= CURRENT_DATE - interval '35 days')"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
