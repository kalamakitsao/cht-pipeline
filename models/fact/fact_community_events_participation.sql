-- models/fact/metrics/fact_community_events_participation.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','community','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    ce.reported_by_parent              AS location_id,
    (ce.event_date)::date              AS event_day,
    ce.event_types
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'community_event') }} ce
  -- optional pruning if dim_location defines valid CHP areas:
  -- JOIN {{ ref('dim_location') }} dl
  --   ON ce.reported_by_parent = dl.location_id AND dl.level = 'chp area'
  WHERE ce.event_date IS NOT NULL
),

-- Precompute flags ONCE to avoid multiple ILIKE scans & precedence issues
flags AS (
  SELECT
    b.location_id,
    b.event_day,
    b.event_types,
    (b.event_types ILIKE '%monthly_cu_meetings%')                AS has_monthly_tag,
    (b.event_types = 'monthly_cu_meetings')                      AS is_exact_monthly
  FROM base b
),

-- Join to period_id via date map (hits index on date)
with_period AS (
  SELECT
    f.location_id,
    pd.period_id,
    f.has_monthly_tag,
    f.is_exact_monthly
  FROM flags f
  JOIN {{ ref('dim_period_date_map') }} pd
    ON pd.date = f.event_day
),

-- Aggregate once, then unpivot via LATERAL VALUES
agg AS (
  SELECT
    wp.location_id,
    wp.period_id,
    COUNT(*) FILTER (WHERE wp.has_monthly_tag)                                                        AS monthly_cu_meetings,
    COUNT(*) FILTER (WHERE (wp.has_monthly_tag AND NOT wp.is_exact_monthly) OR (NOT wp.has_monthly_tag)) AS other_community_events
  FROM with_period wp
  GROUP BY wp.location_id, wp.period_id
),

metrics AS (
  SELECT
    a.location_id,
    a.period_id,
    m.metric_id,
    m.value
  FROM agg a
  CROSS JOIN LATERAL (
    VALUES
      ('monthly_cu_meetings',   a.monthly_cu_meetings),
      ('other_community_events',a.other_community_events)
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
  {# Recompute only periods that could have changed recently.
     Adjust the window if late-arriving events are common. #}
  {% set preds = [
    "period_id IN (SELECT period_id FROM " ~ ref('dim_period_date_map') ~
    " WHERE date >= CURRENT_DATE - interval '7 days')"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
