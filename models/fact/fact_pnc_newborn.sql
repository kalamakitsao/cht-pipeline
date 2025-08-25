-- models/fact/metrics/fact_pnc_newborn.sql

-- depends_on: {{ ref('dim_period') }}
-- depends_on: {{ ref('dim_period_date_map') }}

{% set dp = ref('dim_period') %}
{% set dpdm = ref('dim_period_date_map') %}

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','pnc','newborn'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    e.location_id,
    e.reported_date,
    e.patient_id,
    e.is_referred_immunization,
    e.needs_danger_signs_follow_up,
    e.needs_missed_visit_follow_up,
    e.place_of_birth_display,
    e.pnc_service_count,
    (e.is_referred OR e.is_referred_for_pnc_services OR e.is_referred_immunization) AS any_referred
  FROM {{ ref('postnatal_care_service_newborn_enriched') }} e
),

-- map day→period via equality join (index-friendly)
with_period AS (
  SELECT
    b.location_id,
    pd.period_id,
    b.patient_id,
    b.is_referred_immunization,
    b.needs_danger_signs_follow_up,
    b.needs_missed_visit_follow_up,
    b.place_of_birth_display,
    b.pnc_service_count,
    b.any_referred
  FROM base b
  JOIN {{ dpdm }} pd
    ON pd.date = b.reported_date
),

-- single pass aggregate with FILTERs on DISTINCT patients
agg AS (
  SELECT
    wp.location_id,
    wp.period_id,

    COUNT(DISTINCT wp.patient_id) FILTER (WHERE wp.is_referred_immunization)                              AS newborn_referred_immunization,
    COUNT(DISTINCT wp.patient_id) FILTER (WHERE wp.needs_danger_signs_follow_up)                          AS newborn_needs_danger_signs_follow_up,
    COUNT(DISTINCT wp.patient_id) FILTER (WHERE wp.needs_missed_visit_follow_up)                          AS newborn_needs_missed_visit_follow_up,
    COUNT(DISTINCT wp.patient_id) FILTER (WHERE wp.place_of_birth_display ILIKE '%home%')                 AS newborn_home_delivery,
    COUNT(DISTINCT wp.patient_id) FILTER (WHERE wp.pnc_service_count IS NOT NULL AND wp.pnc_service_count > 0)
                                                                                                          AS newborn_pnc_service_count,
    COUNT(DISTINCT wp.patient_id) FILTER (WHERE wp.any_referred)                                          AS newborn_any_referred
  FROM with_period wp
  GROUP BY wp.location_id, wp.period_id
),

-- tiny unpivot → metric rows
metrics AS (
  SELECT
    a.location_id,
    a.period_id,
    m.metric_id,
    m.value
  FROM agg a
  CROSS JOIN LATERAL (
    VALUES
      ('newborn_referred_immunization',        a.newborn_referred_immunization),
      ('newborn_needs_danger_signs_follow_up', a.newborn_needs_danger_signs_follow_up),
      ('newborn_needs_missed_visit_follow_up', a.newborn_needs_missed_visit_follow_up),
      ('newborn_home_delivery',                 a.newborn_home_delivery),
      ('newborn_pnc_service_count',             a.newborn_pnc_service_count),
      ('newborn_any_referred',                  a.newborn_any_referred)
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
  {# Recompute only recent periods likely to change; tune the window #}
  {% set preds = [
    "period_id IN (SELECT period_id FROM " ~ dp ~
    " WHERE end_date >= CURRENT_DATE - interval '35 days')"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
