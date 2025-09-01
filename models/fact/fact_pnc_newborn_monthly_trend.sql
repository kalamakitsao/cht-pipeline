-- models/fact/metrics/fact_over_five_metrics_all_time.sql
{{ config(
  materialized = 'incremental',
  unique_key = ['location_id','period_start','metric_id'],
  tags = ['kpi','ncd','cadence_daily'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    location_id,
    reported_date,
    date_trunc('month', reported_date) AS period_start,
    patient_id,
    is_referred_immunization,
    needs_danger_signs_follow_up,
    needs_missed_visit_follow_up,
    place_of_birth_display,
    pnc_service_count,
    (is_referred OR is_referred_for_pnc_services OR is_referred_immunization) AS any_referred
  FROM {{ ref('postnatal_care_service_newborn_enriched') }}
  WHERE reported >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
),

metrics AS (
  SELECT location_id, period_start, patient_id, unnest(vals) AS metric_id
  FROM (
    SELECT
      location_id,
      period_start,
      patient_id,
      ARRAY_REMOVE(ARRAY[
        CASE WHEN is_referred_immunization THEN 'newborn_referred_immunization' END,
        CASE WHEN needs_danger_signs_follow_up THEN 'newborn_needs_danger_signs_follow_up' END,
        CASE WHEN needs_missed_visit_follow_up THEN 'newborn_needs_missed_visit_follow_up' END,
        CASE WHEN place_of_birth_display ILIKE '%home%' THEN 'newborn_home_delivery' END,
        CASE WHEN pnc_service_count IS NOT NULL AND pnc_service_count > 0 THEN 'newborn_pnc_service_count' END,
        CASE WHEN any_referred THEN 'newborn_any_referred' END
      ], NULL) AS vals
    FROM base
  ) t
)

SELECT
  location_id,
  period_start,
  metric_id,
  COUNT(DISTINCT patient_id) AS value
FROM metrics
GROUP BY location_id, period_start, metric_id
