-- models/fact/metrics/fact_over_five_metrics_all_time.sql
{{ config(
  materialized = 'table',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','ncd','cadence_daily'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    e.reported_by_parent AS location_id,
    e.reported_date      AS report_date,
    e.patient_id,
    e.sex,
    e.screened_for_diabetes,
    e.is_referred_diabetes,
    e.screened_for_hypertension,
    e.is_referred_hypertension,
    e.screened_for_mental_health,
    e.is_referred_mental_health,
    e.has_been_referred
  FROM {{ ref('over_five_assessment_enriched') }} e
),

-- EVENTS (workload)
events_expanded AS (
  SELECT
    b.location_id,
    b.report_date,
    UNNEST(ARRAY[
      CASE WHEN b.screened_for_diabetes      IS TRUE THEN 'screenings_diabetes' END,
      CASE WHEN b.screened_for_hypertension  IS TRUE THEN 'screenings_hypertension' END,
      CASE WHEN b.screened_for_mental_health IS TRUE THEN 'screenings_mental_health' END,

      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex='male'   THEN 'screenings_diabetes_male'   END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex='female' THEN 'screenings_diabetes_female' END,

      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex='male'   THEN 'screenings_hypertension_male'   END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex='female' THEN 'screenings_hypertension_female' END,

      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex='male'   THEN 'screenings_mental_health_male'   END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex='female' THEN 'screenings_mental_health_female' END
    ]) AS metric_id
  FROM base b
),

events_dated AS (
  SELECT e.location_id, p.period_id, e.metric_id
  FROM events_expanded e
  JOIN {{ ref('dim_period_date_map') }} p
    ON p.date = e.report_date
  WHERE p.period_id_name = 'all_time'
    AND e.metric_id IS NOT NULL
),

events_agg AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(*) AS value
  FROM events_dated
  GROUP BY 1,2,3
),

-- PEOPLE (unique individuals)
people_expanded AS (
  SELECT
    b.location_id,
    b.report_date,
    b.patient_id,
    UNNEST(ARRAY[
      CASE WHEN b.screened_for_diabetes      IS TRUE THEN 'screened_diabetes' END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex='male'   THEN 'screened_diabetes_male'   END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex='female' THEN 'screened_diabetes_female' END,

      CASE WHEN b.screened_for_hypertension  IS TRUE THEN 'screened_hypertension' END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex='male'   THEN 'screened_hypertension_male'   END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex='female' THEN 'screened_hypertension_female' END,

      CASE WHEN b.screened_for_mental_health IS TRUE THEN 'screened_mental_health' END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex='male'   THEN 'screened_mental_health_male'   END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex='female' THEN 'screened_mental_health_female' END,

      CASE WHEN b.is_referred_diabetes       IS TRUE THEN 'referred_diabetes' END,
      CASE WHEN b.is_referred_diabetes       IS TRUE AND b.sex='male'   THEN 'referred_diabetes_male'   END,
      CASE WHEN b.is_referred_diabetes       IS TRUE AND b.sex='female' THEN 'referred_diabetes_female' END,

      CASE WHEN b.is_referred_hypertension   IS TRUE THEN 'referred_hypertension' END,
      CASE WHEN b.is_referred_hypertension   IS TRUE AND b.sex='male'   THEN 'referred_hypertension_male'   END,
      CASE WHEN b.is_referred_hypertension   IS TRUE AND b.sex='female' THEN 'referred_hypertension_female' END,

      CASE WHEN b.is_referred_mental_health  IS TRUE THEN 'referred_mental_health' END,
      CASE WHEN b.is_referred_mental_health  IS TRUE AND b.sex='male'   THEN 'referred_mental_health_male'   END,
      CASE WHEN b.is_referred_mental_health  IS TRUE AND b.sex='female' THEN 'referred_mental_health_female' END,

      CASE WHEN b.has_been_referred          IS TRUE THEN 'over_5_referred' END,
      CASE WHEN b.has_been_referred          IS TRUE AND b.sex='male'   THEN 'over_5_referred_male'   END,
      CASE WHEN b.has_been_referred          IS TRUE AND b.sex='female' THEN 'over_5_referred_female' END,

      'over_5_assessments'
    ]) AS metric_id
  FROM base b
),

people_dated AS (
  SELECT e.location_id, p.period_id, e.metric_id, e.patient_id
  FROM people_expanded e
  JOIN {{ ref('dim_period_date_map') }} p
    ON p.date = e.report_date
  WHERE p.period_id_name = 'all_time'
    AND e.metric_id IS NOT NULL
),

people_agg AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(patient_id) AS value
  FROM people_dated
  GROUP BY 1,2,3
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM (
  SELECT * FROM events_agg
  UNION ALL
  SELECT * FROM people_agg
) s
WHERE value > 0