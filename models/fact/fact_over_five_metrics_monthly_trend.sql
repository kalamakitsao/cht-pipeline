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
    reported_by_parent AS location_id,
    date_trunc('month', reported_date) AS period_start,
    patient_id,
    sex,
    screened_for_diabetes,
    is_referred_diabetes,
    screened_for_hypertension,
    is_referred_hypertension,
    screened_for_mental_health,
    is_referred_mental_health,
    has_been_referred
  FROM {{ ref('over_five_assessment_enriched') }}
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
        CASE WHEN screened_for_diabetes THEN 'screened_diabetes' END,
        CASE WHEN screened_for_diabetes AND sex='male' THEN 'screened_diabetes_male' END,
        CASE WHEN screened_for_diabetes AND sex='female' THEN 'screened_diabetes_female' END,
        CASE WHEN is_referred_diabetes THEN 'referred_diabetes' END,
        CASE WHEN is_referred_diabetes AND sex='male' THEN 'referred_diabetes_male' END,
        CASE WHEN is_referred_diabetes AND sex='female' THEN 'referred_diabetes_female' END,
        CASE WHEN screened_for_hypertension THEN 'screened_hypertension' END,
        CASE WHEN screened_for_hypertension AND sex='male' THEN 'screened_hypertension_male' END,
        CASE WHEN screened_for_hypertension AND sex='female' THEN 'screened_hypertension_female' END,
        CASE WHEN is_referred_hypertension THEN 'referred_hypertension' END,
        CASE WHEN is_referred_hypertension AND sex='male' THEN 'referred_hypertension_male' END,
        CASE WHEN is_referred_hypertension AND sex='female' THEN 'referred_hypertension_female' END,
        CASE WHEN screened_for_mental_health THEN 'screened_mental_health' END,
        CASE WHEN screened_for_mental_health AND sex='male' THEN 'screened_mental_health_male' END,
        CASE WHEN screened_for_mental_health AND sex='female' THEN 'screened_mental_health_female' END,
        CASE WHEN is_referred_mental_health THEN 'referred_mental_health' END,
        CASE WHEN is_referred_mental_health AND sex='male' THEN 'referred_mental_health_male' END,
        CASE WHEN is_referred_mental_health AND sex='female' THEN 'referred_mental_health_female' END,
        CASE WHEN has_been_referred THEN 'over_5_referred' END,
        CASE WHEN has_been_referred AND sex='male' THEN 'over_5_referred_male' END,
        CASE WHEN has_been_referred AND sex='female' THEN 'over_5_referred_female' END,
        'over_5_assessments'
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
