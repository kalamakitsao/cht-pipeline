{{ config(
    materialized = 'incremental',
    on_schema_change = 'append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    indexes = [
        {'columns': ['period_id', 'location_id', 'metric_id']},
        {'columns': ['patient_id']}
    ],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

filtered_data AS (
  SELECT
    o.reported_by_parent_parent AS location_id,
    o.reported::DATE AS reported_date,
    o.patient_id,
    p.sex,
    o.screened_for_diabetes,
    o.is_referred_diabetes,
    o.screened_for_hypertension,
    o.is_referred_hypertension,
    o.screened_for_mental_health,
    o.is_referred_mental_health,
    o.has_been_referred
  FROM {{ ref('over_five_assessment') }} o
  JOIN {{ ref('patient_f_client') }} p ON o.patient_id = p.uuid
  WHERE o.reported IS NOT NULL
    AND o.patient_id IS NOT NULL
),

metric_map AS (
  SELECT
    fd.reported_date,
    fd.patient_id,
    fd.location_id,
    unnest(array_remove(array[
      CASE WHEN screened_for_diabetes THEN 'screened_diabetes' END,
      CASE WHEN screened_for_diabetes AND sex = 'male' THEN 'screened_diabetes_male' END,
      CASE WHEN screened_for_diabetes AND sex = 'female' THEN 'screened_diabetes_female' END,

      CASE WHEN is_referred_diabetes THEN 'referred_diabetes' END,
      CASE WHEN is_referred_diabetes AND sex = 'male' THEN 'referred_diabetes_male' END,
      CASE WHEN is_referred_diabetes AND sex = 'female' THEN 'referred_diabetes_female' END,

      CASE WHEN screened_for_hypertension THEN 'screened_hypertension' END,
      CASE WHEN screened_for_hypertension AND sex = 'male' THEN 'screened_hypertension_male' END,
      CASE WHEN screened_for_hypertension AND sex = 'female' THEN 'screened_hypertension_female' END,

      CASE WHEN is_referred_hypertension THEN 'referred_hypertension' END,
      CASE WHEN is_referred_hypertension AND sex = 'male' THEN 'referred_hypertension_male' END,
      CASE WHEN is_referred_hypertension AND sex = 'female' THEN 'referred_hypertension_female' END,

      CASE WHEN screened_for_mental_health THEN 'screened_mental_health' END,
      CASE WHEN screened_for_mental_health AND sex = 'male' THEN 'screened_mental_health_male' END,
      CASE WHEN screened_for_mental_health AND sex = 'female' THEN 'screened_mental_health_female' END,

      CASE WHEN is_referred_mental_health THEN 'referred_mental_health' END,
      CASE WHEN is_referred_mental_health AND sex = 'male' THEN 'referred_mental_health_male' END,
      CASE WHEN is_referred_mental_health AND sex = 'female' THEN 'referred_mental_health_female' END,

      CASE WHEN has_been_referred THEN 'over_5_referred' END,
      CASE WHEN has_been_referred AND sex = 'male' THEN 'over_5_referred_male' END,
      CASE WHEN has_been_referred AND sex = 'female' THEN 'over_5_referred_female' END,

      'over_5_assessments'
    ], NULL)) AS metric_id
  FROM filtered_data fd
)

SELECT
  p.period_id,
  m.location_id,
  m.metric_id,
  m.patient_id
FROM metric_map m
JOIN periods p ON m.reported_date BETWEEN p.start_date AND p.end_date
WHERE m.metric_id IS NOT NULL
