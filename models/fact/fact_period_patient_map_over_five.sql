{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    indexes=[
      {'columns': ['period_id', 'location_id', 'metric_id']},
      {'columns': ['patient_id']}
    ],
    tags=["daily_refresh"]
) }}

WITH base AS (
  SELECT
    date_trunc('day', o.reported) AS date,
    o.patient_id,
    o.reported_by_parent_parent AS location_id,
    p.sex,
    o.reported
  FROM {{ ref('over_five_assessment') }} o
        join {{ ref('patient_f_client') }} p on o.patient_id = p.uuid
  WHERE o.reported IS NOT NULL
)
,
metrics AS (
  SELECT
    b.date,
    b.patient_id,
    b.location_id,
    unnest(ARRAY[
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
      CASE WHEN is_referred_mental_health AND sex = 'female' THEN 'referred_mental_health_female' END
    ]) AS metric_id
  FROM base b
  JOIN {{ ref('over_five_assessment') }} r ON r.patient_id = b.patient_id AND r.reported = b.reported 
  WHERE r.patient_id IS NOT NULL
)

SELECT
  p.period_id AS period_id,
  m.location_id,
  m.metric_id,
  m.patient_id
FROM metrics m
JOIN {{ ref('dim_period') }} p
  ON m.date BETWEEN p.start_date AND p.end_date
WHERE m.metric_id IS NOT NULL
