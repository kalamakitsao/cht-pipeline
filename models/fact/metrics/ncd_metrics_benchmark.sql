{{ config(
  materialized = 'incremental',
  unique_key = ['location_id', 'period_id', 'metric_id'],
  tags = ['kpi', 'ncd'],
  on_schema_change = 'ignore'
) }}

WITH relevant_assessments AS (
  SELECT o.*
  FROM {{ ref('over_five_assessment') }} o
  JOIN {{ ref('period_date_map') }} pd
    ON o.reported::date = pd.date
),

relevant_patients AS (
  SELECT * FROM {{ ref('dim_patient_sex') }}
),

joined_data AS (
  SELECT
    o.reported_by_parent AS location_id,
    o.reported::date AS reported_date,
    o.patient_id,
    p.sex,
    o.screened_for_diabetes,
    o.is_referred_diabetes,
    o.screened_for_hypertension,
    o.is_referred_hypertension,
    o.screened_for_mental_health,
    o.is_referred_mental_health,
    o.has_been_referred
  FROM relevant_assessments o
  LEFT JOIN relevant_patients p ON o.patient_id = p.uuid
),

expanded_metrics AS (
  SELECT
    location_id,
    reported_date,
    patient_id,
    UNNEST(ARRAY[
      CASE WHEN screened_for_diabetes IS TRUE THEN 'screened_diabetes' ELSE NULL END,
      CASE WHEN screened_for_diabetes IS TRUE AND sex = 'male' THEN 'screened_diabetes_male' ELSE NULL END,
      CASE WHEN screened_for_diabetes IS TRUE AND sex = 'female' THEN 'screened_diabetes_female' ELSE NULL END,
      CASE WHEN is_referred_diabetes IS TRUE THEN 'referred_diabetes' ELSE NULL END,
      CASE WHEN is_referred_diabetes IS TRUE AND sex = 'male' THEN 'referred_diabetes_male' ELSE NULL END,
      CASE WHEN is_referred_diabetes IS TRUE AND sex = 'female' THEN 'referred_diabetes_female' ELSE NULL END,
      CASE WHEN screened_for_hypertension IS TRUE THEN 'screened_hypertension' ELSE NULL END,
      CASE WHEN screened_for_hypertension IS TRUE AND sex = 'male' THEN 'screened_hypertension_male' ELSE NULL END,
      CASE WHEN screened_for_hypertension IS TRUE AND sex = 'female' THEN 'screened_hypertension_female' ELSE NULL END,
      CASE WHEN is_referred_hypertension IS TRUE THEN 'referred_hypertension' ELSE NULL END,
      CASE WHEN is_referred_hypertension IS TRUE AND sex = 'male' THEN 'referred_hypertension_male' ELSE NULL END,
      CASE WHEN is_referred_hypertension IS TRUE AND sex = 'female' THEN 'referred_hypertension_female' ELSE NULL END,
      CASE WHEN screened_for_mental_health IS TRUE THEN 'screened_mental_health' ELSE NULL END,
      CASE WHEN screened_for_mental_health IS TRUE AND sex = 'male' THEN 'screened_mental_health_male' ELSE NULL END,
      CASE WHEN screened_for_mental_health IS TRUE AND sex = 'female' THEN 'screened_mental_health_female' ELSE NULL END,
      CASE WHEN is_referred_mental_health IS TRUE THEN 'referred_mental_health' ELSE NULL END,
      CASE WHEN is_referred_mental_health IS TRUE AND sex = 'male' THEN 'referred_mental_health_male' ELSE NULL END,
      CASE WHEN is_referred_mental_health IS TRUE AND sex = 'female' THEN 'referred_mental_health_female' ELSE NULL END,
      CASE WHEN has_been_referred IS TRUE THEN 'over_5_referred' ELSE NULL END,
      CASE WHEN has_been_referred IS TRUE AND sex = 'male' THEN 'over_5_referred_male' ELSE NULL END,
      CASE WHEN has_been_referred IS TRUE AND sex = 'female' THEN 'over_5_referred_female' ELSE NULL END,
      'over_5_assessments'
    ]) AS metric_id
  FROM joined_data
),

dated_metrics AS (
  SELECT
    e.location_id,
    pd.period_id,
    e.metric_id,
    e.patient_id
  FROM expanded_metrics e
  JOIN {{ ref('period_date_map') }} pd
    ON e.reported_date = pd.date
),

aggregated AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(DISTINCT patient_id) AS value
  FROM dated_metrics
  GROUP BY 1, 2, 3
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE value > 0
