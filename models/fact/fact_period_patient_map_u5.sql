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
    date_trunc('day', u.reported) AS date,
    u.patient_id,
    u.reported_by_parent_parent AS location_id,
    p.sex,
    u.reported,
    u.muac_color,
    u.has_fast_breathing,
    u.has_chest_indrawing,
    u.has_diarrhoea,
    u.gave_zinc,
    u.gave_ors,
    u.gave_amox,
    u.has_fever,
    u.fever_duration,
    u.rdt_result,
    u.repeat_rdt_result,
    u.gave_al,
    u.referred_for_development_milestones,
    u.has_been_referred
  FROM {{ ref('u5_assessment') }} u
  LEFT JOIN {{ ref('patient_f_client') }} p ON u.patient_id = p.uuid
  WHERE u.patient_id IS NOT NULL
)
,
metrics AS (
  SELECT
    b.date,
    b.patient_id,
    b.location_id,
    unnest(ARRAY[
      'u5_assessed',
      CASE WHEN b.has_diarrhoea THEN 'u5_diarrhea_cases' END,
      CASE WHEN b.has_fast_breathing OR b.has_chest_indrawing THEN 'u5_pneumonia_cases' END,
      CASE WHEN b.muac_color IN ('red', 'yellow') THEN 'u5_malnutrition_cases' END,
      CASE WHEN b.muac_color IN ('red', 'yellow') AND b.sex = 'male' THEN 'u5_malnutrition_male' END,
      CASE WHEN b.muac_color IN ('red', 'yellow') AND b.sex = 'female' THEN 'u5_malnutrition_female' END,
      CASE WHEN b.rdt_result IS NOT NULL THEN 'u5_tested_malaria' END,
      CASE WHEN b.rdt_result IN ('positive') THEN 'u5_confirmed_malaria_cases' END,
      CASE WHEN b.has_fever THEN 'u5_suspected_malaria_cases' END,
      CASE WHEN b.gave_al THEN 'u5_treated_malaria' END,
      CASE WHEN b.gave_amox THEN 'u5_treated_pneumonia' END,
      CASE WHEN b.gave_ors OR b.gave_zinc THEN 'u5_treated_diarrhoea' END,
      CASE WHEN b.rdt_result = 'positive' AND b.has_been_referred THEN 'referred_for_malaria' END,
      CASE WHEN b.referred_for_development_milestones THEN 'referred_for_development_milestones' END,
      CASE WHEN b.referred_for_development_milestones AND b.sex = 'male' THEN 'male_referred_for_development_milestones' END,
      CASE WHEN b.referred_for_development_milestones AND b.sex = 'female' THEN 'female_referred_for_development_milestones' END,
      CASE WHEN b.has_been_referred THEN 'u5_referred' END,
      CASE WHEN b.gave_ors OR b.gave_amox OR b.gave_al OR b.gave_zinc THEN 'u5_treated' END,
      CASE WHEN b.gave_al THEN 'u5_treated_malaria' END,
      CASE WHEN b.gave_ors OR b.gave_zinc THEN 'u5_treated_diarrhoea' END,
      CASE WHEN b.gave_amox THEN 'u5_treated_pneumonia' END,
      CASE WHEN b.rdt_result <> 'not_done' OR b.rdt_result IS NOT NULL THEN 'u5_tested_malaria' END
    ]) AS metric_id
  FROM base b
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
