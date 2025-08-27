-- models/fact/metrics/u5_conditions_from_enriched.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','u5', 'cadence_hourly'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    e.reported_by_parent AS location_id,
    e.reported_date      AS report_date,
    e.patient_id,
    e.sex,
    e.has_diarrhoea,
    e.has_fever,
    e.has_pneumonia,
    e.has_malnutrition,
    e.has_malaria,
    e.referred_for_development_milestones,
    e.has_been_referred,
    e.rdt_result,
    COALESCE(e.gave_amox, FALSE) AS gave_amox,
    COALESCE(e.gave_zinc, FALSE) AS gave_zinc,
    COALESCE(e.gave_ors,  FALSE) AS gave_ors,
    COALESCE(e.gave_al,   FALSE) AS gave_al
  FROM {{ ref('under_five_assessment_enriched') }} e
),

expanded AS (
  SELECT
    location_id,
    report_date,
    patient_id,
    UNNEST(ARRAY[
      /* assessed */
      'u5_assessed',
      /* conditions */
      CASE WHEN has_diarrhoea    THEN 'u5_diarrhea_cases'          END,
      CASE WHEN has_pneumonia    THEN 'u5_pneumonia_cases'         END,
      CASE WHEN has_malnutrition THEN 'u5_malnutrition_cases'      END,
      CASE WHEN has_malaria      THEN 'u5_confirmed_malaria_cases' END,
      CASE WHEN has_fever AND NOT has_malaria AND (NOT has_pneumonia OR NOT has_diarrhoea)
           THEN 'u5_suspected_malaria_cases' END,
      /* referrals */
      CASE WHEN has_fever       AND has_been_referred THEN 'referred_for_malaria'      END,
      CASE WHEN has_pneumonia   AND has_been_referred THEN 'referred_for_pneumonia'    END,
      CASE WHEN has_malnutrition AND has_been_referred THEN 'referred_for_malnutrition' END,
      CASE WHEN has_diarrhoea   AND has_been_referred THEN 'referred_for_diarrhoea'    END,
      CASE WHEN has_been_referred THEN 'u5_referred' END,
      CASE WHEN has_been_referred AND sex = 'male'   THEN 'u5_referred_male'   END,
      CASE WHEN has_been_referred AND sex = 'female' THEN 'u5_referred_female' END,
      /* development milestones */
      CASE WHEN referred_for_development_milestones THEN 'referred_for_development_milestones' END,
      CASE WHEN referred_for_development_milestones AND sex='male'   THEN 'male_referred_for_development_milestones'   END,
      CASE WHEN referred_for_development_milestones AND sex='female' THEN 'female_referred_for_development_milestones' END,
      /* treatment */
      CASE WHEN (gave_ors OR gave_zinc OR gave_al OR gave_amox) THEN 'u5_treated' END,
      CASE WHEN gave_al                                THEN 'u5_treated_malaria'   END,
      CASE WHEN (gave_ors OR gave_zinc)                THEN 'u5_treated_diarrhoea' END,
      CASE WHEN (gave_amox AND has_pneumonia)          THEN 'u5_treated_pneumonia' END,
      /* testing */
      CASE WHEN (rdt_result IS NOT NULL AND rdt_result <> 'not_done')
           THEN 'u5_tested_malaria' END
    ]) AS metric_id
  FROM base
),

dated AS (
  SELECT
    e.location_id,
    pd.period_id,
    e.metric_id,
    e.patient_id
  FROM expanded e
  JOIN {{ ref('dim_period_date_map') }} pd
    ON pd.date = e.report_date
),

agg AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(DISTINCT patient_id) AS value
  FROM dated
  WHERE metric_id IS NOT NULL
  GROUP BY 1,2,3
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM agg
WHERE value > 0

{% if is_incremental() %}
  {# Re-write only periods touched by new/changed rows (today), adjust if you want a wider window #}
  {% set preds = [
    "period_id in (select period_id from " ~ ref('dim_period_date_map') ~ " where date >= current_date - interval '1 day')"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
