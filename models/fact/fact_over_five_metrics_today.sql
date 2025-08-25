-- models/fact/metrics/fact_over_five_metrics_today.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','ncd','cadence_hourly'],
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
  WHERE e.reported_date = CURRENT_DATE
),

expanded AS (
  SELECT
    location_id, report_date, patient_id,
    UNNEST(ARRAY[
      /* diabetes */
      CASE WHEN screened_for_diabetes IS TRUE                    THEN 'screened_diabetes'            END,
      CASE WHEN screened_for_diabetes IS TRUE AND sex='male'     THEN 'screened_diabetes_male'       END,
      CASE WHEN screened_for_diabetes IS TRUE AND sex='female'   THEN 'screened_diabetes_female'     END,
      CASE WHEN is_referred_diabetes IS TRUE                     THEN 'referred_diabetes'            END,
      CASE WHEN is_referred_diabetes IS TRUE AND sex='male'      THEN 'referred_diabetes_male'       END,
      CASE WHEN is_referred_diabetes IS TRUE AND sex='female'    THEN 'referred_diabetes_female'     END,
      /* hypertension */
      CASE WHEN screened_for_hypertension IS TRUE                 THEN 'screened_hypertension'        END,
      CASE WHEN screened_for_hypertension IS TRUE AND sex='male'  THEN 'screened_hypertension_male'   END,
      CASE WHEN screened_for_hypertension IS TRUE AND sex='female'THEN 'screened_hypertension_female' END,
      CASE WHEN is_referred_hypertension IS TRUE                  THEN 'referred_hypertension'        END,
      CASE WHEN is_referred_hypertension IS TRUE AND sex='male'   THEN 'referred_hypertension_male'   END,
      CASE WHEN is_referred_hypertension IS TRUE AND sex='female' THEN 'referred_hypertension_female' END,
      /* mental health */
      CASE WHEN screened_for_mental_health IS TRUE                THEN 'screened_mental_health'       END,
      CASE WHEN screened_for_mental_health IS TRUE AND sex='male' THEN 'screened_mental_health_male'  END,
      CASE WHEN screened_for_mental_health IS TRUE AND sex='female' THEN 'screened_mental_health_female' END,
      CASE WHEN is_referred_mental_health IS TRUE                 THEN 'referred_mental_health'       END,
      CASE WHEN is_referred_mental_health IS TRUE AND sex='male'  THEN 'referred_mental_health_male'  END,
      CASE WHEN is_referred_mental_health IS TRUE AND sex='female'THEN 'referred_mental_health_female' END,
      /* overall */
      CASE WHEN has_been_referred IS TRUE                         THEN 'over_5_referred'              END,
      CASE WHEN has_been_referred IS TRUE AND sex='male'          THEN 'over_5_referred_male'         END,
      CASE WHEN has_been_referred IS TRUE AND sex='female'        THEN 'over_5_referred_female'       END,
      'over_5_assessments'
    ]) AS metric_id
  FROM base
),

dated AS (
  SELECT e.location_id, pd.period_id, e.metric_id, e.patient_id
  FROM expanded e
  JOIN {{ ref('dim_period_date_map') }} pd ON pd.date = e.report_date
),

agg AS (
  SELECT location_id, period_id, metric_id, COUNT(DISTINCT patient_id) AS value
  FROM dated
  WHERE metric_id IS NOT NULL
  GROUP BY 1,2,3
)

SELECT location_id, period_id, metric_id, value, CURRENT_TIMESTAMP AS last_updated
FROM agg
WHERE value > 0

{% if is_incremental() %}
  {% set preds = [
    "period_id IN (SELECT period_id FROM " ~ ref('dim_period_date_map') ~ " WHERE date = CURRENT_DATE)"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
