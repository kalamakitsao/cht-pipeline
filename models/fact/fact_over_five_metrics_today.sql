-- models/fact/metrics/fact_over_five_metrics_today.sql
{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','ncd','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

-- 1) Base: scope to "today" 
WITH base AS (
  SELECT
    e.reported_by_parent  AS location_id,
    e.reported_date       AS report_date,
    e.patient_id,
    e.sex,  
    e.screened_for_diabetes,
    e.screened_for_hypertension,
    e.screened_for_mental_health
  FROM {{ ref('over_five_assessment_enriched') }} e
  WHERE e.reported_date = CURRENT_DATE
),

base_dedup AS (
  SELECT DISTINCT
    location_id, report_date, patient_id, sex,
    screened_for_diabetes, screened_for_hypertension, screened_for_mental_health
  FROM base
),

-- Measure workload/event metrics with each screening done as an event
events_expanded AS (
  SELECT
    b.location_id,
    b.report_date,
    UNNEST(ARRAY[
      CASE WHEN b.screened_for_diabetes      IS TRUE THEN 'screenings_diabetes' END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex = 'male'   THEN 'screenings_diabetes_male'   END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex = 'female' THEN 'screenings_diabetes_female' END,

      CASE WHEN b.screened_for_hypertension  IS TRUE THEN 'screenings_hypertension' END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex = 'male'   THEN 'screenings_hypertension_male'   END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex = 'female' THEN 'screenings_hypertension_female' END,

      CASE WHEN b.screened_for_mental_health IS TRUE THEN 'screenings_mental_health' END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex = 'male'   THEN 'screenings_mental_health_male'   END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex = 'female' THEN 'screenings_mental_health_female' END
    ]) AS metric_id
  FROM base_dedup b
),

-- Unique-person metrics
people_expanded AS (
  SELECT
    b.location_id,
    b.report_date,
    b.patient_id,
    UNNEST(ARRAY[
      CASE WHEN b.screened_for_diabetes      IS TRUE THEN 'screened_diabetes' END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex = 'male'   THEN 'screened_diabetes_male'   END,
      CASE WHEN b.screened_for_diabetes      IS TRUE AND b.sex = 'female' THEN 'screened_diabetes_female' END,

      CASE WHEN b.screened_for_hypertension  IS TRUE THEN 'screened_hypertension' END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex = 'male'   THEN 'screened_hypertension_male'   END,
      CASE WHEN b.screened_for_hypertension  IS TRUE AND b.sex = 'female' THEN 'screened_hypertension_female' END,

      CASE WHEN b.screened_for_mental_health IS TRUE THEN 'screened_mental_health' END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex = 'male'   THEN 'screened_mental_health_male'   END,
      CASE WHEN b.screened_for_mental_health IS TRUE AND b.sex = 'female' THEN 'screened_mental_health_female' END
    ]) AS metric_id
  FROM base_dedup b
),

events_dated AS (
  SELECT
    e.location_id,
    p.period_id,
    e.metric_id
  FROM events_expanded e
  JOIN {{ ref('dim_period_date_map') }} p
    ON p.date = e.report_date
   AND p.period_id_name = 'today'
  WHERE e.metric_id IS NOT NULL
),

people_dated AS (
  SELECT
    e.location_id,
    p.period_id,
    e.metric_id,
    e.patient_id
  FROM people_expanded e
  JOIN {{ ref('dim_period_date_map') }} p
    ON p.date = e.report_date
   AND p.period_id_name = 'today'
  WHERE e.metric_id IS NOT NULL
),

events_agg AS (
  -- Workload: count screening *events*
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(*) AS value
  FROM events_dated
  GROUP BY 1,2,3
),

people_agg AS (
  -- People: count *unique patients* screened
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(DISTINCT patient_id) AS value
  FROM people_dated
  GROUP BY 1,2,3
),

all_agg AS (
  SELECT * FROM events_agg
  UNION ALL
  SELECT * FROM people_agg
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM all_agg
WHERE value > 0