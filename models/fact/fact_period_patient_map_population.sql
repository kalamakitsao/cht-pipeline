{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

valid_locations AS (
  SELECT location_id FROM {{ ref('dim_location') }}
),

clients AS (
  SELECT
    c.uuid AS patient_id,
    c.household_id,
    c.sex,
    c.date_of_birth,
    c.muted,
    c.reported
  FROM {{ ref('patient_f_client') }} c
),

households AS (
  SELECT h.uuid, h.chv_area_id
  FROM {{ ref('household') }} h
),

deaths AS (
  SELECT
    d.patient_id,
    d.date_of_death,
    d.patient_age_in_days
  FROM {{ ref('death_report') }} d
),

joined AS (
  SELECT
    hh.chv_area_id AS location_id,
    p.period_id AS period_id,
    cl.patient_id,
    cl.sex,
    cl.date_of_birth,
    cl.muted,
    p.start_date,
    p.end_date,
    AGE(p.end_date, cl.date_of_birth) < INTERVAL '5 years' AS is_u5,
    cl.sex = 'male' AS is_male,
    cl.sex = 'female' AS is_female,
    cl.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' AND cl.muted::DATE BETWEEN p.start_date AND p.end_date AS is_muted,
    cl.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' AND cl.muted::DATE BETWEEN p.start_date AND p.end_date AND AGE(p.end_date, cl.date_of_birth) < INTERVAL '5 years' AS is_u5_muted,
    d.date_of_death IS NOT NULL AND d.date_of_death BETWEEN p.start_date AND p.end_date AS is_dead,
    d.patient_age_in_days
  FROM clients cl
  JOIN households hh ON cl.household_id = hh.uuid
  JOIN periods p ON cl.reported <= p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  LEFT JOIN deaths d ON cl.patient_id = d.patient_id
)
,
metrics AS (
  SELECT
    j.period_id,
    j.location_id,
    j.patient_id,
    unnest(
        ARRAY_REMOVE(ARRAY[
            CASE WHEN COALESCE(NOT is_muted, true) AND COALESCE(NOT is_dead, true) THEN 'population' END,
            CASE WHEN COALESCE(NOT is_muted, true) AND COALESCE(NOT is_dead, true) AND is_male THEN 'population_male' END,
            CASE WHEN COALESCE(NOT is_muted, true) AND COALESCE(NOT is_dead, true) AND is_female THEN 'population_female' END,
            CASE WHEN COALESCE(NOT is_u5_muted, true) AND COALESCE(NOT is_dead, true) AND is_u5 THEN 'under_5_population' END,
            CASE WHEN COALESCE(NOT is_u5_muted, true) AND COALESCE(NOT is_dead, true) AND is_u5 AND is_male THEN 'population_under_5_male' END,
            CASE WHEN COALESCE(NOT is_u5_muted, true) AND COALESCE(NOT is_dead, true) AND is_u5 AND is_female THEN 'population_under_5_female' END
        ], NULL)
        ) AS metric_id
  FROM joined j
)

SELECT period_id,location_id,patient_id,metric_id
FROM metrics
WHERE metric_id IS NOT NULL
