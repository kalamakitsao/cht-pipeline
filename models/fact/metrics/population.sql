-- models/fact/metrics/population.sql
{{ config(
    materialized = 'incremental',
    unique_key = ['snapshot_date', 'location_id', 'period_id', 'metric_id'],
    on_schema_change='append_new_columns'
) }}

WITH periods AS (
  SELECT * FROM {{ ref('dim_period') }}
),

valid_locations AS (
  SELECT location_id FROM {{ ref('dim_location') }}
),

clients AS (
  SELECT
    c.uuid,
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
    d.uuid,
    d.patient_id,
    d.reported_by_parent AS location_id,
    d.date_of_death,
    d.patient_age_in_days
  FROM {{ ref('death_report') }} d
),

base AS (
  SELECT
    hh.chv_area_id AS location_id,
    p.period_id,
    cl.uuid AS client_id,
    cl.sex,
    cl.date_of_birth,
    cl.muted,
    cl.reported,
    AGE(p.end_date, cl.date_of_birth) < INTERVAL '5 years' AS is_u5,
    cl.sex = 'male' AS is_male,
    cl.sex = 'female' AS is_female,
    cl.muted ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' AND cl.muted::DATE BETWEEN p.start_date AND p.end_date AS is_muted,
    cl.muted ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' AND cl.muted::DATE BETWEEN p.start_date AND p.end_date AND AGE(p.end_date, cl.date_of_birth) < INTERVAL '5 years' AS is_u5_muted
  FROM clients cl
  JOIN households hh ON cl.household_id = hh.uuid
  JOIN periods p ON cl.reported <= p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
),

joined_deaths AS (
  SELECT
    d.uuid,
    d.location_id,
    p.period_id,
    d.patient_id,
    d.patient_age_in_days,
    c.sex
  FROM deaths d
  JOIN periods p ON d.date_of_death BETWEEN p.start_date AND p.end_date
  LEFT JOIN {{ ref('patient_f_client') }} c ON d.patient_id = c.uuid
  JOIN valid_locations l ON d.location_id = l.location_id
),

aggregated AS (

  SELECT b.location_id, b.period_id, 'population' AS metric_id,
         COUNT(DISTINCT client_id) 
         - COUNT(DISTINCT CASE WHEN is_muted THEN client_id END)
         - COUNT(DISTINCT jd.uuid) FILTER (WHERE jd.location_id = b.location_id AND jd.period_id = b.period_id) AS value
  FROM base b
  LEFT JOIN joined_deaths jd ON b.client_id = jd.patient_id AND jd.location_id = b.location_id AND jd.period_id = b.period_id
  GROUP BY b.location_id, b.period_id

  UNION ALL

  SELECT b.location_id, b.period_id, 'population_male',
         COUNT(DISTINCT CASE WHEN is_male THEN client_id END)
         - COUNT(DISTINCT CASE WHEN is_male AND is_muted THEN client_id END)
         - COUNT(DISTINCT jd.uuid) FILTER (WHERE jd.sex = 'male') 
  FROM base b
  LEFT JOIN joined_deaths jd ON b.client_id = jd.patient_id AND jd.location_id = b.location_id AND jd.period_id = b.period_id
  GROUP BY b.location_id, b.period_id

  UNION ALL

  SELECT b.location_id, b.period_id, 'population_female',
         COUNT(DISTINCT CASE WHEN is_female THEN client_id END)
         - COUNT(DISTINCT CASE WHEN is_female AND is_muted THEN client_id END)
         - COUNT(DISTINCT jd.uuid) FILTER (WHERE jd.sex = 'female')
  FROM base b
  LEFT JOIN joined_deaths jd ON b.client_id = jd.patient_id AND jd.location_id = b.location_id AND jd.period_id = b.period_id
  GROUP BY b.location_id, b.period_id

  UNION ALL

  SELECT b.location_id, b.period_id, 'under_5_population',
         COUNT(DISTINCT CASE WHEN is_u5 THEN client_id END)
         - COUNT(DISTINCT CASE WHEN is_u5 AND is_u5_muted THEN client_id END)
         - COUNT(DISTINCT jd.uuid) FILTER (WHERE jd.patient_age_in_days < 1827)
  FROM base b
  LEFT JOIN joined_deaths jd ON b.client_id = jd.patient_id AND jd.location_id = b.location_id AND jd.period_id = b.period_id
  GROUP BY b.location_id, b.period_id

  UNION ALL

  SELECT b.location_id, b.period_id, 'population_under_5_male',
         COUNT(DISTINCT CASE WHEN is_u5 AND is_male THEN client_id END)
         - COUNT(DISTINCT CASE WHEN is_u5 AND is_male AND is_u5_muted THEN client_id END)
         - COUNT(DISTINCT jd.uuid) FILTER (WHERE jd.sex = 'male' AND jd.patient_age_in_days < 1827)
  FROM base b
  LEFT JOIN joined_deaths jd ON b.client_id = jd.patient_id AND jd.location_id = b.location_id AND jd.period_id = b.period_id
  GROUP BY b.location_id, b.period_id

  UNION ALL

  SELECT b.location_id, b.period_id, 'population_under_5_female',
         COUNT(DISTINCT CASE WHEN is_u5 AND is_female THEN client_id END)
         - COUNT(DISTINCT CASE WHEN is_u5 AND is_female AND is_u5_muted THEN client_id END)
         - COUNT(DISTINCT jd.uuid) FILTER (WHERE jd.sex = 'female' AND jd.patient_age_in_days < 1827)
  FROM base b
  LEFT JOIN joined_deaths jd ON b.client_id = jd.patient_id AND jd.location_id = b.location_id AND jd.period_id = b.period_id
  GROUP BY b.location_id, b.period_id
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE value > 0
