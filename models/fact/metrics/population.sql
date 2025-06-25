-- models/fact/metrics/population.sql
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH periods AS (
  SELECT * FROM {{ ref('dim_period') }}
),

valid_locations AS (
  SELECT location_id FROM {{ ref('dim_location') }}
),

-- Main population
registrations AS (
  SELECT hh.chv_area_id AS location_id, p.period_id, COUNT(DISTINCT clients.uuid) AS pax_registered
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.reported <= p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  GROUP BY hh.chv_area_id, p.period_id
),

deaths AS (
  SELECT d.reported_by_parent AS location_id, p.period_id, COUNT(DISTINCT d.uuid) AS pax_deaths
  FROM {{ ref('death_report') }} d
  JOIN periods p ON d.date_of_death BETWEEN p.start_date AND p.end_date
  JOIN valid_locations l ON d.reported_by_parent = l.location_id
  GROUP BY d.reported_by_parent, p.period_id
),

muted AS (
  SELECT hh.chv_area_id AS location_id, p.period_id, COUNT(DISTINCT clients.uuid) AS pax_muted
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' AND clients.muted::DATE BETWEEN p.start_date AND p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  WHERE clients.muted IS NOT NULL
  GROUP BY hh.chv_area_id, p.period_id
),

-- Under 5
under_5_raw AS (
  SELECT hh.chv_area_id AS location_id, p.period_id, COUNT(DISTINCT clients.uuid) AS u5_registered
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.date_of_birth IS NOT NULL
                 AND clients.reported <= p.end_date
                 AND AGE(p.end_date, clients.date_of_birth) < INTERVAL '5 years'
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  GROUP BY hh.chv_area_id, p.period_id
),

under_5_deaths AS (
  SELECT d.reported_by_parent AS location_id, p.period_id, COUNT(DISTINCT d.uuid) AS u5_deaths
  FROM {{ ref('death_report') }} d
  JOIN periods p ON d.date_of_death BETWEEN p.start_date AND p.end_date
  JOIN valid_locations l ON d.reported_by_parent = l.location_id
  WHERE d.patient_age_in_days < 1827
  GROUP BY d.reported_by_parent, p.period_id
),

under_5_muted AS (
  SELECT hh.chv_area_id AS location_id, p.period_id, COUNT(DISTINCT clients.uuid) AS u5_muted
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' AND clients.muted::DATE BETWEEN p.start_date AND p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  WHERE clients.muted IS NOT NULL
    AND clients.date_of_birth IS NOT NULL
    AND AGE(p.end_date, clients.date_of_birth) < INTERVAL '5 years'
  GROUP BY hh.chv_area_id, p.period_id
),

population_combined AS (
  SELECT r.location_id, r.period_id, 
         r.pax_registered,
         COALESCE(d.pax_deaths, 0) AS pax_deaths,
         COALESCE(m.pax_muted, 0) AS pax_muted
  FROM registrations r
  LEFT JOIN deaths d ON r.location_id = d.location_id AND r.period_id = d.period_id
  LEFT JOIN muted m ON r.location_id = m.location_id AND r.period_id = m.period_id
),

under_5_combined AS (
  SELECT u.location_id, u.period_id, 
         u.u5_registered,
         COALESCE(d.u5_deaths, 0) AS u5_deaths,
         COALESCE(m.u5_muted, 0) AS u5_muted
  FROM under_5_raw u
  LEFT JOIN under_5_deaths d ON u.location_id = d.location_id AND u.period_id = d.period_id
  LEFT JOIN under_5_muted m ON u.location_id = m.location_id AND u.period_id = m.period_id
)

SELECT
  location_id,
  period_id,
  'population' AS metric_id,
  pax_registered - pax_deaths - pax_muted AS value,
  CURRENT_TIMESTAMP AS last_updated
FROM population_combined
WHERE pax_registered - pax_deaths - pax_muted > 0

UNION ALL

SELECT
  location_id,
  period_id,
  'under_5_population' AS metric_id,
  u5_registered - u5_deaths - u5_muted AS value,
  CURRENT_TIMESTAMP AS last_updated
FROM under_5_combined
WHERE u5_registered - u5_deaths - u5_muted > 0
