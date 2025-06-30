-- models/fact/metrics/population_male.sql
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

registrations AS (
  SELECT hh.chv_area_id AS location_id, p.period_id, COUNT(DISTINCT clients.uuid) AS pax_registered
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.reported <= p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  WHERE clients.sex = 'male'
  GROUP BY hh.chv_area_id, p.period_id
),

deaths AS (
  SELECT d.reported_by_parent AS location_id, p.period_id, COUNT(DISTINCT d.uuid) AS pax_deaths
  FROM {{ ref('death_report') }} d
  JOIN periods p ON d.date_of_death BETWEEN p.start_date AND p.end_date
  JOIN valid_locations l ON d.reported_by_parent = l.location_id
  JOIN {{ ref('patient_f_client') }} pax ON d.patient_id = pax.uuid
  WHERE pax.sex = 'male'
  GROUP BY d.reported_by_parent, p.period_id
),

muted AS (
  SELECT hh.chv_area_id AS location_id, p.period_id, COUNT(DISTINCT clients.uuid) AS pax_muted
  FROM {{ ref('patient_f_client') }} clients
  JOIN {{ ref('household') }} hh ON clients.household_id = hh.uuid
  JOIN periods p ON clients.muted ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' AND clients.muted::DATE BETWEEN p.start_date AND p.end_date
  JOIN valid_locations l ON hh.chv_area_id = l.location_id
  WHERE clients.muted IS NOT NULL
   AND clients.sex = 'male'
  GROUP BY hh.chv_area_id, p.period_id
),

aggregated AS (
  SELECT r.location_id, r.period_id, 'population' AS metric_id,
         pax_registered - COALESCE(d.pax_deaths, 0) - COALESCE(m.pax_muted, 0) AS value
  FROM registrations r
  LEFT JOIN deaths d ON r.location_id = d.location_id AND r.period_id = d.period_id
  LEFT JOIN muted m ON r.location_id = m.location_id AND r.period_id = m.period_id
  WHERE pax_registered - COALESCE(d.pax_deaths, 0) - COALESCE(m.pax_muted, 0) > 0
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM aggregated