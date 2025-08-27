-- models/staging/death_report_enriched.sql
{{ config(
  materialized = 'table',
  tags = ['staging','death','cadence_hourly'],
  on_schema_change = 'ignore',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['reported_by_parent']},
    {'columns': ['patient_age_in_days']},
    {'columns': ['sex']},
    {'columns': ['reported_by_parent','reported_date']}
  ]
) }}

WITH base AS (
  SELECT
    d.*,                       -- includes uuid, reported, patient_age_* fields, death_type, patient_id, etc.
    (d.reported)::date AS reported_date
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'death_report') }} d
  -- keep to CHP areas to prune early
  JOIN {{ ref('dim_location') }} dl
    ON d.reported_by_parent = dl.location_id
   AND dl.level = 'chp area'
  WHERE d.uuid IS NOT NULL
),

enriched AS (
  SELECT
    b.uuid,
    b.reported,
    b.reported_date,
    b.reported_by_parent,
    b.patient_id,
    b.patient_age_in_days,
    b.death_type,
    b.place_of_death,
    b.date_of_death,
    ps.sex
  FROM base b
  LEFT JOIN {{ ref('dim_patient_sex') }} ps
    ON b.patient_id = ps.uuid
)

SELECT * FROM enriched
