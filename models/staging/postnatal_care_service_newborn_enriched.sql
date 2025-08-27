-- models/staging/postnatal_care_service_newborn_enriched.sql
{{ config(
  materialized = 'table',
  tags = ['staging','pnc','newborn','cadence_hourly'],
  on_schema_change = 'ignore',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['location_id']},
    {'columns': ['patient_id']},
    {'columns': ['location_id','reported_date']}
  ]
) }}

WITH base AS (
  SELECT
    pnc.uuid,
    pnc.reported,
    (pnc.reported)::date                  AS reported_date,
    pnc.reported_by_parent                AS location_id,
    pnc.patient_id,
    pnc.place_of_birth_display,
    pnc.pnc_service_count,
    COALESCE(pnc.is_referred_immunization, FALSE)      AS is_referred_immunization,
    COALESCE(pnc.needs_danger_signs_follow_up, FALSE)  AS needs_danger_signs_follow_up,
    COALESCE(pnc.needs_missed_visit_follow_up, FALSE)  AS needs_missed_visit_follow_up,
    COALESCE(pnc.is_referred_for_pnc_services, FALSE)  AS is_referred_for_pnc_services,
    COALESCE(pnc.is_referred, FALSE)                   AS is_referred
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'postnatal_care_service_newborn') }} pnc
  WHERE pnc.reported IS NOT NULL
    AND pnc.patient_id IS NOT NULL
)

SELECT * FROM base
