-- models/fact/period_maps/fact_period_patient_map_newborn.sql

{{ config(
    materialized = 'incremental',
    on_schema_change = 'append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_newborn_pnc AS (
  SELECT
    pnc.reported_by_parent AS location_id,
    DATE(pnc.reported) AS reported_date,
    pnc.patient_id,
    pnc.place_of_birth_display,
    pnc.pnc_service_count,
    COALESCE(pnc.is_immunization_defaulter, FALSE) AS is_immunization_defaulter,
    COALESCE(pnc.is_referred_immunization, FALSE) AS is_referred_immunization,
    COALESCE(pnc.is_referred_for_pnc_services, FALSE) AS is_referred_for_pnc_services,
    COALESCE(pnc.needs_danger_signs_follow_up, FALSE) AS needs_danger_signs_follow_up,
    COALESCE(pnc.needs_missed_visit_follow_up, FALSE) AS needs_missed_visit_follow_up,
    COALESCE(pnc.is_referred, FALSE) AS is_referred
  FROM {{ ref('postnatal_care_service_newborn') }} pnc
  WHERE pnc.patient_id IS NOT NULL
),

newborn_mapped AS (
  SELECT
    p.period_id,
    b.location_id,
    b.patient_id,
    unnest(
      ARRAY_REMOVE(ARRAY[
        CASE WHEN b.is_referred_immunization THEN 'newborn_referred_immunization' END,
        CASE WHEN b.needs_danger_signs_follow_up THEN 'newborn_needs_danger_signs_follow_up' END,
        CASE WHEN b.needs_missed_visit_follow_up THEN 'newborn_needs_missed_visit_follow_up' END,
        CASE WHEN b.place_of_birth_display ILIKE '%home%' THEN 'newborn_home_delivery' END,
        CASE WHEN b.pnc_service_count IS NOT NULL AND b.pnc_service_count > 0 THEN 'newborn_pnc_service_count' END,
        CASE WHEN b.is_referred OR b.is_referred_for_pnc_services OR b.is_referred_immunization THEN 'newborn_any_referred' END
      ], NULL)
    ) AS metric_id
  FROM base_newborn_pnc b
  JOIN periods p ON b.reported_date BETWEEN p.start_date AND p.end_date
)

SELECT * FROM newborn_mapped
