{{ config(
    materialized = "table",
    unique_key = ["patient_id", "report_date"]
) }}

SELECT
    i.reported_by_parent AS location_id,
    i.reported AS report_date,
    i.patient_id,
    c.sex,
    i.patient_age_in_months,
    i.has_measles_9,
    (i.imm_schedule_upto_date)::boolean,
    (i.needs_immunization_referral)::boolean,
    (i.needs_deworming_follow_up)::boolean,
    (i.needs_growth_monitoring_referral)::boolean,
    c.chp_area_id,
    c.period_id
FROM {{ source(env_var('POSTGRES_SCHEMA'), 'immunization_status') }} i
JOIN {{ ref('children_turning_one') }} c
  ON i.patient_id = c.patient_id
