{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['chv_area_id', 'period_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH period_filter AS (
    SELECT
        period_id,
        start_date,
        end_date
    FROM {{ ref('dim_period') }}
),

children_turning_1 AS (
    SELECT
        hh.chv_area_id,
        pax.uuid AS patient_id,
        pax.sex,
        dp.period_id
    FROM {{ ref('patient_f_client') }} pax
    JOIN {{ ref('household') }} hh ON pax.household_id = hh.uuid
    JOIN period_filter dp 
        ON pax.date_of_birth BETWEEN (dp.start_date - INTERVAL '1 year')::date
                                AND (dp.end_date - INTERVAL '1 year')::date
    WHERE pax.muted IS NULL
),

all_immunization_records AS (
    SELECT
        f.reported_by_parent AS location_id,
        f.reported::DATE AS report_date,
        f.patient_id,
        p.sex,
        f.patient_age_in_months,
        f.has_measles_9,
        f.imm_schedule_upto_date,
        f.needs_immunization_referral,
        f.needs_deworming_follow_up,
        f.needs_growth_monitoring_referral
    FROM {{ ref('immunization_status') }} f
    JOIN {{ ref('patient_f_client') }} p ON f.patient_id = p.uuid
    WHERE f.reported_by_parent IN (
        SELECT location_id
        FROM {{ ref('dim_location') }}
        WHERE level = 'chp area'
    )
),

period_map AS (
    SELECT
        p.period_id,
        c.chv_area_id AS location_id,
        c.patient_id,
        unnest(array_remove(array[
            'children_turning_one',
            CASE WHEN c.sex = 'male' THEN 'male_turning_one' END,
            CASE WHEN c.sex = 'female' THEN 'female_turning_one' END,
            CASE WHEN air.has_measles_9 = 'complete' AND air.sex = 'male' THEN 'fully_immunized_male' END,
            CASE WHEN air.has_measles_9 = 'complete' AND air.sex = 'female' THEN 'fully_immunized_female' END,
            CASE WHEN air.needs_immunization_referral = 'true' THEN 'referred_for_immunization' END,
            CASE WHEN air.needs_immunization_referral = 'true' AND air.sex = 'male' THEN 'referred_immunization_male' END,
            CASE WHEN air.needs_immunization_referral = 'true' AND air.sex = 'female' THEN 'referred_immunization_female' END,
            CASE WHEN air.needs_immunization_referral = 'true' AND air.imm_schedule_upto_date IS DISTINCT FROM 'yes' AND air.sex = 'male' THEN 'referred_missed_vaccine_male' END,
            CASE WHEN air.needs_immunization_referral = 'true' AND air.imm_schedule_upto_date IS DISTINCT FROM 'yes' AND air.sex = 'female' THEN 'referred_missed_vaccine_female' END,
            CASE WHEN air.needs_growth_monitoring_referral = 'true' AND air.sex = 'male' THEN 'referred_growth_monitoring_male' END,
            CASE WHEN air.needs_growth_monitoring_referral = 'true' AND air.sex = 'female' THEN 'referred_growth_monitoring_female' END,
            CASE WHEN air.needs_deworming_follow_up = 'yes' AND air.sex = 'male' THEN 'needs_deworming_follow_up_male' END,
            CASE WHEN air.needs_deworming_follow_up = 'yes' AND air.sex = 'female' THEN 'needs_deworming_follow_up_female' END
        ], NULL)) AS metric_id
    FROM children_turning_1 c
    JOIN period_filter p ON c.period_id = p.period_id
    LEFT JOIN all_immunization_records air ON air.patient_id = c.patient_id
)

SELECT * FROM period_map
