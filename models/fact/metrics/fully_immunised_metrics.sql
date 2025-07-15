-- models/fact/fully_immunised_metrics.sql

{{ config(
    materialized = 'table',
    unique_key = ['chv_area_id', 'period_id', 'metric_id']
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

latest_immunization_status AS (
    SELECT
        imm.patient_id,
        imm.location_id,
        imm.report_date,
        imm.has_measles_9,
        imm.sex,
        imm.patient_age_in_months,
        imm.imm_schedule_upto_date,
        ROW_NUMBER() OVER (
            PARTITION BY imm.patient_id
            ORDER BY imm.report_date DESC
        ) AS rn
    FROM all_immunization_records imm
    WHERE imm.patient_id IN (SELECT patient_id FROM children_turning_1)
),

male_turning_one AS (
    SELECT
        chv_area_id,
        period_id,
        'male_turning_one' AS metric_id,
        COUNT(patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM children_turning_1
    WHERE sex = 'male'
    GROUP BY chv_area_id, period_id
),

female_turning_one AS (
    SELECT
        chv_area_id,
        period_id,
        'female_turning_one' AS metric_id,
        COUNT(patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM children_turning_1
    WHERE sex = 'female'
    GROUP BY chv_area_id, period_id
),

children_turning_one AS (
    SELECT
        chv_area_id,
        period_id,
        'children_turning_one' AS metric_id,
        COUNT(patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM children_turning_1
    GROUP BY chv_area_id, period_id
),

fully_immunized_male AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'fully_immunized_male' AS metric_id,
        COUNT(lis.patient_id) FILTER (WHERE lis.sex = 'male' AND lis.has_measles_9 = 'complete') AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM latest_immunization_status lis
    JOIN children_turning_1 ct1 ON lis.patient_id = ct1.patient_id
    WHERE lis.rn = 1
    GROUP BY ct1.chv_area_id, ct1.period_id
),

fully_immunized_female AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'fully_immunized_female' AS metric_id,
        COUNT(lis.patient_id) FILTER (WHERE lis.sex = 'female' AND lis.has_measles_9 = 'complete') AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM latest_immunization_status lis
    JOIN children_turning_1 ct1 ON lis.patient_id = ct1.patient_id
    WHERE lis.rn = 1
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_for_immunization AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_for_immunization' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_immunization_referral = 'true'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_immunization_male AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_immunization_male' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_immunization_referral = 'true' AND air.sex = 'male'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_immunization_female AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_immunization_female' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_immunization_referral = 'true' AND air.sex = 'female'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_missed_vaccine_male AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_missed_vaccine_male' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_immunization_referral = 'true' AND COALESCE(air.imm_schedule_upto_date, 'no') <> 'yes' AND air.sex = 'male'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_missed_vaccine_female AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_missed_vaccine_female' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_immunization_referral = 'true' AND COALESCE(air.imm_schedule_upto_date, 'no') <> 'yes' AND air.sex = 'female'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_growth_monitoring_male AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_growth_monitoring_male' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_growth_monitoring_referral = 'true' AND air.sex = 'male'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

referred_growth_monitoring_female AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'referred_growth_monitoring_female' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_growth_monitoring_referral = 'true' AND air.sex = 'female'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

needs_deworming_follow_up_male AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'needs_deworming_follow_up_male' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_deworming_follow_up = 'yes' AND air.sex = 'male'
    GROUP BY ct1.chv_area_id, ct1.period_id
),

needs_deworming_follow_up_female AS (
    SELECT
        ct1.chv_area_id,
        ct1.period_id,
        'needs_deworming_follow_up_female' AS metric_id,
        COUNT(DISTINCT air.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM all_immunization_records air
    JOIN children_turning_1 ct1 ON air.patient_id = ct1.patient_id
    WHERE air.needs_deworming_follow_up = 'yes' AND air.sex = 'female'
    GROUP BY ct1.chv_area_id, ct1.period_id
)

-- Final union
SELECT * FROM male_turning_one
UNION ALL SELECT * FROM female_turning_one
UNION ALL SELECT * FROM children_turning_one
UNION ALL SELECT * FROM fully_immunized_male
UNION ALL SELECT * FROM fully_immunized_female
UNION ALL SELECT * FROM referred_for_immunization
UNION ALL SELECT * FROM referred_immunization_male
UNION ALL SELECT * FROM referred_immunization_female
UNION ALL SELECT * FROM referred_missed_vaccine_male
UNION ALL SELECT * FROM referred_missed_vaccine_female
UNION ALL SELECT * FROM referred_growth_monitoring_male
UNION ALL SELECT * FROM referred_growth_monitoring_female
UNION ALL SELECT * FROM needs_deworming_follow_up_male
UNION ALL SELECT * FROM needs_deworming_follow_up_female