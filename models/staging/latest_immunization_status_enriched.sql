{{ config(
    materialized = "table",
    unique_key = ["patient_id", "period_id"],
    indexes = [
        {'columns': ['patient_id', 'period_id'], 'unique': true},
        {'columns': ['period_id']},
        {'columns': ['chp_area_id']},
        {'columns': ['location_id']}
    ],
    tags = ['cadence_hourly']
) }}

WITH periodized AS (
    SELECT
        i.location_id,
        i.report_date,
        i.patient_id,
        i.sex,
        i.patient_age_in_months,
        i.has_measles_9,
        i.imm_schedule_upto_date,
        i.needs_immunization_referral,
        i.needs_deworming_follow_up,
        i.needs_growth_monitoring_referral,
        i.chp_area_id,
        dp.period_id,
        dp.end_date
    FROM {{ ref('immunization_records_enriched') }} i
    JOIN {{ ref('dim_period') }} dp
      ON i.report_date BETWEEN dp.start_date AND dp.end_date
    WHERE dp.period_id_name <> 'today'
),

ranked AS (
    SELECT
        p.*,
        ROW_NUMBER() OVER (
            PARTITION BY p.patient_id, p.period_id
            ORDER BY p.report_date DESC, p.end_date DESC
        ) AS rn
    FROM periodized p
)

SELECT
    location_id,
    report_date,
    patient_id,
    sex,
    patient_age_in_months,
    has_measles_9,
    imm_schedule_upto_date,
    needs_immunization_referral,
    needs_deworming_follow_up,
    needs_growth_monitoring_referral,
    chp_area_id,
    period_id
FROM ranked
WHERE rn = 1
