{{ config(
    materialized = "table",
    unique_key = ["patient_id", "period_id"],
    tags = ['cadence_hourly']
) }}

WITH ranked AS (
    SELECT
        i.*,
        ROW_NUMBER() OVER (
            PARTITION BY i.patient_id, i.period_id
            ORDER BY i.report_date DESC
        ) AS rn
    FROM {{ ref('immunization_records_enriched') }} i
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
