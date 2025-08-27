{{ config(
    materialized = "table",
    unique_key = ["patient_id", "period_id"],
    tags = ['cadence_hourly']
) }}

SELECT
    pax.chp_area_id,
    pax.uuid AS patient_id,
    pax.sex,
    dp.period_id
FROM {{ ref('household_person_enriched') }} pax
JOIN {{ ref('dim_period') }} dp
    ON pax.date_of_birth BETWEEN (dp.start_date - INTERVAL '1 year')::date
                                AND (dp.end_date   - INTERVAL '1 year')::date
WHERE pax.muted IS NULL
