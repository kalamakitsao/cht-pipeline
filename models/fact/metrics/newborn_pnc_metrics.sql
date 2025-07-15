-- models/fact/metrics/newborn_pnc_metrics.sql

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH period_dates AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_newborn_pnc AS (
    SELECT
        pnc.reported_by_parent AS location_id,
        pnc.reported,
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
    WHERE pnc.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

periodized_data AS (
    SELECT
        b.location_id,
        p.period_id,
        b.patient_id,
        b.place_of_birth_display,
        b.pnc_service_count,
        b.is_immunization_defaulter,
        b.is_referred_immunization,
        b.is_referred_for_pnc_services,
        b.needs_danger_signs_follow_up,
        b.needs_missed_visit_follow_up,
        b.is_referred
    FROM base_newborn_pnc b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
),

metric_aggregates AS (
    SELECT location_id, period_id, 'newborn_referred_immunization' AS metric_id,
           COUNT(DISTINCT patient_id) FILTER (WHERE is_referred_immunization) AS value
    FROM periodized_data GROUP BY location_id, period_id

    UNION ALL

    SELECT location_id, period_id, 'newborn_needs_danger_signs_follow_up',
           COUNT(DISTINCT patient_id) FILTER (WHERE needs_danger_signs_follow_up)
    FROM periodized_data GROUP BY location_id, period_id

    UNION ALL

    SELECT location_id, period_id, 'newborn_needs_missed_visit_follow_up',
           COUNT(DISTINCT patient_id) FILTER (WHERE needs_missed_visit_follow_up)
    FROM periodized_data GROUP BY location_id, period_id

    UNION ALL

    SELECT location_id, period_id, 'newborn_home_delivery',
           COUNT(DISTINCT patient_id) FILTER (WHERE place_of_birth_display ILIKE '%home%')
    FROM periodized_data GROUP BY location_id, period_id

    UNION ALL

    SELECT location_id, period_id, 'newborn_pnc_service_count',
           COUNT(DISTINCT patient_id) FILTER (WHERE pnc_service_count IS NOT NULL AND pnc_service_count > 0)
    FROM periodized_data GROUP BY location_id, period_id

    UNION ALL

    SELECT location_id, period_id, 'newborn_any_referred',
           COUNT(DISTINCT patient_id) FILTER (WHERE is_referred OR is_referred_for_pnc_services OR is_referred_immunization)
    FROM periodized_data GROUP BY location_id, period_id
)

SELECT 
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM metric_aggregates