-- models/fact/metrics/ncd_metrics.sql
-- Add logic to count screenings and referrals for diabetes and hypertension
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH filtered_data AS (
    SELECT
        o.reported_by_parent AS location_id,
        o.reported::DATE AS reported_date,
        o.patient_id,
        o.screened_for_diabetes,
        o.is_referred_diabetes,
        o.screened_for_hypertension,
        o.is_referred_hypertension
    FROM {{ ref('over_five_assessment') }} o
    WHERE o.reported_by_parent IN (
        SELECT location_id FROM {{ ref('dim_location') }} WHERE level = 'chp area'
    )
),

unpivoted AS (
    SELECT location_id, reported_date, patient_id, 'screened_diabetes' AS metric_id
    FROM filtered_data
    WHERE screened_for_diabetes IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_diabetes' AS metric_id
    FROM filtered_data
    WHERE is_referred_diabetes IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_hypertension' AS metric_id
    FROM filtered_data
    WHERE screened_for_hypertension IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_hypertension' AS metric_id
    FROM filtered_data
    WHERE is_referred_hypertension IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'over_5_assessments' AS metric_id
    FROM filtered_data
),

with_periods AS (
    SELECT
        u.location_id,
        p.period_id,
        u.metric_id,
        COUNT(DISTINCT u.patient_id) AS value
    FROM unpivoted u
    JOIN {{ ref('dim_period') }} p ON u.reported_date BETWEEN p.start_date AND p.end_date
    GROUP BY u.location_id, p.period_id, u.metric_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM with_periods
WHERE value > 0
