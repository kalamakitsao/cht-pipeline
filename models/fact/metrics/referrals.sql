-- models/fact/metrics/referrals.sql
-- Add logic to count total referrals
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH combined_referrals AS (
    SELECT
        reported_by_parent AS location_id,
        DATE(reported) AS report_date,
        patient_id
    FROM {{ ref('u5_assessment') }}
    WHERE has_been_referred = TRUE
      AND reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})

    UNION ALL

    SELECT
        reported_by_parent AS location_id,
        DATE(reported) AS report_date,
        patient_id
    FROM {{ ref('over_five_assessment') }}
    WHERE has_been_referred = TRUE
      AND reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

period_mapped AS (
    SELECT
        r.location_id,
        p.period_id,
        r.patient_id
    FROM combined_referrals r
    JOIN {{ ref('dim_period') }} p
      ON r.report_date BETWEEN p.start_date AND p.end_date
),

aggregated AS (
    SELECT
        location_id,
        period_id,
        COUNT(DISTINCT patient_id) AS value
    FROM period_mapped
    GROUP BY location_id, period_id
)

SELECT
    location_id,
    period_id,
    'total_referrals' AS metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE value > 0
