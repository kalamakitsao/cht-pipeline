-- models/fact/metrics/deaths.sql
-- Add logic to compute maternal, neonatal, child, and total deaths
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH death_data AS (
    SELECT
        dr.reported_by_parent AS location_id,
        dr.reported::DATE AS reported_date,
        dr.patient_id,
        dr.death_type,
        dr.patient_age_in_days
    FROM {{ ref('death_report') }} dr
    WHERE dr.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

deaths_with_period AS (
    SELECT
        d.location_id,
        p.period_id,
        d.death_type,
        d.patient_age_in_days,
        d.patient_id
    FROM death_data d
    JOIN {{ ref('dim_period') }} p
      ON d.reported_date BETWEEN p.start_date AND p.end_date
),

aggregated AS (
    SELECT
        location_id,
        period_id,
        'maternal_deaths' AS metric_id,
        COUNT(*) FILTER (WHERE death_type = 'maternal death') AS value
    FROM deaths_with_period
    GROUP BY location_id, period_id

    UNION ALL

    SELECT
        location_id,
        period_id,
        'neonatal_deaths' AS metric_id,
        COUNT(*) FILTER (WHERE patient_age_in_days < 29)
    FROM deaths_with_period
    GROUP BY location_id, period_id

    UNION ALL

    SELECT
        location_id,
        period_id,
        'child_deaths' AS metric_id,
        COUNT(*) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827)
    FROM deaths_with_period
    GROUP BY location_id, period_id

    UNION ALL

    SELECT
        location_id,
        period_id,
        'total_deaths' AS metric_id,
        COUNT(DISTINCT patient_id)
    FROM deaths_with_period
    GROUP BY location_id, period_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE value > 0
