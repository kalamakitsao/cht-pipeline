-- models/fact/metrics/sha_metrics.sql
-- Add logic to compute households_assessed_sha and households_with_sha
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH latest_sha_registration AS (
    SELECT DISTINCT ON (c.household_id)
        c.household_id,
        sr.reported_by_parent AS location_id,
        DATE(sr.reported) AS date_id,
        sr.has_sha_registration
    FROM {{ ref('sha_registration') }} sr
    INNER JOIN {{ ref('patient_f_client') }} c ON sr.member_uuid = c.uuid
    WHERE sr.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
    ORDER BY c.household_id, sr.reported DESC
),

daily_metrics AS (
    SELECT
        location_id,
        date_id,
        COUNT(*) AS households_assessed_sha,
        COUNT(*) FILTER (WHERE has_sha_registration IS TRUE) AS households_with_sha
    FROM latest_sha_registration
    GROUP BY location_id, date_id
),

periods AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

unpivoted AS (
    SELECT
        d.location_id,
        p.period_id,
        'households_assessed_sha' AS metric_id,
        COALESCE(SUM(d.households_assessed_sha), 0) AS value
    FROM periods p
    JOIN daily_metrics d ON d.date_id BETWEEN p.start_date AND p.end_date
    GROUP BY d.location_id, p.period_id

    UNION ALL

    SELECT
        d.location_id,
        p.period_id,
        'households_with_sha' AS metric_id,
        COALESCE(SUM(d.households_with_sha), 0) AS value
    FROM periods p
    JOIN daily_metrics d ON d.date_id BETWEEN p.start_date AND p.end_date
    GROUP BY d.location_id, p.period_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM unpivoted
WHERE value > 0
