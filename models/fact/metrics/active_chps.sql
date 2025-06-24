-- models/fact/metrics/active_chps.sql
-- Add logic to count CHPs who conducted household visits
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH visits_per_day AS (
    SELECT DISTINCT
        reported_by_parent AS location_id,
        DATE(reported) AS visit_date
    FROM {{ ref('household_visit') }}
    WHERE reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

periods AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

period_mapped AS (
    SELECT
        v.location_id,
        p.period_id
    FROM visits_per_day v
    JOIN periods p ON v.visit_date BETWEEN p.start_date AND p.end_date
),

aggregated AS (
    SELECT location_id, period_id, COUNT(*) AS value
    FROM period_mapped
    GROUP BY location_id, period_id
)

SELECT
    location_id,
    period_id,
    'active_chps' AS metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
