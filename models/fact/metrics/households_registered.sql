-- models/fact/metrics/households_registered.sql
-- Add logic to count total registered households
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH base_daily_households_registered AS (
    SELECT
        hh.chv_area_id AS location_id,
        DATE(hh.reported) AS date_id,
        COUNT(hh.uuid) AS value
    FROM {{ ref('household') }} hh
    WHERE hh.chv_area_id IN (SELECT location_id FROM {{ ref('dim_location') }})
    GROUP BY hh.chv_area_id, DATE(hh.reported)
),

periods AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

locations AS (
    SELECT DISTINCT location_id FROM base_daily_households_registered
),

metrics AS (
    SELECT 'households_registered'::TEXT AS metric_id
),

aggregated AS (
    SELECT
        l.location_id,
        p.period_id,
        m.metric_id,
        COALESCE(SUM(b.value), 0) AS value
    FROM periods p
    CROSS JOIN locations l
    CROSS JOIN metrics m
    LEFT JOIN base_daily_households_registered b
      ON b.location_id = l.location_id AND b.date_id <= p.end_date
    GROUP BY l.location_id, p.period_id, m.metric_id
    HAVING SUM(b.value) > 0
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
