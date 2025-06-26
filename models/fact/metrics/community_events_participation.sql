-- models/fact/metrics/community_events_participation.sql
-- Add logic to compute attendance of community events
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH community_events AS (
    SELECT
        ce.reported_by_parent AS location_id,
        ce.reported::DATE AS reported_date,
        ce.event_types,
        ce.event_date
    FROM {{ ref('community_event') }} ce
    WHERE ce.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

community_events_with_period AS (
    SELECT
        c.location_id,
        p.period_id,
        c.event_types,
        c.event_date
    FROM community_events c
    JOIN {{ ref('dim_period') }} p
      ON c.event_date BETWEEN p.start_date AND p.end_date
),

aggregated AS (
    SELECT
        location_id,
        period_id,
        'monthly_cu_meetings' AS metric_id,
        COUNT(*) FILTER (WHERE event_types ilike '%monthly_cu_meetings') AS value
    FROM community_events_with_period
    GROUP BY location_id, period_id

    UNION ALL

    SELECT
    location_id,
    period_id,
    'other_community_events' AS metric_id,
    COUNT(*) FILTER (
        WHERE event_types ILIKE '%monthly_cu_meetings%' AND event_types <> 'monthly_cu_meetings'
           OR event_types NOT ILIKE '%monthly_cu_meetings%'
        ) AS value
    FROM community_events_with_period
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
