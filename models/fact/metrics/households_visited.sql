-- models/fact/metrics/households_visited.sql
-- Add logic to count unique households visited
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH base_daily_household_visits AS (
    SELECT
        hhvisit.reported_by_parent AS location_id,
        DATE(hhvisit.reported) AS date_id,
        COUNT(DISTINCT hhvisit.household) AS value
    FROM {{ ref('household_visit') }} hhvisit
    WHERE hhvisit.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
      AND hhvisit.household IS NOT NULL
    GROUP BY hhvisit.reported_by_parent, DATE(hhvisit.reported)
),

periods AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

locations AS (
    SELECT DISTINCT location_id FROM base_daily_household_visits
),

aggregated AS (
    SELECT
        l.location_id,
        p.period_id,
        'hh_visited' AS metric_id,
        COALESCE(SUM(b.value), 0) AS value
    FROM periods p
    CROSS JOIN locations l
    LEFT JOIN base_daily_household_visits b
      ON b.location_id = l.location_id AND b.date_id BETWEEN p.start_date AND p.end_date
    GROUP BY l.location_id, p.period_id
    HAVING SUM(b.value) > 0
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
