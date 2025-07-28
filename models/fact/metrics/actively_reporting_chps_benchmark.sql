{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    tags = ['kpi', 'chp_compliance'],
    on_schema_change = 'ignore'
) }}

WITH hh_visits AS (
    SELECT
        location_id,
        period_id,
        SUM(value) AS hh_visited
    FROM {{ ref('households_visited') }}
    WHERE metric_id = 'hh_visited' AND period_id IN ('4', '11')
    GROUP BY location_id, period_id
),

households_registered AS (
    SELECT
        location_id,
        period_id,
        SUM(value) AS households_registered
    FROM {{ ref('households_registered') }}
    WHERE metric_id = 'households_registered' AND period_id IN ('4', '11')
    GROUP BY location_id, period_id
),

scored AS (
    SELECT
        v.location_id,
        v.period_id,
        CASE
            WHEN hr.households_registered IS NULL OR hr.households_registered = 0 THEN 0
            WHEN v.hh_visited IS NULL THEN 0
            WHEN v.hh_visited::FLOAT / hr.households_registered > 0.165 THEN 1
            ELSE 0
        END AS is_active
    FROM hh_visits v
    LEFT JOIN households_registered hr
      ON v.location_id = hr.location_id AND v.period_id = hr.period_id
)

SELECT
    location_id,
    period_id,
    'active_chps_new' AS metric_id,
    1 AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM scored
WHERE is_active = 1
