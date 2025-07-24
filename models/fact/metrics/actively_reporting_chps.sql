-- models/fact/chp_active_reporting_compliance.sql


{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH hh_visits AS (
    SELECT location_id, period_id, SUM(value) AS hh_visited
    FROM {{ ref('households_visited') }}
    WHERE metric_id = 'hh_visited'
    GROUP BY location_id, period_id
),
households_registered AS (
    SELECT location_id, period_id, SUM(value) AS households_registered
    FROM {{ ref('households_registered') }}
    WHERE metric_id = 'households_registered'
    GROUP BY location_id, period_id
),

scored AS (
    SELECT
        v.location_id,
        v.period_id,
        -- Coverage based on households_registered
        CASE
            WHEN v.hh_visited IS NULL OR hr.households_registered IS NULL OR hr.households_registered = 0 THEN 0
            ELSE
                CASE
                    WHEN v.hh_visited::FLOAT / hr.households_registered > 0.165 THEN 1
                    ELSE 0
                END
        END AS coverage_score

    FROM hh_visits v
    LEFT JOIN households_registered hr ON v.location_id = hr.location_id AND v.period_id = hr.period_id
)
)

SELECT
    location_id,
    period_id,
    'active_chps_new' AS metric_id,
    1 AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM scored
WHERE coverage_score = 1
