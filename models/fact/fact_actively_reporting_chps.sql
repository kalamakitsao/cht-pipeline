{{ config(
    materialized = 'table',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    tags = ['kpi', 'cadence_hourly'],
    on_schema_change = 'ignore'
) }}

WITH hh_visits AS (
    SELECT
        location_id,
        hv.period_id,
        p.period_id_name,
        SUM(value) AS hh_visited
    FROM {{ ref('fact_households_visited_union') }} hv
        JOIN {{ ref('dim_period') }} p ON hv.period_id = p.period_id
    WHERE metric_id = 'hh_visited'
        AND period_id_name IN ('today','yesterday','last_7_days','last_1_month','last_3_months','last_6_months', 'this_week', 'this_month', 'last_month', 'this_quarter', 'last_quarter','last_6_months') -- ('4', '11')
    GROUP BY location_id, hv.period_id, period_id_name
),

households_registered AS (
    SELECT
        location_id,
        hr.period_id,
        p.period_id_name,
        SUM(value) AS households_registered
    FROM {{ ref('fact_households_registered') }} hr
        JOIN {{ ref('dim_period') }} p ON hr.period_id = p.period_id
    WHERE metric_id = 'households_registered' 
        AND period_id_name IN ('today','yesterday','last_7_days','last_1_month','last_3_months','last_6_months', 'this_week', 'this_month', 'last_month', 'this_quarter', 'last_quarter', 'last_6_months') -- ('4', '11')period_id IN ('4', '11')
    GROUP BY location_id, hr.period_id, period_id_name
),

scored AS (
    SELECT
        v.location_id,
        v.period_id,
        CASE
            WHEN hr.households_registered IS NULL OR hr.households_registered = 0 THEN 0
            WHEN v.hh_visited IS NULL THEN 0
            WHEN v.hh_visited::FLOAT / hr.households_registered > 0.165 
                AND hr.period_id_name IN ('last_1_month','this_month', 'last_month') THEN 1
            WHEN v.hh_visited::FLOAT / hr.households_registered > 0.5 
                AND hr.period_id_name IN ('last_3_months','this_quarter', 'last_quarter') THEN 1
            WHEN v.hh_visited::FLOAT / hr.households_registered > 0.75 
                AND hr.period_id_name IN ('last_6_months') THEN 1
            WHEN v.hh_visited::FLOAT > 0
                AND hr.period_id_name IN ('today','yesterday', 'last_7_days', 'this_week') THEN 1
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