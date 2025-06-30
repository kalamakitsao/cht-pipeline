-- models/fact/metrics/revised_active_chps.sql

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

county_location AS (
    SELECT
        county.name AS county,
        chp_area.location_id
    FROM {{ ref('dim_location') }} chp_area
    LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp_area.parent_id
    LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
    LEFT JOIN {{ ref('dim_location') }} county ON county.location_id = sub.parent_id
    WHERE chp_area.level = 'chp area'
      AND chp_area.name !~ '^[0-9]+$'
    GROUP BY county.name, chp_area.location_id
),

expected_workload AS (
    SELECT
        loc.location_id,
        e.county,
        e.expected_households_per_month
    FROM {{ ref('chp_expected_workload') }} e
    JOIN county_location loc ON loc.county = e.county
),

referrals AS (
    SELECT location_id, period_id, SUM(value) AS total_referrals
    FROM {{ ref('referrals') }}
    WHERE metric_id = 'total_referrals'
    GROUP BY location_id, period_id
),

u5_assessed AS (
    SELECT location_id, period_id, SUM(value) AS u5
    FROM {{ ref('u5_conditions') }}
    WHERE metric_id = 'u5_assessed'
    GROUP BY location_id, period_id
),

over_5_assessed AS (
    SELECT location_id, period_id, SUM(value) AS over5
    FROM {{ ref('ncd_metrics') }}
    WHERE metric_id = 'over_5_assessments'
    GROUP BY location_id, period_id
),

community_events AS (
    SELECT location_id, period_id,
           MAX(CASE WHEN metric_id = 'monthly_cu_meetings' THEN value ELSE 0 END) AS cu_meetings,
           MAX(CASE WHEN metric_id = 'other_community_events' THEN value ELSE 0 END) AS other_events
    FROM {{ ref('community_events_participation') }}
    GROUP BY location_id, period_id
),

scored AS (
    SELECT
        v.location_id,
        v.period_id,
        -- Coverage
        CASE
            WHEN v.hh_visited IS NULL OR w.expected_households_per_month IS NULL OR w.expected_households_per_month = 0 THEN 0
            ELSE
                CASE
                    WHEN v.hh_visited::FLOAT / w.expected_households_per_month <= 0.25 THEN 10
                    WHEN v.hh_visited::FLOAT / w.expected_households_per_month <= 0.5 THEN 20
                    WHEN v.hh_visited::FLOAT / w.expected_households_per_month <= 0.75 THEN 30
                    ELSE 40
                END
        END AS coverage_score,

        CASE WHEN r.total_referrals >= 1 THEN 10 ELSE 0 END AS referral_score,
        CASE WHEN u.u5 >= 1 THEN 20 ELSE 0 END + CASE WHEN o.over5 >= 1 THEN 20 ELSE 0 END AS assessment_score,
        CASE WHEN ce.cu_meetings >= 1 THEN 5 ELSE 0 END + CASE WHEN ce.other_events >= 1 THEN 5 ELSE 0 END AS events_score

    FROM hh_visits v
    LEFT JOIN expected_workload w ON v.location_id = w.location_id
    LEFT JOIN referrals r ON v.location_id = r.location_id AND v.period_id = r.period_id
    LEFT JOIN u5_assessed u ON v.location_id = u.location_id AND v.period_id = u.period_id
    LEFT JOIN over_5_assessed o ON v.location_id = o.location_id AND v.period_id = o.period_id
    LEFT JOIN community_events ce ON v.location_id = ce.location_id AND v.period_id = ce.period_id
),

scored_with_total AS (
    SELECT *,
           coverage_score + referral_score + assessment_score + events_score AS total_score
    FROM scored
)

SELECT
    location_id,
    period_id,
    'revised_active_chps' AS metric_id,
    1 AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM scored_with_total
WHERE total_score >= 80
