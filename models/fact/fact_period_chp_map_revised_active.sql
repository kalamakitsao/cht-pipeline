{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'chp_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

hh_visits AS (
    SELECT period_id, location_id, SUM(value) AS hh_visited
    FROM {{ ref('agg_households_visited_metrics_rolling') }}
    WHERE metric_id = 'hh_visited'
    GROUP BY location_id, period_id
),

referrals AS (
    SELECT period_id, location_id, SUM(value) AS total_referrals
    FROM {{ ref('agg_referral_metrics_rolling') }}
    WHERE metric_id = 'total_referrals'
    GROUP BY location_id, period_id
),

u5_assessed AS (
    SELECT period_id, location_id, SUM(value) AS u5
    FROM {{ ref('agg_u5_metrics_rolling') }}
    WHERE metric_id = 'u5_assessed'
    GROUP BY location_id, period_id
),

over_5_assessed AS (
    SELECT period_id, location_id, SUM(value) AS over5
    FROM {{ ref('agg_over_five_metrics_rolling') }}
    WHERE metric_id = 'over_5_assessments'
    GROUP BY location_id, period_id
),

community_events AS (
    SELECT period_id, location_id,
           MAX(CASE WHEN metric_id = 'monthly_cu_meetings' THEN value ELSE 0 END) AS cu_meetings,
           MAX(CASE WHEN metric_id = 'other_community_events' THEN value ELSE 0 END) AS other_events
    FROM {{ ref('agg_community_events_metrics_rolling') }}
    GROUP BY location_id, period_id
),

households_registered AS (
    SELECT period_id, location_id, SUM(value) AS households_registered
    FROM {{ ref('agg_households_registered_and_chp_counts_rolling') }}
    WHERE metric_id = 'households_registered'
    GROUP BY location_id, period_id
),

scored AS (
    SELECT
        v.location_id,
        v.period_id,
        -- Use location_id as chp_id here for now (or replace with real CHPs table)
        v.location_id AS chp_id,

        CASE
            WHEN v.hh_visited IS NULL OR hr.households_registered IS NULL OR hr.households_registered = 0 THEN 0
            ELSE
                CASE
                    WHEN v.hh_visited::FLOAT / hr.households_registered <= 0.25 THEN 10
                    WHEN v.hh_visited::FLOAT / hr.households_registered <= 0.5 THEN 20
                    WHEN v.hh_visited::FLOAT / hr.households_registered <= 0.75 THEN 30
                    ELSE 40
                END
        END AS coverage_score,

        CASE WHEN r.total_referrals >= 1 THEN 10 ELSE 0 END AS referral_score,
        CASE WHEN u.u5 >= 1 THEN 20 ELSE 0 END + CASE WHEN o.over5 >= 1 THEN 20 ELSE 0 END AS assessment_score,
        CASE WHEN ce.cu_meetings >= 1 THEN 5 ELSE 0 END + CASE WHEN ce.other_events >= 1 THEN 5 ELSE 0 END AS events_score

    FROM hh_visits v
    LEFT JOIN households_registered hr ON v.location_id = hr.location_id AND v.period_id = hr.period_id
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
    period_id,
    location_id,
    'revised_active_chps' AS metric_id,
    chp_id,
    1 AS value
FROM scored_with_total
WHERE total_score >= 80
