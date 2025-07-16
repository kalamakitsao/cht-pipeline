-- models/metrics/mv_fact_aggregate_metrics_summary.sql

{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["location_id", "period_id", "metric_id"], "unique": true},
      {"columns": ["chp_area"]},
      {"columns": ["county"]},
      {"columns": ["label"]},
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ]
) }}

WITH location_hierarchy AS (
    SELECT
        chp_area_id,
        chp_area,
        community_unit,
        sub_county,
        county
    FROM {{ ref('mv_location_hierarchy') }}
),

aggregates AS (
    SELECT
        fa.location_id,
        fa.period_id,
        fa.metric_id,
        fa.value,
        fa.last_updated
    FROM {{ ref('fact_aggregate') }} fa
),

joined AS (
    SELECT
        lh.chp_area,
        lh.community_unit,
        lh.sub_county,
        lh.county,
        dp.start_date AS period_start,
        dp.end_date AS period_end,
        dp.label,
        dm.group_name AS metric_group,
        dm.name AS metric,
        a.value,
        'chp area' AS level,
        a.location_id,
        dp.period_id,
        a.metric_id,
        a.last_updated
    FROM aggregates a
    JOIN location_hierarchy lh ON lh.chp_area_id = a.location_id
    JOIN {{ ref('dim_period') }} dp ON dp.period_id = a.period_id
    JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = a.metric_id
)

SELECT
    level,
    chp_area,
    community_unit,
    sub_county,
    county,
    period_start,
    period_end,
    label,
    metric_group,
    metric,
    value,
    location_id,
    metric_id,
    period_id,
    last_updated
FROM joined
