-- models/metrics/mv_aggregate_metrics_summary.sql

{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["location_id", "period_id", "metric_id"], "unique": true},
      {"columns": ["chp_area"]},
      {"columns": ["county"]},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ]
) }}

WITH location_hierarchy AS (
    SELECT
        chp_area.location_id AS chp_area_id,
        chp_area.name AS chp_area,
        chu.name AS community_unit,
        sub.name AS sub_county,
        county.name AS county
    FROM {{ ref('dim_location') }} chp_area
    LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp_area.parent_id
    LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
    LEFT JOIN {{ ref('dim_location') }} county ON county.location_id = sub.parent_id
    WHERE chp_area.level = 'chp area'
      AND chp_area.name !~ '^[0-9]+$'
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
        dp.label AS period_label,
        dm.group_name AS metric_group,
        dm.name AS metric,
        a.value,
        'chp area' AS level,
        a.location_id,
        a.period_id,
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
    period_label,
    metric_group,
    metric,
    value,
    location_id,
    period_id,
    metric_id,
    last_updated
FROM joined
