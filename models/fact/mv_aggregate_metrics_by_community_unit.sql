{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county", "sub_county", "community_unit", "period_id", "metric_id"], "unique": true},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ]
) }}

WITH location_hierarchy AS (
    SELECT
        chp_area_id,
        community_unit,
        sub_county,
        county
    FROM {{ ref('mv_location_hierarchy') }}
)

SELECT
    'community unit' AS level,
    lh.county,
    lh.sub_county,
    lh.community_unit,
    dp.start_date AS period_start,
    dp.end_date AS period_end,
    dp.label AS period_label,
    dm.group_name AS metric_group,
    dm.name AS metric,
    SUM(fa.value) AS value,
    NULL AS location_id,
    fa.period_id,
    fa.metric_id,
    MAX(fa.last_updated) AS last_updated
FROM {{ ref('fact_aggregate') }} fa
JOIN location_hierarchy lh ON lh.chp_area_id = fa.location_id
JOIN {{ ref('dim_period') }} dp ON dp.period_id = fa.period_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
GROUP BY
    lh.community_unit,
    lh.sub_county,
    lh.county,
    dp.start_date,
    dp.end_date,
    dp.label,
    dm.group_name,
    dm.name,
    fa.period_id,
    fa.metric_id
