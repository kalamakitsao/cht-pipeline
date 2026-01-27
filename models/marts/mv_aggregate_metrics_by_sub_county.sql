{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["sub_county_id", "period_id", "metric_id"], "unique": true},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ],
    tags=['cadence_hourly']
) }}

WITH location_hierarchy AS (
    SELECT
        sub_county_id,
        sub_county,
        county_id,
        county
    FROM {{ ref('mv_location_hierarchy') }}
)

SELECT
    'sub county' AS level,
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    dp.start_date AS period_start,
    dp.end_date AS period_end,
    dp.label AS period_label,
    dm.group_name AS metric_group,
    dm.metric_group_id AS metric_group_id,
    dm.name AS metric,
    SUM(fa.value) AS value,
    fa.period_id,
    fa.metric_id,
    MAX(fa.last_updated) AS last_updated
FROM {{ ref('fact_aggregate') }} fa
JOIN location_hierarchy lh ON lh.chp_area_id = fa.location_id
JOIN {{ ref('dim_period') }} dp ON dp.period_id = fa.period_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
GROUP BY
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    dp.start_date,
    dp.end_date,
    dp.label,
    dm.group_name,
    dm.metric_group_id,
    dm.name,
    fa.period_id,
    fa.metric_id
