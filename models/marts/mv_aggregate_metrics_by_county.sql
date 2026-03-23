{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county_id", "period_id", "metric_id"], "unique": true},
      -- Composite: drives county-level dashboard queries
      {"columns": ["county", "period_label"]},
      -- ID-based path used internally by mart_trends_monthly_rates_county
      {"columns": ["county_id", "period_id"]},
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ],
    on_schema_change = 'append_new_columns',
    tags = ['cadence_hourly']
) }}
WITH location_hierarchy AS (
    SELECT
        chp_area_id,
        county_id,
        county
    FROM {{ ref('mv_location_hierarchy') }}
)

SELECT
    'county' AS level,
    lh.county_id,
    lh.county,
    dp.start_date AS period_start,
    dp.end_date AS period_end,
    dp.label AS period_label,
    dm.group_name AS metric_group,
    dm.metric_group_id,
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
    dp.start_date,
    dp.end_date,
    dp.label,
    dm.group_name,
    dm.metric_group_id,
    dm.name,
    fa.period_id,
    fa.metric_id
UNION ALL
SELECT
    'county' AS level,
    lh.location_id AS county_id,
    lh.name AS county,
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
JOIN {{ ref('dim_location') }} lh ON lh.location_id = fa.location_id
JOIN {{ ref('dim_period') }} dp ON dp.period_id = fa.period_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
WHERE fa.metric_id='chps_expected'
GROUP BY
    lh.location_id,
    lh.name,
    dp.start_date,
    dp.end_date,
    dp.label,
    dm.group_name,
    dm.metric_group_id,
    dm.name,
    fa.period_id,
    fa.metric_id
