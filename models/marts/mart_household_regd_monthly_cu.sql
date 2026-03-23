{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county_id", "sub_county_id", "community_unit_id", "period_start", "metric_id"], "unique": true},
      {"columns": ["county", "sub_county", "community_unit", "period_start"]},
      {"columns": ["metric_group"]}
    ],
    on_schema_change = 'append_new_columns',
    tags = ['cadence_daily']
) }}
WITH location_hierarchy AS (
    SELECT
        chp_area_id,
        community_unit_id,
        community_unit,
        sub_county_id,
        sub_county,
        county_id,
        county
    FROM {{ ref('mv_location_hierarchy') }}
)

SELECT
    'community unit' AS level,
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    lh.community_unit_id,
    lh.community_unit,
    fa.period_start,
    TO_CHAR(fa.period_start, 'FMMonth YYYY') AS month_year,
    dm.group_name AS metric_group,
    dm.metric_group_id AS metric_group_id,
    dm.name AS metric,
    SUM(fa.value) AS value,
    fa.metric_id
FROM {{ ref('fact_households_registered_monthly_trend') }} fa
JOIN location_hierarchy lh ON lh.chp_area_id = fa.location_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
GROUP BY
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    lh.community_unit_id,
    lh.community_unit,
    fa.period_start,
    dm.group_name,
    dm.metric_group_id,
    dm.name,
    fa.metric_id
