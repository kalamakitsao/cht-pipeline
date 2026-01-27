{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county_id", "sub_county_id", "community_unit_id", "chp_area_id", "period_start", "metric_id"], "unique": true},
      {"columns": ["metric_group"]}
    ],
    tags=['cadence_daily']
) }}
WITH location_hierarchy AS (
    SELECT
        chp_area_id,
        chp_area,
        community_unit_id,
        community_unit,
        sub_county_id,
        sub_county,
        county_id,
        county
    FROM {{ ref('mv_location_hierarchy') }}
)

SELECT
    'chp area' AS level,
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    lh.community_unit_id,
    lh.community_unit,
    lh.chp_area_id,
    lh.chp_area,
    fa.period_start,
    TO_CHAR(fa.period_start, 'FMMonth YYYY') AS month_year,
    dm.group_name AS metric_group,
    dm.metric_group_id AS metric_group_id,
    dm.name AS metric,
    SUM(fa.value) AS value,
    fa.metric_id
FROM {{ ref('fact_under_five_conditions_monthly_trend') }} fa
JOIN location_hierarchy lh ON lh.chp_area_id = fa.location_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
GROUP BY
    lh.county_id,
    lh.county,
    lh.sub_county_id,
    lh.sub_county,
    lh.community_unit_id,
    lh.community_unit,
    lh.chp_area_id,
    lh.chp_area,
    fa.period_start,
    dm.group_name,
    dm.metric_group_id,
    dm.name,
    fa.metric_id
