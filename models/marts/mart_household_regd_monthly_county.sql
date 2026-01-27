{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["county_id", "period_start", "metric_id"], "unique": true},
      {"columns": ["metric_group"]}
    ],
    tags=['cadence_daily']
) }}
WITH location_hierarchy AS (
    SELECT
        county_id,
        county
    FROM {{ ref('mv_location_hierarchy') }}
)

SELECT
    'county' AS level,
    lh.county_id,
    lh.county,
    fa.period_start,
    TO_CHAR(fa.period_start, 'FMMonth YYYY') AS month_year,
    dm.group_name AS metric_group,
    dm.metric_group_id,
    dm.name AS metric,
    SUM(fa.value) AS value,
    fa.metric_id
FROM {{ ref('fact_households_registered_monthly_trend') }} fa
JOIN location_hierarchy lh ON lh.chp_area_id = fa.location_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
GROUP BY
    lh.county_id,
    lh.county,
    fa.period_start,
    dm.group_name,
    dm.metric_group_id,
    dm.name,
    fa.metric_id