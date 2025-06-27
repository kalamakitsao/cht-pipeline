{{ config(
    materialized = 'table',
    indexes = [
      {"columns": ["community_unit", "period_id", "metric_id"], "unique": true},
      {"columns": ["period_label"]},
      {"columns": ["metric_group"]},
      {"columns": ["period_start", "period_end"]},
      {"columns": ["last_updated"]}
    ]
) }}

SELECT
    'community unit' AS level,
    county_loc.name AS county,
    sub.name AS sub_county,
    chu.name AS community_unit,
    NULL AS chp_area,
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
JOIN {{ ref('dim_location') }} chp_area ON chp_area.location_id = fa.location_id
LEFT JOIN {{ ref('dim_location') }} chu ON chu.location_id = chp_area.parent_id
LEFT JOIN {{ ref('dim_location') }} sub ON sub.location_id = chu.parent_id
LEFT JOIN {{ ref('dim_location') }} county_loc ON county_loc.location_id = sub.parent_id
JOIN {{ ref('dim_period') }} dp ON dp.period_id = fa.period_id
JOIN {{ ref('dim_metric') }} dm ON dm.metric_id = fa.metric_id
WHERE chp_area.level = 'chp area'
  AND chp_area.name !~ '^[0-9]+$'
GROUP BY
    chu.name,
    sub.name,
    county_loc.name,
    dp.start_date,
    dp.end_date,
    dp.label,
    dm.group_name,
    dm.name,
    fa.period_id,
    fa.metric_id
