{{ config(
    materialized = 'table',
    on_schema_change = 'append_new_columns',
    indexes = [
      {"columns": ["county", "sub_county", "community_unit", "chp_area", "period_start"]},
      {"columns": ["county", "sub_county", "community_unit", "period_start"]},
      {"columns": ["county", "period_start"]}
    ],
    tags = ['trends','monthly','chp_area','api']
) }}

WITH metrics_map AS (
  SELECT *
  FROM ( VALUES
    ('monthly_rates_reporting_chp',   'chps_reporting',      'chps_enrolled',         1.0, 1),
    ('monthly_rates_active_chp',      'chps_meeting_target', 'chps_enrolled',         1.0, 1),
    ('monthly_rates_pop_coverage',    'people_served',       'people_registered',     1.0, 2),
    ('monthly_rates_household_visit', 'hh_visited',          'households_registered', 1.0, 2)
  ) AS t(metric_id, numerator_metric_id, denominator_metric_id, scale_factor, round_to)
),

dim_metric_enriched AS (
  SELECT LOWER(TRIM(metric_id)) AS metric_id, name AS metric, group_name AS metric_group, metric_group_id
  FROM afyabi.dim_metric
),

monthly_base AS (
  SELECT LOWER(level) AS level, county_id, county, sub_county_id, sub_county, community_unit_id, community_unit, chp_area_id, chp_area, period_start, month_year,
         LOWER(metric_id) AS metric_id, value::numeric AS value
  FROM afyabi.mart_household_regd_monthly_area
  UNION ALL
  SELECT LOWER(level), county_id, county, sub_county_id, sub_county, community_unit_id, community_unit, chp_area_id, chp_area, period_start, month_year,
         LOWER(metric_id), value::numeric
  FROM afyabi.mart_household_visit_monthly_area
),

num_sums_std AS (
  SELECT m.metric_id, b.level, b.county_id, b.county, b.sub_county_id, b.sub_county, b.community_unit_id, b.community_unit, b.chp_area_id, b.chp_area, b.period_start, b.month_year,
         SUM(b.value) AS numerator_value
  FROM metrics_map m JOIN monthly_base b ON b.metric_id = LOWER(m.numerator_metric_id)
  WHERE m.metric_id IN ('monthly_rates_pop_coverage','monthly_rates_household_visit')
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

den_sums_std AS (
  SELECT m.metric_id, b.level, b.county_id, b.county, b.sub_county_id, b.sub_county, b.community_unit_id, b.community_unit, b.chp_area_id, b.chp_area, b.period_start, b.month_year,
         SUM(b.value) AS denominator_value
  FROM metrics_map m JOIN monthly_base b ON b.metric_id = LOWER(m.denominator_metric_id)
  WHERE m.metric_id IN ('monthly_rates_pop_coverage','monthly_rates_household_visit')
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

computed_std AS (
  SELECT ns.level, ns.county_id, ns.county, ns.sub_county_id, ns.sub_county, ns.community_unit_id, ns.community_unit, ns.chp_area_id, ns.chp_area, ns.period_start, ns.month_year, ns.metric_id,
         ROUND( COALESCE(ns.numerator_value,0)::numeric / NULLIF(COALESCE(ds.denominator_value,0)::numeric,0) * mm.scale_factor
         , mm.round_to) AS value
  FROM num_sums_std ns
  LEFT JOIN den_sums_std ds
    ON ds.metric_id=ns.metric_id AND ds.level=ns.level AND ds.county_id=ns.county_id
   AND COALESCE(ds.sub_county,'')=COALESCE(ns.sub_county,'')
   AND COALESCE(ds.community_unit,'')=COALESCE(ns.community_unit,'')
   AND COALESCE(ds.chp_area,'')=COALESCE(ns.chp_area,'')
   AND ds.period_start=ns.period_start AND ds.month_year=ns.month_year
  JOIN metrics_map mm ON mm.metric_id = ns.metric_id
),

chp_activity AS (
  SELECT LOWER(level) AS level, county_id, county, sub_county_id, sub_county, community_unit_id, community_unit, chp_area_id, chp_area, period_start, month_year,
         LOWER(metric_id) AS metric_id, value::numeric AS value
  FROM afyabi.mart_chp_activity_monthly_area
),

num_chp AS (
  SELECT m.metric_id, a.level, a.county_id, a.county, a.sub_county_id, a.sub_county, a.community_unit_id, a.community_unit, a.chp_area_id, a.chp_area, a.period_start, a.month_year,
         SUM(a.value) AS numerator_value
  FROM chp_activity a
  JOIN metrics_map m ON m.numerator_metric_id = a.metric_id
  WHERE a.metric_id IN ('chps_meeting_target', 'chps_reporting')
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

den_chp AS (
  SELECT county_id, county, sub_county_id, sub_county, community_unit_id, community_unit, chp_area_id, chp_area, value::numeric AS chps_enrolled
  FROM afyabi.mv_aggregate_metrics_summary
  WHERE metric_id = 'chps_enrolled' AND period_id = 1
),

computed_chp AS (
  SELECT n.level, n.county_id, n.county, n.sub_county_id, n.sub_county, n.community_unit_id, n.community_unit, n.chp_area_id, n.chp_area, n.period_start, n.month_year, n.metric_id,
         ROUND( COALESCE(n.numerator_value,0)::numeric / NULLIF(COALESCE(d.chps_enrolled,0)::numeric,0) * mm.scale_factor
         , mm.round_to) AS value
  FROM num_chp n
  JOIN den_chp d ON LOWER(TRIM(d.county))=LOWER(TRIM(n.county))
                AND LOWER(TRIM(d.sub_county))=LOWER(TRIM(n.sub_county))
                AND LOWER(TRIM(d.community_unit))=LOWER(TRIM(n.community_unit))
                AND LOWER(TRIM(d.chp_area))=LOWER(TRIM(n.chp_area))
  JOIN metrics_map mm ON mm.metric_id = n.metric_id
),

computed AS (
  SELECT * FROM computed_std
  UNION ALL
  SELECT * FROM computed_chp
)

SELECT
  c.level,
  c.county_id,
  c.county,
  c.sub_county_id,
  c.sub_county,
  c.community_unit_id,
  c.community_unit,
  c.chp_area_id,
  c.chp_area,
  c.period_start,
  c.month_year,
  d.metric_group,
  d.metric_group_id,
  d.metric,
  c.value,
  c.metric_id
FROM computed c
LEFT JOIN dim_metric_enriched d ON d.metric_id = c.metric_id

{% if is_incremental() %}
WHERE c.period_start >= (SELECT COALESCE(MAX(period_start), '1900-01-01') FROM {{ this }})
{% endif %}