{{ config(
  materialized = 'table',
  unique_key   = ['level','period_start','metric_id'],
  on_schema_change = 'append_new_columns',
  tags = ['trends','monthly','national','api']
) }}

WITH metrics_map AS (
  SELECT *
  FROM ( VALUES
    ('monthly_rates_reporting_chp',   'is_reporting_chps',   'chps_enrolled',          1.0, 1),
    ('monthly_rates_active_chp',      'active_chps_new',     'chps_enrolled',          1.0, 1),
    ('monthly_rates_pop_coverage',    'people_served',       'people_registered',      1.0, 2),
    ('monthly_rates_household_visit', 'hh_visited',          'households_registered',  1.0, 2)
  ) AS t(metric_id, numerator_metric_id, denominator_metric_id, scale_factor, round_to)
),

dim_metric_enriched AS (
  SELECT LOWER(TRIM(metric_id)) AS metric_id,
         name AS metric,
         group_name AS metric_group,
         metric_group_id
  FROM afyabi.dim_metric
),

monthly_base AS (
  SELECT LOWER(level) AS level,
         period_start,
         month_year,
         LOWER(metric_id) AS metric_id,
         value::numeric AS value
  FROM afyabi.mart_household_regd_monthly_national
  UNION ALL
  SELECT LOWER(level),
         period_start,
         month_year,
         LOWER(metric_id),
         value::numeric
  FROM afyabi.mart_household_visit_monthly_national
),

num_sums_std AS (
  SELECT m.metric_id,
         b.level,
         b.period_start,
         b.month_year,
         SUM(b.value) AS numerator_value
  FROM metrics_map m
  JOIN monthly_base b
    ON b.metric_id = LOWER(m.numerator_metric_id)
  WHERE m.metric_id IN ('monthly_rates_pop_coverage','monthly_rates_household_visit')
  GROUP BY 1,2,3,4
),

den_sums_std AS (
  SELECT m.metric_id,
         b.level,
         b.period_start,
         b.month_year,
         SUM(b.value) AS denominator_value
  FROM metrics_map m
  JOIN monthly_base b
    ON b.metric_id = LOWER(m.denominator_metric_id)
  WHERE m.metric_id IN ('monthly_rates_pop_coverage','monthly_rates_household_visit')
  GROUP BY 1,2,3,4
),

computed_std AS (
  SELECT ns.level,
         ns.period_start,
         ns.month_year,
         ns.metric_id,
         ROUND(
           COALESCE(ns.numerator_value,0)::numeric
           / NULLIF(COALESCE(ds.denominator_value,0)::numeric,0)
           * mm.scale_factor
         , mm.round_to) AS value
  FROM num_sums_std ns
  LEFT JOIN den_sums_std ds
    ON ds.metric_id   = ns.metric_id
   AND ds.level       = ns.level
   AND ds.period_start = ns.period_start
   AND ds.month_year   = ns.month_year
  JOIN metrics_map mm
    ON mm.metric_id = ns.metric_id
),

chp_activity AS (
  SELECT LOWER(level) AS level,
         period_start,
         month_year,
         LOWER(metric_id) AS metric_id,
         value::numeric AS value
  FROM afyabi.mart_chp_activity_monthly_national
),

num_chp AS (
  SELECT m.metric_id,
         a.level,
         a.period_start,
         a.month_year,
         SUM(a.value) AS numerator_value
  FROM chp_activity a
  JOIN metrics_map m ON m.numerator_metric_id = a.metric_id
  WHERE a.metric_id IN ('active_chps_new', 'is_reporting_chps')
  GROUP BY 1,2,3,4
),

den_chp AS (
  SELECT value::numeric AS chps_enrolled
  FROM afyabi.mv_aggregate_national_metrics_summary
  WHERE metric_id = 'chps_enrolled' AND period_id = 1
  LIMIT 1
),

computed_chp AS (
  SELECT n.level,
         n.period_start,
         n.month_year,
         n.metric_id,
         ROUND(
           COALESCE(n.numerator_value,0)::numeric
           / NULLIF((SELECT chps_enrolled FROM den_chp), 0)
           * mm.scale_factor
         , mm.round_to) AS value
  FROM num_chp n
  JOIN metrics_map mm ON mm.metric_id = n.metric_id
),

computed AS (
  SELECT * FROM computed_std
  UNION ALL
  SELECT * FROM computed_chp
)

SELECT
  c.level,
  c.period_start,
  c.month_year,
  d.metric_group,
  d.metric_group_id,
  d.metric,
  c.value,
  c.metric_id
FROM computed c
LEFT JOIN dim_metric_enriched d
  ON d.metric_id = c.metric_id
ORDER BY metric_id, period_start 