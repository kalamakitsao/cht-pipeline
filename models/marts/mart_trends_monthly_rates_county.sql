{{ config(
  materialized = 'table',
  unique_key   = ['level','county_id','county','period_start','metric_id'],
  on_schema_change = 'append_new_columns',
  tags = ['trends','monthly','county','api']
) }}

WITH metrics_map AS (
  SELECT *
  FROM (
    VALUES
      ('monthly_rates_reporting_chp',   'is_reporting_chps',   'chps_enrolled',         1.0, 1),
      ('monthly_rates_active_chp',      'active_chps_new',     'chps_enrolled',          1.0, 1),
      ('monthly_rates_pop_coverage',    'people_served',       'people_registered',      1.0, 2),
      ('monthly_rates_household_visit', 'hh_visited',          'households_registered',  1.0, 2)
  ) AS t(metric_id, numerator_metric_id, denominator_metric_id, scale_factor, round_to)
),

dim_metric_enriched AS (
  SELECT
    LOWER(TRIM(metric_id)) AS metric_id,
    name                   AS metric,
    group_name             AS metric_group,
    metric_group_id
  FROM {{ ref('dim_metric') }}
),

monthly_base AS (
  SELECT
    LOWER(level)     AS level,
    county_id,
    county,
    period_start,
    month_year,
    LOWER(metric_id) AS metric_id,
    value::NUMERIC   AS value
  FROM {{ ref('mart_household_regd_monthly_county') }}
  UNION ALL
  SELECT
    LOWER(level)     AS level,
    county_id,
    county,
    period_start,
    month_year,
    LOWER(metric_id) AS metric_id,
    value::NUMERIC   AS value
  FROM {{ ref('mart_household_visit_monthly_county') }}
),

num_sums_std AS (
  SELECT
    m.metric_id,
    b.level,
    b.county_id,
    b.county,
    b.period_start,
    b.month_year,
    SUM(b.value) AS numerator_value
  FROM metrics_map m
  JOIN monthly_base b
    ON b.metric_id = LOWER(m.numerator_metric_id)
  WHERE m.metric_id IN ('monthly_rates_pop_coverage','monthly_rates_household_visit')
  GROUP BY 1,2,3,4,5,6
),

den_sums_std AS (
  SELECT
    m.metric_id,
    b.level,
    b.county_id,
    b.county,
    b.period_start,
    b.month_year,
    SUM(b.value) AS denominator_value
  FROM metrics_map m
  JOIN monthly_base b
    ON b.metric_id = LOWER(m.denominator_metric_id)
  WHERE m.metric_id IN ('monthly_rates_pop_coverage','monthly_rates_household_visit')
  GROUP BY 1,2,3,4,5,6
),

computed_std AS (
  SELECT
    ns.level,
    ns.county_id,
    ns.county,
    ns.period_start,
    ns.month_year,
    ns.metric_id,
    ROUND(
      COALESCE(ns.numerator_value,0)::NUMERIC
      / NULLIF(COALESCE(ds.denominator_value,0)::NUMERIC, 0)
      * mm.scale_factor
    , mm.round_to) AS value
  FROM num_sums_std ns
  LEFT JOIN den_sums_std ds
    ON ds.metric_id    = ns.metric_id
   AND ds.level        = ns.level
   AND ds.county_id    = ns.county_id
   AND ds.period_start = ns.period_start
   AND ds.month_year   = ns.month_year
  JOIN metrics_map mm
    ON mm.metric_id    = ns.metric_id
),

chp_activity AS (
  SELECT
    LOWER(level)     AS level,
    county_id,
    county,
    period_start,
    month_year,
    LOWER(metric_id) AS metric_id,
    value::NUMERIC   AS value
  FROM {{ ref('mart_chp_activity_monthly_county') }}
),

num_chp AS (
  SELECT
    m.metric_id,
    a.level,
    a.county_id,
    a.county,
    a.period_start,
    a.month_year,
    SUM(a.value) AS numerator_value
  FROM chp_activity a
  JOIN metrics_map m ON m.numerator_metric_id = a.metric_id
  WHERE a.metric_id IN ('active_chps_new', 'is_reporting_chps')
  GROUP BY 1,2,3,4,5,6
),

den_chp AS (
  SELECT
    county_id,
    county,
    value::NUMERIC AS chps_enrolled
  FROM {{ ref('mv_aggregate_metrics_by_county') }}
  WHERE metric_id = 'chps_enrolled' AND period_id = 1
),

computed_chp AS (
  SELECT
    n.level,
    n.county_id,
    n.county,
    n.period_start,
    n.month_year,
    n.metric_id,
    ROUND(
      COALESCE(n.numerator_value,0)::NUMERIC
      / NULLIF(COALESCE(d.chps_enrolled,0)::NUMERIC, 0)
      * mm.scale_factor
    , mm.round_to) AS value
  FROM num_chp n
  JOIN den_chp d
    ON LOWER(TRIM(d.county)) = LOWER(TRIM(n.county))
  JOIN metrics_map mm
    ON mm.metric_id = n.metric_id
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