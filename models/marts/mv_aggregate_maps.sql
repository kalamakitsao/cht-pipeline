{{ config(
  materialized = 'incremental',
  unique_key   = ['level','county','sub_county','period_id','metric_id'],
  on_schema_change = 'ignore',
  tags = ['maps','metrics','api']
) }}

WITH metrics_map AS (
  SELECT *
  FROM (
    VALUES
      VALUES
      ('rate_teen_pregnancy',            'teen_pregnancies',           'currently_pregnant',            1.0,   3),
      ('rate_maternal_death',            'maternal_deaths',            'deliveries',                    100000.0, 1),
      ('rate_malnutrition_referral',     'u5_malnutrition_referred',   'u5_screened',                   1.0,   3),
      ('rate_pneumonia_referral',        'u5_pneumonia_referred',      'u5_screened',                   1.0,   3),
      ('rate_malaria_referral',          'u5_malaria_referred',        'u5_screened',                   1.0,   3),
      ('rate_diarrhoea_referral',        'u5_diarrhoea_referred',      'u5_screened',                   100.0,   2),
      ('rate_mental_health_referral',    'mental_health_referred',                'mh_screened',                   1.0,   3),
      ('rate_hypertension_referral',     'hypertension_referred',               'htn_screened',                  1.0,   3),
      ('rate_diabetes_referral',         'diabetes_referred',                'dm_screened',                   1.0,   3),
      ('rate_active_chp',                'active_chps_new',                'expected_chps',                 1.0,   3),
      ('rate_echis_pop_coverage',        'people_registered',          'population_projection',         1.0,   3),
      ('rate_household_visit',           'households_visited',         'households_target_or_total',    1.0,   3)
  ) AS t(metric_id, numerator_metric_id, denominator_metric_id, scale_factor, round_to)
  ) AS t(metric_id, numerator_ids, denominator_ids, scale_factor, round_to)
),

dim_metric_enriched AS (
  SELECT
    LOWER(TRIM(metric_id)) AS metric_id,
    name AS metric,
    group_name AS metric_group,
    metric_group_id
  FROM {{ ref('dim_metric') }}
),

county_base AS (
  SELECT
    LOWER(level) AS level,
    county,
    NULL::TEXT AS sub_county,
    period_id,
    period_label,
    LOWER(metric_id) AS metric_id,
    value::NUMERIC AS value,
    last_updated
  FROM {{ ref('mv_aggregate_metrics_by_county') }}
),

sub_county_base AS (
  SELECT
    LOWER(level) AS level,
    county,
    sub_county,
    period_id,
    period_label,
    LOWER(metric_id) AS metric_id,
    value::NUMERIC AS value,
    last_updated
  FROM {{ ref('mv_aggregate_metrics_by_sub_county') }}
),

base AS (
  SELECT * FROM county_base
  UNION ALL
  SELECT * FROM sub_county_base
),

num_sums AS (
  SELECT
    m.metric_id,
    b.level,
    b.county,
    b.sub_county,
    b.period_id,
    ANY_VALUE(b.period_label) AS period_label,
    SUM(b.value) AS numerator_value
  FROM metrics_map m
  JOIN base b
    ON b.metric_id = ANY(m.numerator_ids)
  GROUP BY 1,2,3,4,5
),

den_sums AS (
  SELECT
    m.metric_id,
    b.level,
    b.county,
    b.sub_county,
    b.period_id,
    SUM(b.value) AS denominator_value
  FROM metrics_map m
  JOIN base b
    ON b.metric_id = ANY(m.denominator_ids)
  GROUP BY 1,2,3,4,5
),

computed AS (
  SELECT
    ns.level,
    ns.county,
    CASE WHEN ns.level = 'county' THEN NULL::TEXT ELSE ns.sub_county END AS sub_county,
    ns.period_id,
    ns.period_label,
    ns.metric_id,
    ROUND(
      COALESCE(ns.numerator_value,0)::NUMERIC
      / NULLIF(COALESCE(ds.denominator_value,0)::NUMERIC, 0)
      * mm.scale_factor
    , mm.round_to) AS value,
    GREATEST(
      MAX(COALESCE(b.last_updated, NOW())),
      NOW()
    ) AS last_updated
  FROM num_sums ns
  LEFT JOIN den_sums ds
    ON ds.metric_id  = ns.metric_id
   AND ds.level      = ns.level
   AND ds.county     = ns.county
   AND COALESCE(ds.sub_county,'') = COALESCE(ns.sub_county,'')
   AND ds.period_id  = ns.period_id
  JOIN metrics_map mm
    ON mm.metric_id  = ns.metric_id
  LEFT JOIN base b
    ON b.level       = ns.level
   AND b.county      = ns.county
   AND COALESCE(b.sub_county,'') = COALESCE(ns.sub_county,'')
   AND b.period_id   = ns.period_id
  GROUP BY 1,2,3,4,5,6, mm.scale_factor, mm.round_to,
           ns.numerator_value, ds.denominator_value
),

final AS (
  SELECT
    c.level,
    c.county,
    c.sub_county,
    c.period_id,
    c.period_label,
    d.metric_group,
    d.metric_group_id,
    d.metric,
    c.metric_id,
    c.value,
    c.last_updated
  FROM computed c
  LEFT JOIN dim_metric_enriched d
    ON d.metric_id = c.metric_id
)

SELECT * FROM final
{% if is_incremental() %}
WHERE period_id >= (SELECT COALESCE(MAX(period_id), '000000') FROM {{ this }})
{% endif %}