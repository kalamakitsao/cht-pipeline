-- models/fact/metrics/fact_actively_reporting_chps.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','chp_compliance'],
  on_schema_change = 'ignore'
) }}

-- 1) Periods we score (small set → pushdown to both inputs)
WITH valid_periods AS (
  SELECT period_id, period_id_name
  FROM {{ ref('dim_period') }}
  WHERE period_id_name IN (
    'today','yesterday','last_7_days',
    'last_1_month','this_month','last_month',
    'last_3_months','this_quarter','last_quarter',
    'last_6_months','this_week'
  )
),

-- 2) Threshold map by period_id_name
thresholds AS (
  SELECT *
  FROM (VALUES
    -- period_id_name          min_ratio   min_abs_visits
    ('last_1_month'   , 0.165::float, 0),
    ('this_month'     , 0.165::float, 0),
    ('last_month'     , 0.165::float, 0),

    ('last_3_months'  , 0.50::float , 0),
    ('this_quarter'   , 0.50::float , 0),
    ('last_quarter'   , 0.50::float , 0),

    ('last_6_months'  , 0.75::float , 0),

    -- “activity” windows: any >0 visit counts as active
    ('today'          , 0.00::float , 1),
    ('yesterday'      , 0.00::float , 1),
    ('last_7_days'    , 0.00::float , 1),
    ('this_week'      , 0.00::float , 1)
  ) AS t(period_id_name, min_ratio, min_abs_visits)
),

-- 3) Sum visits only for the periods we care about
hv AS (
  SELECT
    hv.location_id,
    hv.period_id,
    SUM(hv.value)::bigint AS hh_visited
  FROM {{ ref('fact_households_visited_union') }} hv
  JOIN valid_periods vp ON hv.period_id = vp.period_id
  WHERE hv.metric_id = 'hh_visited'
  GROUP BY hv.location_id, hv.period_id
),

-- 4) Sum registered households for the same period set
hr AS (
  SELECT
    hr.location_id,
    hr.period_id,
    SUM(hr.value)::bigint AS households_registered
  FROM {{ ref('fact_households_registered') }} hr
  JOIN valid_periods vp ON hr.period_id = vp.period_id
  WHERE hr.metric_id = 'households_registered'
  GROUP BY hr.location_id, hr.period_id
),

-- 5) Score: active if absolute threshold met OR ratio threshold met
scored AS (
  SELECT
    v.location_id,
    v.period_id,
    CASE
      WHEN COALESCE(v.hh_visited,0) >= th.min_abs_visits THEN 1
      WHEN COALESCE(h.households_registered,0) > 0
           AND (COALESCE(v.hh_visited,0)::float / h.households_registered) > th.min_ratio THEN 1
      ELSE 0
    END AS is_active
  FROM hv v
  -- visits drive activity; if there are no visits, only the “abs>0” rule would pass anyway
  LEFT JOIN hr h
         ON h.location_id = v.location_id
        AND h.period_id   = v.period_id
  JOIN valid_periods vp   ON vp.period_id = v.period_id
  JOIN thresholds   th    ON th.period_id_name = vp.period_id_name
)

SELECT
  location_id,
  period_id,
  'active_chps_new' AS metric_id,
  1                 AS value,
  CURRENT_TIMESTAMP AS last_updated
FROM scored
WHERE is_active = 1

{% if is_incremental() %}
  {# Only rewrite the dynamic period set each run #}
  {% set preds = [
    "period_id IN (SELECT period_id FROM " ~ ref('dim_period') ~ " WHERE period_id_name IN (" ~
      "'today','yesterday','last_7_days','last_1_month','this_month','last_month'," ~
      "'last_3_months','this_quarter','last_quarter','last_6_months','this_week'))"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
