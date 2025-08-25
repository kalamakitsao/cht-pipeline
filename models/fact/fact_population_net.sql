-- depends_on: {{ ref('dim_period') }}

{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','population','net'],
  on_schema_change = 'ignore',
  indexes = [
    {"columns": ["period_id","location_id","metric_id"]},
    {"columns": ["metric_id"]},
    {"columns": ["location_id","period_id"]}
  ]
) }}

{# --- Pull population metrics and pivot once --- #}
WITH pop_raw AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    value::bigint AS value
  FROM {{ ref('fact_population_registered') }}
  WHERE metric_id IN (
    'population','population_male','population_female',
    'population_under_5','population_under_5_male','population_under_5_female'
  )
),
pop AS (
  SELECT
    location_id,
    period_id,
    MAX(value) FILTER (WHERE metric_id = 'population')                   AS population,
    MAX(value) FILTER (WHERE metric_id = 'population_male')              AS population_male,
    MAX(value) FILTER (WHERE metric_id = 'population_female')            AS population_female,
    MAX(value) FILTER (WHERE metric_id = 'population_under_5')           AS population_under_5,
    MAX(value) FILTER (WHERE metric_id = 'population_under_5_male')      AS population_under_5_male,
    MAX(value) FILTER (WHERE metric_id = 'population_under_5_female')    AS population_under_5_female
  FROM pop_raw
  GROUP BY location_id, period_id
),

{# --- Pull death metrics and pivot once --- #}
death_raw AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    value::bigint AS value
  FROM {{ ref('fact_death_metrics') }}
  WHERE metric_id IN (
    'total_deaths',
    'neonatal_deaths',
    'child_deaths','child_deaths_male','child_deaths_female',
    'over_5_deaths_male','over_5_deaths_female'
  )
),
death AS (
  SELECT
    location_id,
    period_id,
    MAX(value) FILTER (WHERE metric_id = 'total_deaths')        AS total_deaths,
    MAX(value) FILTER (WHERE metric_id = 'neonatal_deaths')      AS neonatal_deaths,
    MAX(value) FILTER (WHERE metric_id = 'child_deaths')         AS child_deaths,
    MAX(value) FILTER (WHERE metric_id = 'child_deaths_male')    AS child_deaths_male,
    MAX(value) FILTER (WHERE metric_id = 'child_deaths_female')  AS child_deaths_female,
    MAX(value) FILTER (WHERE metric_id = 'over_5_deaths_male')   AS over_5_deaths_male,
    MAX(value) FILTER (WHERE metric_id = 'over_5_deaths_female') AS over_5_deaths_female
  FROM death_raw
  GROUP BY location_id, period_id
),

{# --- Join pop & death and compute net buckets --- #}
joined AS (
  SELECT
    p.location_id,
    p.period_id,
    COALESCE(p.population,0)                 AS population,
    COALESCE(p.population_male,0)            AS population_male,
    COALESCE(p.population_female,0)          AS population_female,
    COALESCE(p.population_under_5,0)         AS population_under_5,
    COALESCE(p.population_under_5_male,0)    AS population_under_5_male,
    COALESCE(p.population_under_5_female,0)  AS population_under_5_female,

    COALESCE(d.total_deaths,0)               AS total_deaths,
    COALESCE(d.neonatal_deaths,0)            AS neonatal_deaths,
    COALESCE(d.child_deaths,0)               AS child_deaths,
    COALESCE(d.child_deaths_male,0)          AS child_deaths_male,
    COALESCE(d.child_deaths_female,0)        AS child_deaths_female,
    COALESCE(d.over_5_deaths_male,0)         AS over_5_deaths_male,
    COALESCE(d.over_5_deaths_female,0)       AS over_5_deaths_female
  FROM pop p
  LEFT JOIN death d
    ON d.location_id = p.location_id
   AND d.period_id   = p.period_id
),

net AS (
  SELECT
    j.location_id,
    j.period_id,

    /* Overall population net of all recorded deaths */
    GREATEST(j.population - j.total_deaths, 0)::bigint AS net_population,

    /* Sex breakdowns (assumption: neonatal deaths not split by sex in source) */
    GREATEST(j.population_male   - (j.child_deaths_male  + j.over_5_deaths_male), 0)::bigint   AS net_population_male,
    GREATEST(j.population_female - (j.child_deaths_female+ j.over_5_deaths_female), 0)::bigint AS net_population_female,

    /* Under-5: subtract neonatal + child deaths; sex-specific under-5 uses child_deaths_* only */
    GREATEST(j.population_under_5       - (j.neonatal_deaths + j.child_deaths), 0)::bigint     AS net_population_under_5,
    GREATEST(j.population_under_5_male  - j.child_deaths_male, 0)::bigint                      AS net_population_under_5_male,
    GREATEST(j.population_under_5_female- j.child_deaths_female, 0)::bigint                    AS net_population_under_5_female
  FROM joined j
),

metrics AS (
  SELECT
    n.location_id,
    n.period_id,
    m.metric_id,
    m.value
  FROM net n
  CROSS JOIN LATERAL (
    VALUES
      ('population',                 n.net_population),
      ('population_male',            n.net_population_male),
      ('population_female',          n.net_population_female),
      ('population_under_5',         n.net_population_under_5),
      ('population_under_5_male',    n.net_population_under_5_male),
      ('population_under_5_female',  n.net_population_under_5_female)
  ) AS m(metric_id, value)
)

SELECT
  location_id,
  period_id,
  metric_id,
  value,
  CURRENT_TIMESTAMP AS last_updated
FROM metrics
WHERE value > 0

{% if is_incremental() %}
  {# Recompute only recent/active periods; adjust window to your lateness #}
  {% set preds = [
    "period_id IN (SELECT period_id FROM " ~ ref('dim_period') ~ " WHERE end_date >= CURRENT_DATE - interval '400 days')"
  ] %}
  {{ config(incremental_predicates = preds) }}
{% endif %}
