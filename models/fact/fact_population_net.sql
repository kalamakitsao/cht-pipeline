-- models/fact/fact_population_net.sql

{{ config(
  materialized = 'table',
  on_schema_change = 'ignore',
  tags = ['kpi','population','cadence_hourly'],
  indexes = [
    {"columns": ["period_id","location_id","metric_id"]},
    {"columns": ["metric_id"]},
    {"columns": ["location_id","period_id"]}
  ]
) }}

{# --------------------------------------------------------------------------
   1. Periods (we use all periods; population snapshots are as of end_date)
   -------------------------------------------------------------------------- #}
WITH periods AS (
  SELECT
    period_id,
    end_date::date AS end_date
  FROM {{ ref('dim_period') }}
),

{# --------------------------------------------------------------------------
   2. Registered population (already per period from fact_population_registered)
   -------------------------------------------------------------------------- #}
pop_raw AS (
  SELECT
    fp.location_id,
    fp.period_id,
    fp.metric_id,
    fp.value::bigint AS value
  FROM {{ ref('fact_population_registered') }} fp
),

pop_pivot AS (
  SELECT
    p.location_id,
    p.period_id,
    MAX(value) FILTER (WHERE metric_id = 'population')                AS population,
    MAX(value) FILTER (WHERE metric_id = 'population_male')           AS population_male,
    MAX(value) FILTER (WHERE metric_id = 'population_female')         AS population_female,
    MAX(value) FILTER (WHERE metric_id = 'population_under_5')        AS population_under_5,
    MAX(value) FILTER (WHERE metric_id = 'population_under_5_male')   AS population_under_5_male,
    MAX(value) FILTER (WHERE metric_id = 'population_under_5_female') AS population_under_5_female
  FROM pop_raw p
  GROUP BY 1,2
),

{# --------------------------------------------------------------------------
   3. Deaths: raw events (location + date + age + sex + type)
   -------------------------------------------------------------------------- #}
death_raw AS (
  SELECT
    dre.reported_by_parent AS location_id,
    dre.reported_date,
    dre.patient_age_in_days,
    dre.sex,
    dre.death_type
  FROM {{ ref('death_report_enriched') }} dre
),

{# --------------------------------------------------------------------------
   4. CUMULATIVE deaths per (location, period):
      For each period, include ALL deaths with reported_date <= period.end_date
   -------------------------------------------------------------------------- #}
death_cumulative AS (
  SELECT
    per.period_id,
    dr.location_id,

    COUNT(*) FILTER (WHERE dr.death_type = 'maternal death')                      AS maternal_deaths,
    COUNT(*) FILTER (WHERE dr.patient_age_in_days IS NOT NULL
                     AND dr.patient_age_in_days < 29)                              AS neonatal_deaths,
    COUNT(*) FILTER (WHERE dr.patient_age_in_days BETWEEN 29 AND 1827)             AS child_deaths,
    COUNT(*) FILTER (WHERE dr.patient_age_in_days BETWEEN 29 AND 1827
                     AND dr.sex = 'male')                                          AS child_deaths_male,
    COUNT(*) FILTER (WHERE dr.patient_age_in_days BETWEEN 29 AND 1827
                     AND dr.sex = 'female')                                        AS child_deaths_female,
    COUNT(*) FILTER (WHERE dr.patient_age_in_days > 1827
                     AND dr.sex = 'male')                                          AS over_5_deaths_male,
    COUNT(*) FILTER (WHERE dr.patient_age_in_days > 1827
                     AND dr.sex = 'female')                                        AS over_5_deaths_female,
    COUNT(*)                                                                        AS total_deaths

  FROM periods per
  LEFT JOIN death_raw dr
    ON dr.reported_date <= per.end_date
  GROUP BY per.period_id, dr.location_id
),

{# --------------------------------------------------------------------------
   5. Join population + cumulative deaths
   -------------------------------------------------------------------------- #}
joined AS (
  SELECT
    p.location_id,
    p.period_id,

    COALESCE(p.population,                0) AS population,
    COALESCE(p.population_male,           0) AS population_male,
    COALESCE(p.population_female,         0) AS population_female,
    COALESCE(p.population_under_5,        0) AS population_under_5,
    COALESCE(p.population_under_5_male,   0) AS population_under_5_male,
    COALESCE(p.population_under_5_female, 0) AS population_under_5_female,

    COALESCE(d.total_deaths,        0) AS total_deaths,
    COALESCE(d.neonatal_deaths,     0) AS neonatal_deaths,
    COALESCE(d.child_deaths,        0) AS child_deaths,
    COALESCE(d.child_deaths_male,   0) AS child_deaths_male,
    COALESCE(d.child_deaths_female, 0) AS child_deaths_female,
    COALESCE(d.over_5_deaths_male,  0) AS over_5_deaths_male,
    COALESCE(d.over_5_deaths_female,0) AS over_5_deaths_female
  FROM pop_pivot p
  LEFT JOIN death_cumulative d
    ON d.period_id   = p.period_id
   AND d.location_id = p.location_id
),

{# --------------------------------------------------------------------------
   6. Net population = population snapshot - cumulative deaths (as of end_date)
   -------------------------------------------------------------------------- #}
net AS (
  SELECT
    location_id,
    period_id,

    /* Overall population net of all recorded deaths up to period end */
    GREATEST(population - total_deaths, 0)::bigint AS net_population,

    /* Sex breakdowns (assuming neonatal deaths not split by sex in source) */
    GREATEST(population_male   - (child_deaths_male + over_5_deaths_male), 0)::bigint
      AS net_population_male,
    GREATEST(population_female - (child_deaths_female + over_5_deaths_female), 0)::bigint
      AS net_population_female,

    /* Under-5 net = snapshot under-5 minus cumulative under-5 deaths */
    GREATEST(population_under_5 - (neonatal_deaths + child_deaths), 0)::bigint
      AS net_population_under_5,
    GREATEST(population_under_5_male - child_deaths_male, 0)::bigint
      AS net_population_under_5_male,
    GREATEST(population_under_5_female - child_deaths_female, 0)::bigint
      AS net_population_under_5_female
  FROM joined
),

metrics AS (
  SELECT
    location_id,
    period_id,
    m.metric_id,
    m.value
  FROM net n
  CROSS JOIN LATERAL (
    VALUES
      ('population',                n.net_population),
      ('population_male',           n.net_population_male),
      ('population_female',         n.net_population_female),
      ('population_under_5',        n.net_population_under_5),
      ('population_under_5_male',   n.net_population_under_5_male),
      ('population_under_5_female', n.net_population_under_5_female)
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
