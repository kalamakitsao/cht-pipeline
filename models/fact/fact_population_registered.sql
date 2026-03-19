{{ config(
  materialized = 'table',
  indexes = [
    {'columns': ['location_id', 'period_id', 'metric_id'], 'unique': true},
    {'columns': ['period_id']},
    {'columns': ['metric_id']},
    {'columns': ['location_id']}
  ],
  tags = ['kpi','population'],
  on_schema_change = 'ignore'
) }}

WITH periods AS (
  SELECT
    period_id,
    end_date
  FROM {{ ref('dim_period') }}
),

base AS (
  SELECT
    hp.chp_area_id AS location_id,
    hp.sex,
    hp.date_of_birth
  FROM {{ ref('household_person_enriched') }} hp
  WHERE hp.chp_area_id IS NOT NULL
    AND hp.sex IN ('male', 'female')
    AND hp.muted IS DISTINCT FROM TRUE
),

pop AS (
  SELECT
    b.location_id,
    p.period_id,

    COUNT(*)::bigint AS population,
    COUNT(*) FILTER (WHERE b.sex = 'male')::bigint AS population_male,
    COUNT(*) FILTER (WHERE b.sex = 'female')::bigint AS population_female,

    COUNT(*) FILTER (
      WHERE b.date_of_birth IS NOT NULL
        AND b.date_of_birth > (p.end_date - INTERVAL '5 years')
    )::bigint AS population_under_5,

    COUNT(*) FILTER (
      WHERE b.sex = 'male'
        AND b.date_of_birth IS NOT NULL
        AND b.date_of_birth > (p.end_date - INTERVAL '5 years')
    )::bigint AS population_under_5_male,

    COUNT(*) FILTER (
      WHERE b.sex = 'female'
        AND b.date_of_birth IS NOT NULL
        AND b.date_of_birth > (p.end_date - INTERVAL '5 years')
    )::bigint AS population_under_5_female

  FROM base b
  CROSS JOIN periods p
  GROUP BY 1, 2
),

metrics AS (
  SELECT
    location_id,
    period_id,
    m.metric_id,
    m.value
  FROM pop
  CROSS JOIN LATERAL (
    VALUES
      ('population',                population),
      ('population_male',           population_male),
      ('population_female',         population_female),
      ('population_under_5',        population_under_5),
      ('population_under_5_male',   population_under_5_male),
      ('population_under_5_female', population_under_5_female)
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
