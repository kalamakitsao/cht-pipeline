{{ config(
  materialized = 'table',
  tags = ['kpi','population'],
  on_schema_change = 'ignore'
) }}

WITH periods AS (
  -- Narrow if you only want certain period types, e.g. monthly snapshots:
  -- SELECT period_id, end_date FROM {{ ref('dim_period') }} WHERE period_type IN ('monthly','yearly')
  SELECT period_id, end_date
  FROM {{ ref('dim_period') }}
),

pop AS (
  SELECT
    hp.chp_area_id AS location_id,
    p.period_id    AS period_id,

    COUNT(*)                                                     AS population,
    COUNT(*) FILTER (WHERE hp.sex = 'male')                      AS population_male,
    COUNT(*) FILTER (WHERE hp.sex = 'female')                    AS population_female,

    COUNT(*) FILTER (
      WHERE hp.date_of_birth IS NOT NULL
        AND hp.date_of_birth > (p.end_date - INTERVAL '5 years')
    )                                                            AS population_under_5,

    COUNT(*) FILTER (
      WHERE hp.sex = 'male'
        AND hp.date_of_birth IS NOT NULL
        AND hp.date_of_birth > (p.end_date - INTERVAL '5 years')
    )                                                            AS population_under_5_male,

    COUNT(*) FILTER (
      WHERE hp.sex = 'female'
        AND hp.date_of_birth IS NOT NULL
        AND hp.date_of_birth > (p.end_date - INTERVAL '5 years')
    )                                                            AS population_under_5_female

  FROM {{ ref('household_person_enriched') }} hp
  CROSS JOIN periods p
  WHERE hp.chp_area_id IS NOT NULL
    AND hp.sex IN ('male','female')
    AND hp.muted IS NULL
  GROUP BY 1,2
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
  value::bigint AS value,
  CURRENT_TIMESTAMP AS last_updated
FROM metrics
WHERE value > 0
