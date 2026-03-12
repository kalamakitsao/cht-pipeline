{{ config(
  materialized = 'incremental',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','death','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    e.reported_by_parent AS location_id,
    e.reported_date AS report_date,
    e.death_type,
    e.patient_age_in_days,
    e.sex,
    e.reported,
    e.date_of_death
  FROM {{ ref('death_report_enriched') }} e
),

-- Only recompute for days where there are new reports
changed_periods AS (
  {% if is_incremental() %}
    SELECT DISTINCT report_date
    FROM base
    WHERE reported > (SELECT COALESCE(MAX(last_updated),'2020-01-01') FROM {{ this }})
  {% else %}
    SELECT DISTINCT report_date FROM base
  {% endif %}
),

filtered AS (
  SELECT b.*
  FROM base b
  JOIN changed_periods cp ON b.report_date = cp.report_date
),

joined AS (
  SELECT
    f.location_id,
    pd.period_id,
    f.death_type,
    f.patient_age_in_days,
    f.sex,
    pfc.date_of_birth,
    f.date_of_death,
    f.date_of_death - coalesce(pfc.date_of_birth,f.report_date) AS age_at_death
  FROM filtered f
  JOIN {{ ref('dim_period_date_map') }} pd ON pd.date = f.report_date
  JOIN {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} pfc ON f.patient_uuid = pfc.patient_uuid
),

daily AS (
  SELECT
    location_id,
    period_id,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE death_type = 'maternal death') AS maternal_deaths,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE age_at_death < 29) AS neonatal_deaths,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE age_at_death BETWEEN 29 AND 1827) AS child_deaths,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE age_at_death BETWEEN 29 AND 1827 AND sex = 'male')
      AS child_deaths_male,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE age_at_death BETWEEN 29 AND 1827 AND sex = 'female')
      AS child_deaths_female,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE age_at_death > 1827 AND sex = 'male')
      AS over_5_deaths_male,
    COUNT(DISTINCT patient_uuid) FILTER (WHERE age_at_death > 1827 AND sex = 'female')
      AS over_5_deaths_female,
    COUNT(DISTINCT patient_uuid) AS total_deaths
  FROM joined
  GROUP BY location_id, period_id
),

metrics AS (
  SELECT location_id, period_id, m.metric_id, m.value
  FROM daily d,
  LATERAL (
    VALUES
      ('maternal_deaths', d.maternal_deaths),
      ('neonatal_deaths', d.neonatal_deaths),
      ('child_deaths', d.child_deaths),
      ('child_deaths_male', d.child_deaths_male),
      ('child_deaths_female', d.child_deaths_female),
      ('over_5_deaths_male', d.over_5_deaths_male),
      ('over_5_deaths_female', d.over_5_deaths_female),
      ('total_deaths', d.total_deaths)
  ) AS m(metric_id, value)
)

SELECT location_id, period_id, metric_id, value, CURRENT_TIMESTAMP AS last_updated
FROM metrics
WHERE value > 0
