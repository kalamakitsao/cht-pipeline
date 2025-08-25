-- models/fact/metrics/fact_death_metrics.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','death','cadence_daily'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    e.reported_by_parent AS location_id,
    e.reported_date      AS report_date,
    e.uuid,
    e.death_type,
    e.patient_age_in_days,
    e.sex
  FROM {{ ref('death_report_enriched') }} e
),

daily AS (
  SELECT
    b.location_id,
    pd.period_id,
    COUNT(*) FILTER (WHERE b.death_type = 'maternal death')             AS maternal_deaths,
    COUNT(*) FILTER (WHERE b.patient_age_in_days IS NOT NULL AND b.patient_age_in_days < 29)
                                                                      AS neonatal_deaths,
    COUNT(*) FILTER (WHERE b.patient_age_in_days BETWEEN 29 AND 1827)  AS child_deaths,
    COUNT(*) FILTER (WHERE b.patient_age_in_days BETWEEN 29 AND 1827 AND b.sex = 'male')
                                                                      AS child_deaths_male,
    COUNT(*) FILTER (WHERE b.patient_age_in_days BETWEEN 29 AND 1827 AND b.sex = 'female')
                                                                      AS child_deaths_female,
    COUNT(*) FILTER (WHERE b.patient_age_in_days > 1827 AND b.sex = 'male')
                                                                      AS over_5_deaths_male,
    COUNT(*) FILTER (WHERE b.patient_age_in_days > 1827 AND b.sex = 'female')
                                                                      AS over_5_deaths_female,
    COUNT(*)                                                           AS total_deaths
  FROM base b
  JOIN {{ ref('dim_period_date_map') }} pd
    ON pd.date = b.report_date
  GROUP BY b.location_id, pd.period_id
),
metrics AS (
  SELECT location_id, period_id, m.metric_id, m.value
  FROM daily d
  CROSS JOIN LATERAL (
    VALUES
      ('maternal_deaths',         d.maternal_deaths),
      ('neonatal_deaths',         d.neonatal_deaths),
      ('child_deaths',            d.child_deaths),
      ('child_deaths_male',       d.child_deaths_male),
      ('child_deaths_female',     d.child_deaths_female),
      ('over_5_deaths_male',      d.over_5_deaths_male),
      ('over_5_deaths_female',    d.over_5_deaths_female),
      ('total_deaths',            d.total_deaths)
  ) AS m(metric_id, value)
)

SELECT location_id, period_id, metric_id, value, CURRENT_TIMESTAMP AS last_updated
FROM metrics
WHERE value > 0