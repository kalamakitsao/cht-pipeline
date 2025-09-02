-- models/fact/metrics/fact_deaths_monthly_trend.sql

{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_start','metric_id'],
  tags = ['kpi','death','cadence_daily'],
  on_schema_change = 'ignore'
) }}

WITH base AS (
  SELECT
    reported_by_parent as location_id,
    date_trunc('month', reported_date)::date AS period_start,
    COUNT(*) FILTER (WHERE death_type = 'maternal death')             AS maternal_deaths,
    COUNT(*) FILTER (WHERE patient_age_in_days IS NOT NULL AND patient_age_in_days < 29)
                                                                       AS neonatal_deaths,
    COUNT(*) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827)  AS child_deaths,
    COUNT(*) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827 AND sex = 'male')
                                                                       AS child_deaths_male,
    COUNT(*) FILTER (WHERE patient_age_in_days BETWEEN 29 AND 1827 AND sex = 'female')
                                                                       AS child_deaths_female,
    COUNT(*) FILTER (WHERE patient_age_in_days > 1827 AND sex = 'male')
                                                                       AS over_5_deaths_male,
    COUNT(*) FILTER (WHERE patient_age_in_days > 1827 AND sex = 'female')
                                                                       AS over_5_deaths_female,
    COUNT(*)                                                           AS total_deaths
  FROM {{ ref('death_report_enriched') }}
  WHERE reported_date >= date_trunc('month', CURRENT_DATE) - interval '12 months' 
  GROUP BY reported_by_parent, date_trunc('month', reported_date)
),

metrics AS (
  SELECT location_id, period_start, m.metric_id, m.value
  FROM base d
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

SELECT location_id, period_start, metric_id, value
FROM metrics WHERE value > 0
