{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

eligible_immunized AS (
  SELECT
    f.reported_by_parent AS location_id,
    DATE(f.reported) AS report_date,
    f.patient_id
  FROM {{ ref('immunization') }} f
  WHERE f.is_referred_immunization IS FALSE
    AND f.patient_age_in_months <= 12
    AND f.patient_id IS NOT NULL
)

SELECT
  p.period_id AS period_id,
  i.location_id,
  'under_1_immunised' AS metric_id,
  i.patient_id
FROM eligible_immunized i
JOIN periods p ON i.report_date BETWEEN p.start_date AND p.end_date
