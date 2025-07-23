{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

combined_referrals AS (
    SELECT
        reported_by_parent AS location_id,
        DATE(reported) AS report_date,
        patient_id
    FROM {{ ref('u5_assessment') }}
    WHERE has_been_referred IS TRUE
      AND patient_id IS NOT NULL

    UNION ALL

    SELECT
        reported_by_parent AS location_id,
        DATE(reported) AS report_date,
        patient_id
    FROM {{ ref('over_five_assessment') }}
    WHERE has_been_referred IS TRUE
      AND patient_id IS NOT NULL
)

SELECT
    p.period_id AS period_id,
    r.location_id,
    'total_referrals' AS metric_id,
    r.patient_id
FROM combined_referrals r
JOIN periods p ON r.report_date BETWEEN p.start_date AND p.end_date
