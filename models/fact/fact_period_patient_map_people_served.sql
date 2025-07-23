{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH patient_events AS (
    SELECT
        dr.parent_uuid AS location_id,
        DATE(dr.reported) AS report_date,
        dr.patient_id
    FROM {{ ref('data_record') }} dr
    WHERE dr.patient_id IS NOT NULL
),

with_periods AS (
    SELECT
        p.period_id AS period_id,
        e.location_id,
        e.patient_id
    FROM patient_events e
    JOIN {{ ref('dim_period') }} p
      ON e.report_date BETWEEN p.start_date AND p.end_date
)

SELECT
    period_id,
    location_id,
    'people_served' AS metric_id,
    patient_id
FROM with_periods
