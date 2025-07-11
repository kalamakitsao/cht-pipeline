-- models/fact/metrics/people_served.sql
-- Add logic to count unique patients served per period
{{ config(
    materialized = 'incremental',
    unique_key = ['snapshot_date', 'location_id', 'period_id', 'metric_id'],
    on_schema_change='append_new_columns'
) }}

WITH patient_events AS (
    SELECT
        dr.parent_uuid AS location_id,
        DATE(dr.reported) AS report_date,
        dr.patient_id
    FROM {{ ref('data_record') }} dr
    WHERE dr.parent_uuid IN (SELECT location_id FROM {{ ref('dim_location') }})
      AND dr.patient_id IS NOT NULL
),

with_periods AS (
    SELECT
        p.period_id,
        e.location_id,
        e.patient_id
    FROM patient_events e
    JOIN {{ ref('dim_period') }} p
      ON e.report_date BETWEEN p.start_date AND p.end_date
),

deduped AS (
    SELECT
        location_id,
        period_id,
        COUNT(DISTINCT patient_id) AS value
    FROM with_periods
    GROUP BY location_id, period_id
    HAVING COUNT(DISTINCT patient_id) > 0
)

SELECT
    location_id,
    period_id,
    'people_served' AS metric_id,
    value,
    CURRENT_DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS last_updated
FROM deduped
