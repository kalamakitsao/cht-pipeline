-- models/fact/metrics/people_served.sql
-- Add logic to count unique patients served per period
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH base AS (
    SELECT
        records.grandparent_uuid AS location_id,
        records.reported::DATE AS report_date,
        COUNT(DISTINCT records.patient_id) AS value
    FROM {{ ref('data_record') }} records
    WHERE 
        records.grandparent_uuid IN (
            SELECT location_id FROM {{ ref('dim_location') }} WHERE level = 'chp area'
        )
        AND records.patient_id IS NOT NULL
    GROUP BY records.grandparent_uuid, records.reported::DATE
),

periods AS (
    SELECT * FROM {{ ref('dim_period') }}
),

with_periods AS (
    SELECT
        b.location_id,
        p.period_id,
        SUM(b.value) AS value
    FROM base b
    JOIN periods p ON b.report_date BETWEEN p.start_date AND p.end_date
    GROUP BY b.location_id, p.period_id
    HAVING SUM(b.value) > 0
)

SELECT
    location_id,
    period_id,
    'people_served' AS metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM with_periods
