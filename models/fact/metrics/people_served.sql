-- models/fact/metrics/people_served.sql
-- Add logic to count unique patients served per period
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH raw_counts AS (
    SELECT
        records.parent_uuid AS location_id,
        DATE(records.reported) AS report_date,
        COUNT(DISTINCT records.patient_id) AS value
    FROM {{ ref('data_record') }} records
    WHERE records.parent_uuid IN (
        SELECT location_id FROM {{ ref('dim_location') }}
    )
      AND records.patient_id IS NOT NULL
    GROUP BY records.parent_uuid, DATE(records.reported)
),

joined AS (
    SELECT
        rc.location_id,
        p.period_id,
        SUM(rc.value) AS value
    FROM raw_counts rc
    JOIN {{ ref('dim_period') }} p
      ON rc.report_date BETWEEN p.start_date AND p.end_date
    GROUP BY rc.location_id, p.period_id
    HAVING SUM(rc.value) > 0
)

SELECT
    location_id,
    period_id,
    'people_served' AS metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM joined
