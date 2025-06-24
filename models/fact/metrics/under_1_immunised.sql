-- models/fact/metrics/under_1_immunised.sql
-- Add logic to compute fully immunized under-1 children
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH immunized AS (
    SELECT
        f.reported_by_parent AS location_id,
        f.reported::DATE AS report_date,
        f.patient_id
    FROM {{ ref('immunization') }} f
    WHERE f.is_referred_immunization IS FALSE
      AND f.patient_age_in_months <= 12
      AND f.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

with_periods AS (
    SELECT
        i.location_id,
        p.period_id,
        COUNT(DISTINCT i.patient_id) AS value
    FROM immunized i
    JOIN {{ ref('dim_period') }} p
      ON i.report_date BETWEEN p.start_date AND p.end_date
    GROUP BY i.location_id, p.period_id
    HAVING COUNT(DISTINCT i.patient_id) > 0
)

SELECT
    location_id,
    period_id,
    'under_1_immunised' AS metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM with_periods
