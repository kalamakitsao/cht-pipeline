-- models/fact/metrics/u5_conditions.sql
-- Add logic to compute u5_assessed, diarrhea, pneumonia, malnutrition, malaria
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH base AS (
    SELECT
        reported_by_parent AS cha_id,
        reported::DATE AS report_date,
        patient_id,
        TRUE AS u5_assessed,
        has_diarrhoea,
        (has_fast_breathing IS TRUE AND has_chest_indrawing IS TRUE) AS has_pneumonia,
        (muac_color IN ('yellow', 'red')) AS has_malnutrition,
        rdt_result = 'positive' AS has_malaria
    FROM {{ ref('u5_assessment') }}
    WHERE reported_by_parent IN (
        SELECT location_id FROM {{ ref('dim_location') }} WHERE level = 'chp area'
    )
),

metrics_unpivoted AS (
    SELECT cha_id, report_date, 'u5_assessed' AS metric_id, COUNT(DISTINCT patient_id) AS value
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_diarrhea_cases', COUNT(DISTINCT CASE WHEN has_diarrhoea THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_pneumonia_cases', COUNT(DISTINCT CASE WHEN has_pneumonia THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_malnutrition_cases', COUNT(DISTINCT CASE WHEN has_malnutrition THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_malaria_cases', COUNT(DISTINCT CASE WHEN has_malaria THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date
),

with_periods AS (
    SELECT
        m.cha_id AS location_id,
        p.period_id,
        m.metric_id,
        m.value
    FROM metrics_unpivoted m
    JOIN {{ ref('dim_period') }} p
      ON m.report_date BETWEEN p.start_date AND p.end_date
),

aggregated AS (
    SELECT location_id, period_id, metric_id, SUM(value) AS value
    FROM with_periods
    GROUP BY location_id, period_id, metric_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM aggregated
WHERE value > 0
