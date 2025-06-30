-- models/fact/metrics/u5_conditions.sql
-- Add logic to compute u5_assessed, diarrhea, pneumonia, malnutrition, malaria
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH base AS (
    SELECT
        u5.reported_by_parent AS cha_id,
        u5.reported::DATE AS report_date,
        u5.patient_id,
        pax.sex AS sex,
        TRUE AS u5_assessed,
        u5.has_diarrhoea,
        u5.has_fever,
        (has_fast_breathing IS TRUE AND has_chest_indrawing IS TRUE) AS has_pneumonia,
        (muac_color IN ('yellow', 'red')) AS has_malnutrition,
        rdt_result = 'positive' AS has_malaria,
        referred_for_development_milestones,
        has_been_referred,
        u5.rdt_result,
        u5.gave_amox,
        u5.gave_zinc,
        u5.gave_ors,
        u5.gave_al
    FROM {{ ref('u5_assessment') }} u5 
        join {{ ref('patient_f_client') }} pax on u5.patient_id = pax.uuid
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

    UNION ALL

    SELECT cha_id, report_date, 'u5_suspected_malaria_cases', COUNT(DISTINCT CASE WHEN has_fever AND has_malaria IS NOT TRUE AND (has_pneumonia IS NOT TRUE OR has_diarrhoea IS NOT TRUE) THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'referred_for_malaria', COUNT(DISTINCT CASE WHEN has_fever AND has_been_referred THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'referred_for_pneumonia', COUNT(DISTINCT CASE WHEN has_pneumonia  AND has_been_referred THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'referred_for_malnutrition', COUNT(DISTINCT CASE WHEN has_malnutrition AND has_been_referred THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'referred_for_diarrhoes', COUNT(DISTINCT CASE WHEN has_diarrhoea AND has_been_referred THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'referred_for_development_milestones', COUNT(DISTINCT CASE WHEN referred_for_development_milestones THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'male_referred_for_development_milestones', COUNT(DISTINCT CASE WHEN sex = 'male' AND referred_for_development_milestones THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'female_referred_for_development_milestones', COUNT(DISTINCT CASE WHEN sex = 'female' AND referred_for_development_milestones THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_referred', COUNT(DISTINCT CASE WHEN has_been_referred THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_treated', COUNT(DISTINCT CASE WHEN gave_ors OR gave_amox OR gave_al OR gave_zinc THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_treated_malaria', COUNT(DISTINCT CASE WHEN has_been_referred THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_treated_malaria', COUNT(DISTINCT CASE WHEN gave_al THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_treated_diarrhoea', COUNT(DISTINCT CASE WHEN gave_ors OR gave_zinc THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_treated_pneumonia', COUNT(DISTINCT CASE WHEN gave_amox AND has_pneumonia THEN patient_id END)
    FROM base
    GROUP BY cha_id, report_date

    UNION ALL

    SELECT cha_id, report_date, 'u5_tested_malaria', COUNT(DISTINCT CASE WHEN rdt_result <> 'not_done' OR rdt_result IS NOT NULL THEN patient_id END)
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
