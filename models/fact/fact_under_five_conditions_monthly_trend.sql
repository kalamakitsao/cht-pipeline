-- models/fact/metrics/u5_conditions_from_enriched.sql
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['location_id','period_start','metric_id'],
  tags = ['kpi','u5','cadence_monthly'],
  on_schema_change = 'ignore'
) }}

WITH months AS (
    SELECT generate_series(
        DATE '2020-01-01',  -- fixed start date
        date_trunc('month', CURRENT_DATE),
        interval '1 month'
    )::date AS period_start
),
base AS (
    SELECT
        reported_by_parent AS location_id,
        date_trunc('month', reported_date)::date AS period_start,
        patient_id,
        sex,
        has_diarrhoea,
        has_pneumonia,
        has_malnutrition,
        has_malaria,
        has_fever,
        has_been_referred,
        referred_for_development_milestones,
        gave_ors,
        gave_zinc,
        gave_al,
        gave_amox,
        rdt_result
    FROM {{ ref('under_five_assessment_enriched') }}
    WHERE reported >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
),
metrics AS (
    SELECT
        location_id,
        period_start,
        patient_id,
        UNNEST(ARRAY[
            'u5_assessed',
            CASE WHEN has_diarrhoea    THEN 'u5_diarrhea_cases'          END,
            CASE WHEN has_pneumonia    THEN 'u5_pneumonia_cases'         END,
            CASE WHEN has_malnutrition THEN 'u5_malnutrition_cases'      END,
            CASE WHEN has_malaria      THEN 'u5_confirmed_malaria_cases' END,
            CASE WHEN has_fever AND NOT has_malaria AND (NOT has_pneumonia OR NOT has_diarrhoea)
                THEN 'u5_suspected_malaria_cases' END,
            CASE WHEN has_fever       AND has_been_referred THEN 'referred_for_malaria'      END,
            CASE WHEN has_pneumonia   AND has_been_referred THEN 'referred_for_pneumonia'    END,
            CASE WHEN has_malnutrition AND has_been_referred THEN 'referred_for_malnutrition' END,
            CASE WHEN has_diarrhoea   AND has_been_referred THEN 'referred_for_diarrhoea'    END,
            CASE WHEN has_been_referred THEN 'u5_referred' END,
            CASE WHEN has_been_referred AND sex = 'male'   THEN 'u5_referred_male'   END,
            CASE WHEN has_been_referred AND sex = 'female' THEN 'u5_referred_female' END,
            CASE WHEN referred_for_development_milestones THEN 'referred_for_development_milestones' END,
            CASE WHEN referred_for_development_milestones AND sex='male'   THEN 'male_referred_for_development_milestones'   END,
            CASE WHEN referred_for_development_milestones AND sex='female' THEN 'female_referred_for_development_milestones' END,
            CASE WHEN (gave_ors OR gave_zinc OR gave_al OR gave_amox) THEN 'u5_treated' END,
            CASE WHEN gave_al                                THEN 'u5_treated_malaria'   END,
            CASE WHEN (gave_ors OR gave_zinc)                THEN 'u5_treated_diarrhoea' END,
            CASE WHEN (gave_amox AND has_pneumonia)          THEN 'u5_treated_pneumonia' END,
            CASE WHEN (rdt_result IS NOT NULL AND rdt_result <> 'not_done')
                THEN 'u5_tested_malaria' END
        ]) AS metric_id
    FROM base
),
aggregated AS (
    SELECT
        location_id,
        period_start,
        metric_id,
        COUNT(DISTINCT patient_id) AS value
    FROM metrics
    WHERE metric_id IS NOT NULL
    GROUP BY location_id, period_start, metric_id
)
SELECT
    l.location_id,
    m.period_start,
    COALESCE(a.value, 0) AS value,
    a.metric_id
FROM (SELECT DISTINCT location_id FROM base) l
CROSS JOIN months m
LEFT JOIN aggregated a
    ON l.location_id = a.location_id
   AND m.period_start = a.period_start
   AND a.metric_id IS NOT NULL;
