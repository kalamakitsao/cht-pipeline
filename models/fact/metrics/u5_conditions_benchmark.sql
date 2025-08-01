{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH base AS (
    SELECT
        u5.reported_by_parent AS location_id,
        CAST(u5.reported AS DATE) AS reported_date,
        u5.patient_id,
        pax.sex,
        TRUE AS u5_assessed,
        u5.has_diarrhoea,
        u5.has_fever,
        (u5.has_fast_breathing IS TRUE AND u5.has_chest_indrawing IS TRUE) AS has_pneumonia,
        (u5.muac_color IN ('yellow', 'red')) AS has_malnutrition,
        (u5.rdt_result = 'positive') AS has_malaria,
        u5.referred_for_development_milestones,
        u5.has_been_referred,
        u5.rdt_result,
        u5.gave_amox,
        u5.gave_zinc,
        u5.gave_ors,
        u5.gave_al
    FROM {{ ref('u5_assessment') }} u5 
    JOIN {{ ref('patient_f_client') }} pax ON u5.patient_id = pax.uuid
    JOIN {{ ref('dim_location') }} loc ON u5.reported_by_parent = loc.location_id
    WHERE loc.level = 'chp area'

    {% if is_incremental() %}
      AND u5.reported >= (SELECT MIN(start_date) FROM {{ ref('dim_period') }} WHERE period_id >= (SELECT MAX(period_id) FROM {{ this }}))
    {% endif %}
),

with_periods AS (
    SELECT
        b.*,
        p.period_id
    FROM base b
    JOIN {{ ref('dim_period') }} p
      ON b.reported_date BETWEEN p.start_date AND p.end_date
),

metric_map AS (
    SELECT * FROM (
        VALUES
            ('u5_assessed', NULL, 'u5_assessed'),
            ('has_diarrhoea', NULL, 'u5_diarrhea_cases'),
            ('has_pneumonia', NULL, 'u5_pneumonia_cases'),
            ('has_malnutrition', NULL, 'u5_malnutrition_cases'),
            ('has_malaria', NULL, 'u5_confirmed_malaria_cases'),
            ('has_fever_no_other', NULL, 'u5_suspected_malaria_cases'),
            ('ref_malaria', NULL, 'referred_for_malaria'),
            ('ref_pneumonia', NULL, 'referred_for_pneumonia'),
            ('ref_malnutrition', NULL, 'referred_for_malnutrition'),
            ('ref_diarrhoea', NULL, 'referred_for_diarrhoea'),
            ('referred_for_development_milestones', NULL, 'referred_for_development_milestones'),
            ('referred_for_development_milestones', 'male', 'male_referred_for_development_milestones'),
            ('referred_for_development_milestones', 'female', 'female_referred_for_development_milestones'),
            ('has_been_referred', NULL, 'u5_referred'),
            ('has_been_referred', 'male', 'u5_referred_male'),
            ('has_been_referred', 'female', 'u5_referred_female'),
            ('any_treatment', NULL, 'u5_treated'),
            ('gave_al', NULL, 'u5_treated_malaria'),
            ('gave_ors_or_zinc', NULL, 'u5_treated_diarrhoea'),
            ('gave_amox_pneumonia', NULL, 'u5_treated_pneumonia'),
            ('rdt_done', NULL, 'u5_tested_malaria')
    ) AS m(field, sex_filter, metric_id)
),

with_flags AS (
    SELECT *,
        (has_fever AND NOT has_malaria AND NOT has_pneumonia AND NOT has_diarrhoea) AS has_fever_no_other,
        (has_fever AND has_been_referred) AS ref_malaria,
        (has_pneumonia AND has_been_referred) AS ref_pneumonia,
        (has_malnutrition AND has_been_referred) AS ref_malnutrition,
        (has_diarrhoea AND has_been_referred) AS ref_diarrhoea,
        (gave_ors OR gave_amox OR gave_al OR gave_zinc) AS any_treatment,
        (gave_ors OR gave_zinc) AS gave_ors_or_zinc,
        (gave_amox AND has_pneumonia) AS gave_amox_pneumonia,
        (rdt_result IS NOT NULL AND rdt_result <> 'not_done') AS rdt_done
    FROM with_periods
),

unpivoted AS (
    SELECT DISTINCT
        wp.location_id,
        wp.period_id,
        wp.patient_id,
        m.metric_id
    FROM with_flags wp
    JOIN metric_map m
      ON (
        (
            (m.field = 'u5_assessed'                       AND TRUE)
         OR (m.field = 'has_diarrhoea'                     AND wp.has_diarrhoea)
         OR (m.field = 'has_pneumonia'                     AND wp.has_pneumonia)
         OR (m.field = 'has_malnutrition'                  AND wp.has_malnutrition)
         OR (m.field = 'has_malaria'                       AND wp.has_malaria)
         OR (m.field = 'has_fever_no_other'                AND wp.has_fever_no_other)
         OR (m.field = 'ref_malaria'                       AND wp.ref_malaria)
         OR (m.field = 'ref_pneumonia'                     AND wp.ref_pneumonia)
         OR (m.field = 'ref_malnutrition'                  AND wp.ref_malnutrition)
         OR (m.field = 'ref_diarrhoea'                     AND wp.ref_diarrhoea)
         OR (m.field = 'referred_for_development_milestones' AND wp.referred_for_development_milestones)
         OR (m.field = 'has_been_referred'                 AND wp.has_been_referred)
         OR (m.field = 'any_treatment'                     AND wp.any_treatment)
         OR (m.field = 'gave_al'                           AND wp.gave_al)
         OR (m.field = 'gave_ors_or_zinc'                  AND wp.gave_ors_or_zinc)
         OR (m.field = 'gave_amox_pneumonia'               AND wp.gave_amox_pneumonia)
         OR (m.field = 'rdt_done'                          AND wp.rdt_done)
        )
        AND (m.sex_filter IS NULL OR m.sex_filter = wp.sex)
    )
),

final_counts AS (
    SELECT
        location_id,
        period_id,
        metric_id,
        COUNT(DISTINCT patient_id) AS value
    FROM unpivoted
    GROUP BY location_id, period_id, metric_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM final_counts
WHERE value > 0