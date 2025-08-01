{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH filtered_data AS (
    SELECT
        o.reported_by_parent AS location_id,
        CAST(o.reported AS DATE) AS reported_date,
        o.patient_id,
        p.sex,
        o.screened_for_diabetes,
        o.is_referred_diabetes,
        o.screened_for_hypertension,
        o.is_referred_hypertension,
        o.screened_for_mental_health,
        o.is_referred_mental_health,
        o.has_been_referred
    FROM {{ ref('over_five_assessment') }} o
    JOIN {{ ref('patient_f_client') }} p ON o.patient_id = p.uuid
    JOIN {{ ref('dim_location') }} loc ON o.reported_by_parent = loc.location_id
    WHERE loc.level = 'chp area'

    {% if is_incremental() %}
      AND o.reported >= (SELECT MIN(start_date) FROM {{ ref('dim_period') }} WHERE period_id >= (SELECT MAX(period_id) FROM {{ this }}))
    {% endif %}
),

with_periods AS (
    SELECT
        f.*,
        p.period_id
    FROM filtered_data f
    JOIN {{ ref('dim_period') }} p
      ON f.reported_date BETWEEN p.start_date AND p.end_date
),

metric_map AS (
    SELECT * FROM (
        VALUES
            ('screened_for_diabetes',       NULL,      'screened_diabetes'),
            ('screened_for_diabetes',       'male',    'screened_diabetes_male'),
            ('screened_for_diabetes',       'female',  'screened_diabetes_female'),
            ('is_referred_diabetes',        NULL,      'referred_diabetes'),
            ('is_referred_diabetes',        'male',    'referred_diabetes_male'),
            ('is_referred_diabetes',        'female',  'referred_diabetes_female'),
            ('screened_for_hypertension',   NULL,      'screened_hypertension'),
            ('screened_for_hypertension',   'male',    'screened_hypertension_male'),
            ('screened_for_hypertension',   'female',  'screened_hypertension_female'),
            ('is_referred_hypertension',    NULL,      'referred_hypertension'),
            ('is_referred_hypertension',    'male',    'referred_hypertension_male'),
            ('is_referred_hypertension',    'female',  'referred_hypertension_female'),
            ('screened_for_mental_health',  NULL,      'screened_mental_health'),
            ('screened_for_mental_health',  'male',    'screened_mental_health_male'),
            ('screened_for_mental_health',  'female',  'screened_mental_health_female'),
            ('is_referred_mental_health',   NULL,      'referred_mental_health'),
            ('is_referred_mental_health',   'male',    'referred_mental_health_male'),
            ('is_referred_mental_health',   'female',  'referred_mental_health_female'),
            ('has_been_referred',           NULL,      'over_5_referred'),
            ('has_been_referred',           'male',    'over_5_referred_male'),
            ('has_been_referred',           'female',  'over_5_referred_female'),
            ('always_true',                 NULL,      'over_5_assessments')
    ) AS m(field, sex_filter, metric_id)
),

unpivoted AS (
    SELECT DISTINCT
        wp.location_id,
        wp.period_id,
        wp.patient_id,
        m.metric_id
    FROM with_periods wp
    JOIN metric_map m
      ON (
        (
            (m.field = 'screened_for_diabetes'       AND wp.screened_for_diabetes IS TRUE)
         OR (m.field = 'is_referred_diabetes'        AND wp.is_referred_diabetes IS TRUE)
         OR (m.field = 'screened_for_hypertension'   AND wp.screened_for_hypertension IS TRUE)
         OR (m.field = 'is_referred_hypertension'    AND wp.is_referred_hypertension IS TRUE)
         OR (m.field = 'screened_for_mental_health'  AND wp.screened_for_mental_health IS TRUE)
         OR (m.field = 'is_referred_mental_health'   AND wp.is_referred_mental_health IS TRUE)
         OR (m.field = 'has_been_referred'           AND wp.has_been_referred IS TRUE)
         OR (m.field = 'always_true')
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