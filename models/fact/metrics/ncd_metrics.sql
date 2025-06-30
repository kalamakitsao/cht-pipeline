-- models/fact/metrics/ncd_metrics.sql
-- Add logic to count screenings and referrals for diabetes and hypertension
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH filtered_data AS (
    SELECT
        o.reported_by_parent AS location_id,
        o.reported::DATE AS reported_date,
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
        join {{ ref('patient_f_client') }} p on o.patient_id = p.uuid
    WHERE o.reported_by_parent IN (
        SELECT location_id FROM {{ ref('dim_location') }} WHERE level = 'chp area'
    )
),

unpivoted AS (
    SELECT location_id, reported_date, patient_id, 'screened_diabetes' AS metric_id
    FROM filtered_data
    WHERE screened_for_diabetes IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_diabetes_male' AS metric_id
    FROM filtered_data
    WHERE screened_for_diabetes IS TRUE AND sex = 'male'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_diabetes_female' AS metric_id
    FROM filtered_data
    WHERE screened_for_diabetes IS TRUE AND sex = 'female'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_diabetes' AS metric_id
    FROM filtered_data
    WHERE is_referred_diabetes IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_diabetes_male' AS metric_id
    FROM filtered_data
    WHERE is_referred_diabetes IS TRUE  AND sex = 'male'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_diabetes_female' AS metric_id
    FROM filtered_data
    WHERE is_referred_diabetes IS TRUE  AND sex = 'female'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_hypertension' AS metric_id
    FROM filtered_data
    WHERE screened_for_hypertension IS TRUE  AND sex = 'male'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_hypertension_male' AS metric_id
    FROM filtered_data
    WHERE screened_for_hypertension IS TRUE  AND sex = 'male'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_hypertension_female' AS metric_id
    FROM filtered_data
    WHERE screened_for_hypertension IS TRUE  AND sex = 'female'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_hypertension' AS metric_id
    FROM filtered_data
    WHERE is_referred_hypertension IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_hypertension_male' AS metric_id
    FROM filtered_data
    WHERE is_referred_hypertension IS TRUE  AND sex = 'male'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'referred_hypertension_female' AS metric_id
    FROM filtered_data
    WHERE is_referred_hypertension IS TRUE   AND sex = 'female'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'screened_mental_health' AS metric_id
    FROM filtered_data
    WHERE screened_for_mental_health IS TRUE 

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_for_mental_health_male' AS metric_id
    FROM filtered_data
    WHERE screened_for_mental_health IS TRUE  AND sex = 'male'

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'screened_for_mental_health_female' AS metric_id
    FROM filtered_data
    WHERE screened_for_mental_health IS TRUE  AND sex = 'female'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'referred_mental_health' AS metric_id
    FROM filtered_data
    WHERE is_referred_mental_health IS TRUE

    UNION ALL

    SELECT location_id, reported_date, patient_id, 'referred_mental_health_male' AS metric_id
    FROM filtered_data
    WHERE is_referred_hypertension IS TRUE  AND sex = 'male'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'referred_hmental_health_female' AS metric_id
    FROM filtered_data
    WHERE is_referred_mental_health IS TRUE   AND sex = 'female'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'over_5_referred' AS metric_id
    FROM filtered_data
    WHERE has_been_referred IS TRUE

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'over_5_referred_male' AS metric_id
    FROM filtered_data
    WHERE has_been_referred IS TRUE AND sex = 'male'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'over_5_referred_female' AS metric_id
    FROM filtered_data
    WHERE has_been_referred IS TRUE AND sex = 'female'

    UNION ALL
    
    SELECT location_id, reported_date, patient_id, 'over_5_assessments' AS metric_id
    FROM filtered_data
),

with_periods AS (
    SELECT
        u.location_id,
        p.period_id,
        u.metric_id,
        COUNT(DISTINCT u.patient_id) AS value
    FROM unpivoted u
    JOIN {{ ref('dim_period') }} p ON u.reported_date BETWEEN p.start_date AND p.end_date
    GROUP BY u.location_id, p.period_id, u.metric_id
)

SELECT
    location_id,
    period_id,
    metric_id,
    value,
    CURRENT_TIMESTAMP AS last_updated
FROM with_periods
WHERE value > 0
