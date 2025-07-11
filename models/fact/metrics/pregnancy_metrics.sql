-- models/fact/metrics/pregnancy_metrics.sql

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH period_dates AS (
    SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_phv AS (
    SELECT
        phv.reported_by_parent AS cha_id,
        phv.reported,
        phv.patient_id,
        phv.patient_age_in_years,
        phv.is_currently_pregnant,
        phv.is_new_pregnancy,
        phv.has_been_referred,
        phv.has_started_anc,
        phv.is_anc_upto_date,
        phv.current_edd
    FROM {{ ref('pregnancy_home_visit') }} phv
    WHERE phv.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

base_pnc AS (
    SELECT
        pnc.reported_by_parent AS cha_id,
        pnc.reported,
        pnc.patient_id,
        pnc.place_of_delivery,
        pnc.pnc_service_count,
        pnc.date_of_delivery,
        pnc.is_referred_for_pnc_services
    FROM {{ ref('postnatal_care_service') }} pnc
    WHERE pnc.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

currently_pregnant AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'currently_pregnant' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE)
    GROUP BY b.cha_id, p.period_id
),

new_pregnancies AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'new_pregnancies' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE COALESCE(b.is_new_pregnancy, TRUE)
    GROUP BY b.cha_id, p.period_id
),

new_teen_pregnancies AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'new_teen_pregnancies' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE COALESCE(b.is_new_pregnancy, TRUE)
      AND b.patient_age_in_years BETWEEN 10 AND 19
    GROUP BY b.cha_id, p.period_id
),

teen_pregnancies AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'teen_pregnancies' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE (COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE))
      AND b.patient_age_in_years BETWEEN 10 AND 19
    GROUP BY b.cha_id, p.period_id
),

new_pregnant_women_referred_anc AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'new_pregnant_women_referred_anc' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.has_been_referred = TRUE 
      AND b.is_new_pregnancy = TRUE 
      AND b.has_started_anc = FALSE
    GROUP BY b.cha_id, p.period_id
),

pregnant_women_referred_anc AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'pregnant_women_referred_anc' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.has_been_referred = TRUE 
      AND b.is_anc_upto_date = FALSE
    GROUP BY b.cha_id, p.period_id
),

pregnant_women_referred AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'pregnant_women_referred' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.has_been_referred = TRUE
    GROUP BY b.cha_id, p.period_id
),

pregnant_women_visited AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'pregnant_women_visited' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    GROUP BY b.cha_id, p.period_id
),

skilled_birth_attendance AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'skilled_birth_attendance' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_pnc b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.place_of_delivery = 'health_facility'
    GROUP BY b.cha_id, p.period_id
),

deliveries AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'deliveries' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_pnc b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.pnc_service_count = 1
    GROUP BY b.cha_id, p.period_id
),

pnc_referrals AS (
    SELECT
        b.cha_id AS location_id,
        p.period_id,
        'referred_pnc' AS metric_id,
        COUNT(DISTINCT b.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_pnc b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.is_referred_for_pnc_services IS TRUE
    GROUP BY b.cha_id, p.period_id
),

latest_phv_with_edd AS (
    SELECT
        phv.patient_id,
        phv.reported_by_parent AS cha_id,
        phv.current_edd,
        ROW_NUMBER() OVER (
            PARTITION BY phv.patient_id
            ORDER BY phv.reported DESC
        ) AS rn
    FROM {{ ref('pregnancy_home_visit') }} phv
    WHERE phv.is_currently_pregnant = TRUE
      AND phv.current_edd IS NOT NULL
),

pregnancy_window AS (
    SELECT
        patient_id,
        cha_id,
        current_edd,
        current_edd - INTERVAL '9 months' AS pregnancy_start
    FROM latest_phv_with_edd
    WHERE rn = 1
),

actively_pregnant_women AS (
    SELECT
        p.cha_id AS location_id,
        d.period_id,
        'actively_pregnant_women' AS metric_id,
        COUNT(DISTINCT p.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM pregnancy_window p
    JOIN period_dates d ON 
        p.pregnancy_start < d.end_date
        AND p.current_edd >= d.start_date
    GROUP BY p.cha_id, d.period_id
),
first_trimester_pregnancies AS (
    SELECT
        phv.cha_id AS location_id,
        p.period_id,
        'first_trimester_pregnancies' AS metric_id,
        COUNT(DISTINCT phv.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv phv
    JOIN period_dates p ON phv.reported BETWEEN p.start_date AND p.end_date
    WHERE phv.current_edd IS NOT NULL
      AND (((phv.current_edd::date - CURRENT_DATE) / 7.0 - 40) * -1) BETWEEN 0 AND 12
      AND COALESCE(phv.is_new_pregnancy, FALSE)
    GROUP BY phv.cha_id, p.period_id
),

latest_delivery AS (
    SELECT
        patient_id,
        MAX(date_of_delivery) AS last_delivery_date
    FROM base_pnc
    WHERE date_of_delivery IS NOT NULL
    GROUP BY patient_id
),

repeat_pregnancies AS (
    SELECT
        phv.cha_id AS location_id,
        p.period_id,
        'repeat_pregnancies' AS metric_id,
        COUNT(DISTINCT phv.patient_id) AS value,
        CURRENT_TIMESTAMP AS last_updated
    FROM base_phv phv
    JOIN latest_delivery d ON d.patient_id = phv.patient_id
    JOIN period_dates p ON phv.reported BETWEEN p.start_date AND p.end_date
    WHERE phv.is_new_pregnancy = TRUE
      AND phv.reported::date BETWEEN d.last_delivery_date + INTERVAL '1 day'
                                  AND d.last_delivery_date + INTERVAL '12 months'
    GROUP BY phv.cha_id, p.period_id
)

-- Final union of all metrics
SELECT * FROM currently_pregnant
UNION ALL
SELECT * FROM new_pregnancies
UNION ALL
SELECT * FROM new_teen_pregnancies
UNION ALL
SELECT * FROM teen_pregnancies
UNION ALL
SELECT * FROM new_pregnant_women_referred_anc
UNION ALL
SELECT * FROM pregnant_women_referred_anc
UNION ALL
SELECT * FROM pregnant_women_referred
UNION ALL
SELECT * FROM pregnant_women_visited
UNION ALL
SELECT * FROM skilled_birth_attendance
UNION ALL
SELECT * FROM deliveries
UNION ALL
SELECT * FROM actively_pregnant_women
UNION ALL
SELECT * FROM first_trimester_pregnancies
UNION ALL
SELECT * FROM repeat_pregnancies