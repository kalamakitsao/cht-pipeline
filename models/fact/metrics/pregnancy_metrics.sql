-- models/fact/metrics/pregnancy_metrics.sql
-- Add logic for currently pregnant, teen pregnancies, referred, deliveries
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
        phv.has_been_referred
    FROM {{ ref('pregnancy_home_visit') }} phv
    WHERE phv.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

base_pnc AS (
    SELECT
        pnc.reported_by_parent AS cha_id,
        pnc.reported,
        pnc.patient_id,
        pnc.place_of_delivery,
        pnc.pnc_service_count
    FROM {{ ref('postnatal_care_service') }} pnc
    WHERE pnc.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
),

currently_pregnant AS (
    SELECT b.cha_id AS location_id, p.period_id, 'currently_pregnant' AS metric_id,
           COUNT(DISTINCT b.patient_id) AS value
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE)
    GROUP BY b.cha_id, p.period_id
),

teen_pregnancies AS (
    SELECT b.cha_id AS location_id, p.period_id, 'teen_pregnancies' AS metric_id,
           COUNT(DISTINCT b.patient_id) AS value
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE (COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE))
      AND b.patient_age_in_years BETWEEN 10 AND 19
    GROUP BY b.cha_id, p.period_id
),

pregnant_referred AS (
    SELECT b.cha_id AS location_id, p.period_id, 'pregnant_women_referred' AS metric_id,
           COUNT(DISTINCT b.patient_id) AS value
    FROM base_phv b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.has_been_referred = TRUE
    GROUP BY b.cha_id, p.period_id
),

skilled_birth_attendance AS (
    SELECT b.cha_id AS location_id, p.period_id, 'skilled_birth_attendance' AS metric_id,
           COUNT(DISTINCT b.patient_id) AS value
    FROM base_pnc b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.place_of_delivery = 'health_facility'
    GROUP BY b.cha_id, p.period_id
),

deliveries AS (
    SELECT b.cha_id AS location_id, p.period_id, 'deliveries' AS metric_id,
           COUNT(DISTINCT b.patient_id) AS value
    FROM base_pnc b
    JOIN period_dates p ON b.reported BETWEEN p.start_date AND p.end_date
    WHERE b.pnc_service_count = 1
    GROUP BY b.cha_id, p.period_id
)

SELECT * FROM currently_pregnant
UNION ALL
SELECT * FROM teen_pregnancies
UNION ALL
SELECT * FROM pregnant_referred
UNION ALL
SELECT * FROM skilled_birth_attendance
UNION ALL
SELECT * FROM deliveries
