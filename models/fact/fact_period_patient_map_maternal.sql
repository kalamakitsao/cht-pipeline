{{ config(
    materialized = 'incremental',
    on_schema_change = 'append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'patient_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

base_phv AS (
  SELECT
    phv.reported_by_parent AS location_id,
    DATE(phv.reported) AS reported_date,
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
    AND phv.patient_id IS NOT NULL
),

base_pnc AS (
  SELECT
    pnc.reported_by_parent AS location_id,
    DATE(pnc.reported) AS reported_date,
    pnc.patient_id,
    pnc.place_of_delivery,
    pnc.pnc_service_count,
    pnc.date_of_delivery,
    pnc.is_referred_for_pnc_services
  FROM {{ ref('postnatal_care_service') }} pnc
  WHERE pnc.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
    AND pnc.patient_id IS NOT NULL
),

phv_mapped AS (
  SELECT
    p.period_id,
    b.location_id,
    b.patient_id,
    UNNEST(ARRAY_REMOVE(ARRAY[
      CASE WHEN COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE) THEN 'currently_pregnant' END,
      CASE WHEN COALESCE(b.is_new_pregnancy, TRUE) THEN 'new_pregnancies' END,
      CASE WHEN COALESCE(b.is_new_pregnancy, TRUE) AND b.patient_age_in_years BETWEEN 10 AND 19 THEN 'new_teen_pregnancies' END,
      CASE WHEN (COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE)) AND b.patient_age_in_years BETWEEN 10 AND 19 THEN 'teen_pregnancies' END,
      CASE WHEN b.has_been_referred AND b.is_new_pregnancy = TRUE AND b.has_started_anc = FALSE THEN 'new_pregnant_women_referred_anc' END,
      CASE WHEN b.has_been_referred AND b.is_anc_upto_date = FALSE THEN 'pregnant_women_referred_anc' END,
      CASE WHEN b.has_been_referred THEN 'pregnant_women_referred' END,
      'pregnant_women_visited',
      CASE WHEN b.current_edd IS NOT NULL AND (((b.current_edd::date - CURRENT_DATE) / 7.0 - 40) * -1) BETWEEN 0 AND 12 AND COALESCE(b.is_new_pregnancy, FALSE) THEN 'first_trimester_pregnancies' END
    ], NULL)) AS metric_id
  FROM base_phv b
  JOIN periods p ON b.reported_date BETWEEN p.start_date AND p.end_date
),

pnc_mapped AS (
  SELECT
    p.period_id,
    b.location_id,
    b.patient_id,
    UNNEST(ARRAY_REMOVE(ARRAY[
      CASE WHEN b.place_of_delivery = 'health_facility' THEN 'skilled_birth_attendance' END,
      CASE WHEN b.pnc_service_count = 1 THEN 'deliveries' END,
      CASE WHEN b.is_referred_for_pnc_services IS TRUE THEN 'referred_pnc' END
    ], NULL)) AS metric_id
  FROM base_pnc b
  JOIN periods p ON b.reported_date BETWEEN p.start_date AND p.end_date
),

latest_delivery AS (
  SELECT patient_id, MAX(date_of_delivery) AS last_delivery_date
  FROM base_pnc
  WHERE date_of_delivery IS NOT NULL
  GROUP BY patient_id
),

repeat_pregnancies AS (
  SELECT
    p.period_id,
    b.location_id,
    b.patient_id,
    'repeat_pregnancies' AS metric_id
  FROM base_phv b
  JOIN latest_delivery d ON d.patient_id = b.patient_id
  JOIN periods p ON b.reported_date BETWEEN p.start_date AND p.end_date
  WHERE b.is_new_pregnancy = TRUE
    AND b.reported_date BETWEEN d.last_delivery_date + INTERVAL '1 day' AND d.last_delivery_date + INTERVAL '12 months'
)

SELECT * FROM phv_mapped
UNION ALL
SELECT * FROM pnc_mapped
UNION ALL
SELECT * FROM repeat_pregnancies
