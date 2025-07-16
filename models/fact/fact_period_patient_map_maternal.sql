{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
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
    phv.has_been_referred
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
    pnc.pnc_service_count
  FROM {{ ref('postnatal_care_service') }} pnc
  WHERE pnc.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
    AND pnc.patient_id IS NOT NULL
),

phv_mapped AS (
  SELECT
    p.period_id AS period_id,
    b.location_id,
    b.patient_id,
    unnest(
      ARRAY_REMOVE(ARRAY[
        CASE WHEN COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE)
             THEN 'currently_pregnant' END,
        CASE WHEN (COALESCE(b.is_currently_pregnant, FALSE) OR COALESCE(b.is_new_pregnancy, FALSE))
                  AND b.patient_age_in_years BETWEEN 10 AND 19
             THEN 'teen_pregnancies' END,
        CASE WHEN b.has_been_referred THEN 'pregnant_women_referred' END
      ], NULL)
    ) AS metric_id
  FROM base_phv b
  JOIN periods p ON b.reported_date BETWEEN p.start_date AND p.end_date
),

pnc_mapped AS (
  SELECT
    p.period_id AS period_id,
    b.location_id,
    b.patient_id,
    unnest(
      ARRAY_REMOVE(ARRAY[
        CASE WHEN b.place_of_delivery = 'health_facility' THEN 'skilled_birth_attendance' END,
        CASE WHEN b.pnc_service_count = 1 THEN 'deliveries' END
      ], NULL)
    ) AS metric_id
  FROM base_pnc b
  JOIN periods p ON b.reported_date BETWEEN p.start_date AND p.end_date
)

SELECT * FROM phv_mapped
UNION ALL
SELECT * FROM pnc_mapped
