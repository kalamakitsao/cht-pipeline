{{ config(
  materialized = 'incremental',
  unique_key = ['location_id', 'period_id', 'metric_id'],
  tags = ['kpi', 'pregnancy'],
  on_schema_change = 'ignore'
) }}

WITH date_filter AS (
  SELECT DISTINCT date, period_id
  FROM {{ ref('dim_period_date_map') }}
),

phv_with_period AS (
  SELECT
    phv.reported_by_parent AS location_id,
    phv.reported::date AS date,
    phv.patient_id,
    phv.patient_age_in_years,
    phv.is_currently_pregnant,
    phv.is_new_pregnancy,
    phv.has_been_referred,
    phv.has_started_anc,
    phv.is_anc_upto_date,
    phv.current_edd,
    d.period_id
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'pregnancy_home_visit') }} phv
  JOIN date_filter d ON phv.reported::date = d.date
),

unnested_phv_metrics AS (
  SELECT
    location_id,
    period_id,
    UNNEST(ARRAY[
      CASE WHEN is_currently_pregnant OR is_new_pregnancy THEN 'currently_pregnant' END,
      CASE WHEN is_new_pregnancy THEN 'new_pregnancies' END,
      CASE WHEN is_new_pregnancy AND patient_age_in_years BETWEEN 10 AND 19 THEN 'new_teen_pregnancies' END,
      CASE WHEN (is_new_pregnancy OR is_currently_pregnant) AND patient_age_in_years BETWEEN 10 AND 19 THEN 'teen_pregnancies' END,
      CASE WHEN TRUE THEN 'pregnant_women_visited' END,
      CASE WHEN has_been_referred IS TRUE THEN 'pregnant_women_referred' END,
      CASE WHEN has_been_referred IS TRUE AND is_anc_upto_date IS FALSE THEN 'pregnant_women_referred_anc' END,
      CASE WHEN is_new_pregnancy IS TRUE AND has_been_referred IS TRUE AND has_started_anc IS FALSE THEN 'new_pregnant_women_referred_anc' END,
      CASE WHEN is_new_pregnancy IS TRUE AND has_been_referred IS TRUE AND has_started_anc IS FALSE AND patient_age_in_years BETWEEN 10 AND 19 THEN 'new_teen_pregnant_women_referred_anc' END,
      CASE WHEN is_new_pregnancy IS TRUE AND current_edd IS NOT NULL AND (((current_edd::date - CURRENT_DATE) / 7.0 - 40) * -1) BETWEEN 0 AND 12 THEN 'first_trimester_pregnancies' END
    ]) AS metric_id,
    patient_id
  FROM phv_with_period
),

phv_kpis AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(DISTINCT patient_id) AS value,
    CURRENT_TIMESTAMP AS last_updated
  FROM unnested_phv_metrics
  WHERE metric_id IS NOT NULL
  GROUP BY 1, 2, 3
),

pnc_with_period AS (
  SELECT
    pnc.reported_by_parent AS location_id,
    pnc.reported::date AS date,
    pnc.patient_id,
    pnc.place_of_delivery,
    pnc.pnc_service_count,
    pnc.date_of_delivery,
    pnc.is_referred_for_pnc_services,
    d.period_id
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'postnatal_care_service') }} pnc
  JOIN date_filter d ON pnc.reported::date = d.date
),

unnested_pnc_metrics AS (
  SELECT
    location_id,
    period_id,
    UNNEST(ARRAY[
      CASE WHEN pnc_service_count = 1 THEN 'deliveries' END,
      CASE WHEN place_of_delivery = 'health_facility' THEN 'skilled_birth_attendance' END,
      CASE WHEN is_referred_for_pnc_services IS TRUE THEN 'referred_pnc' END
    ]) AS metric_id,
    patient_id
  FROM pnc_with_period
),

pnc_kpis AS (
  SELECT
    location_id,
    period_id,
    metric_id,
    COUNT(DISTINCT patient_id) AS value,
    CURRENT_TIMESTAMP AS last_updated
  FROM unnested_pnc_metrics
  WHERE metric_id IS NOT NULL
  GROUP BY 1, 2, 3
),

latest_phv_with_edd AS (
  SELECT DISTINCT ON (patient_id)
         patient_id,
         reported_by_parent AS location_id,
         current_edd,
         reported::date AS reported_date
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'pregnancy_home_visit') }}
  WHERE is_currently_pregnant = TRUE AND current_edd IS NOT NULL
  ORDER BY patient_id, reported DESC
),

pregnancy_window AS (
  SELECT
    patient_id,
    location_id,
    current_edd,
    current_edd - INTERVAL '9 months' AS pregnancy_start
  FROM latest_phv_with_edd
),

actively_pregnant_women AS (
  SELECT
    pw.location_id,
    pd.period_id,
    'actively_pregnant_women' AS metric_id,
    COUNT(DISTINCT pw.patient_id) AS value,
    CURRENT_TIMESTAMP AS last_updated
  FROM pregnancy_window pw
  JOIN {{ ref('dim_period_date_map') }} pd ON pd.date BETWEEN pw.pregnancy_start AND pw.current_edd
  GROUP BY 1, 2
),

latest_delivery AS (
  SELECT
    patient_id,
    MAX(date_of_delivery) AS last_delivery_date
  FROM pnc_with_period
  WHERE date_of_delivery IS NOT NULL
  GROUP BY patient_id
),

repeat_pregnancies AS (
  SELECT
    phv.location_id,
    phv.period_id,
    'repeat_pregnancies' AS metric_id,
    COUNT(DISTINCT phv.patient_id) AS value,
    CURRENT_TIMESTAMP AS last_updated
  FROM phv_with_period phv
  JOIN latest_delivery d ON d.patient_id = phv.patient_id
  WHERE is_new_pregnancy IS TRUE
    AND phv.date BETWEEN d.last_delivery_date + INTERVAL '1 day'
                     AND d.last_delivery_date + INTERVAL '12 months'
  GROUP BY 1, 2
)

SELECT * FROM phv_kpis
UNION ALL SELECT * FROM pnc_kpis
UNION ALL SELECT * FROM actively_pregnant_women
UNION ALL SELECT * FROM repeat_pregnancies
