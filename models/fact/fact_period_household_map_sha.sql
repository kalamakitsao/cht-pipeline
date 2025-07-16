{{ config(
    materialized = 'incremental',
    on_schema_change='append_new_columns',
    unique_key = ['period_id', 'location_id', 'metric_id', 'household_id'],
    tags = ['daily_refresh']
) }}

WITH periods AS (
  SELECT period_id, start_date, end_date FROM {{ ref('dim_period') }}
),

latest_sha_registration AS (
  SELECT DISTINCT ON (c.household_id)
    c.household_id,
    sr.reported_by_parent AS location_id,
    DATE(sr.reported) AS report_date,
    sr.has_sha_registration
  FROM {{ ref('sha_registration') }} sr
  INNER JOIN {{ ref('patient_f_client') }} c ON sr.member_uuid = c.uuid
  WHERE sr.reported_by_parent IN (SELECT location_id FROM {{ ref('dim_location') }})
  ORDER BY c.household_id, sr.reported DESC
),

mapped AS (
  SELECT
    p.period_id AS period_id,
    s.location_id,
    s.household_id,
    unnest(ARRAY_REMOVE(ARRAY[
      'households_assessed_sha',
      CASE WHEN s.has_sha_registration IS TRUE THEN 'households_with_sha' END
    ], NULL)) AS metric_id
  FROM latest_sha_registration s
  JOIN periods p ON s.report_date BETWEEN p.start_date AND p.end_date
)

SELECT * FROM mapped
