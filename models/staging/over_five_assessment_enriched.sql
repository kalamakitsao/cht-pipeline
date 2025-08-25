-- models/staging/over_five_assessment_enriched.sql
{{ config(
  materialized = 'table',
  tags = ['staging', 'ncd'],
  on_schema_change = 'ignore',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['reported_by_parent']},
    {'columns': ['patient_id']},
    {'columns': ['sex']},
    {'columns': ['reported_by_parent','reported_date','patient_id']}
  ]
) }}

WITH base AS (
  SELECT
    o.*,
    (o.reported)::date AS reported_date
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'over_five_assessment') }} o
  WHERE o.patient_id IS NOT NULL
),

enriched AS (
  SELECT
    b.*,
    ps.sex
  FROM base b
  LEFT JOIN {{ ref('dim_patient_sex') }} ps
    ON b.patient_id = ps.uuid
)

SELECT * FROM enriched
