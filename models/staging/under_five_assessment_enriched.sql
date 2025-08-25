-- models/staging/under_five_assessment_enriched.sql
{{ config(
  materialized = 'table',
  tags = ['staging', 'u5'],
  on_schema_change = 'ignore',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['reported_by_parent']},
    {'columns': ['patient_id']},
    {'columns': ['reported_by_parent','reported_date','patient_id']},
    {'columns': ['sex']}
  ]
) }}

-- u5_assessment already parsed complex JSON into columns like:
--   muac_color, has_fast_breathing, has_chest_indrawing, has_diarrhoea,
--   has_fever, rdt_result, gave_amox, gave_zinc, gave_ors, gave_al,
--   referred_for_development_milestones, has_been_referred, patient_id, reported_by_parent, reported
-- We enrich with derived flags + sex and add reported_date for indexing.

WITH base AS (
  SELECT
    u.*,
    -- indexable date for period mapping and pruning
    (u.reported)::date AS reported_date,
    -- derived flags used by metrics
    (u.has_fast_breathing IS TRUE AND u.has_chest_indrawing IS TRUE) AS has_pneumonia,
    (u.muac_color IN ('yellow','red'))                                AS has_malnutrition,
    (u.rdt_result = 'positive')                                       AS has_malaria
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'u5_assessment') }} u
  WHERE u.patient_id IS NOT NULL
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
