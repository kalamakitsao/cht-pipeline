-- models/staging/sha_registration_enriched.sql
{{ config(
  materialized = 'table',
  tags = ['staging','sha'],
  on_schema_change = 'ignore',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['location_id']},
    {'columns': ['member_uuid']},
    {'columns': ['household_id']},
    {'columns': ['has_sha_registration']},
    {'columns': ['location_id','reported_date']} 
  ]
) }}

WITH base AS (
  SELECT
    sr.uuid,
    sr.saved_timestamp,
    sr.reported,
    (sr.reported)::date                    AS reported_date,
    sr.reported_by_parent,
    sr.member_uuid,
    sr.has_sha_registration,
    sr.cert_seen_by_chp
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'sha_registration') }} sr
  WHERE sr.reported IS NOT NULL
    AND sr.member_uuid IS NOT NULL
),

joined AS (
  SELECT
    b.uuid,
    b.saved_timestamp,
    b.reported,
    b.reported_date,
    /* prefer the household’s CHP area; fall back to reporter if missing */
    COALESCE(hp.chp_area_id, b.reported_by_parent) AS location_id,
    b.member_uuid,
    hp.household_id,
    b.has_sha_registration,
    b.cert_seen_by_chp
  FROM base b
  LEFT JOIN {{ ref('household_person_enriched') }} hp
    ON hp.uuid = b.member_uuid
)

SELECT * FROM joined
