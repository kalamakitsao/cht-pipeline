-- models/staging/sha_registration_enriched.sql
{{ config(
  materialized = 'incremental',
  unique_key   = 'uuid',
  tags         = ['staging','sha','cadence_hourly'],
  on_schema_change = 'sync_all_columns',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['location_id']},
    {'columns': ['member_uuid']},
    {'columns': ['household_id']},
    {'columns': ['has_sha_registration']},
    {'columns': ['location_id','reported_date']}
  ]
) }}

-- 1. Find the max saved_timestamp already loaded
WITH max_saved AS (
  {% if is_incremental() %}
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp) AS max_ts
    FROM {{ this }}
  {% else %}
    SELECT '2020-01-01'::timestamp AS max_ts
  {% endif %}
),

-- 2. Pull only new/updated sha_registration records
base AS (
  SELECT
    sr.uuid,
    sr.saved_timestamp,
    sr.reported,
    (sr.reported)::date AS reported_date,
    sr.reported_by_parent,
    sr.member_uuid,
    sr.has_sha_registration,
    sr.cert_seen_by_chp
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'sha_registration') }} sr
  JOIN max_saved m
    ON sr.saved_timestamp > m.max_ts
  WHERE sr.reported IS NOT NULL
    AND sr.member_uuid IS NOT NULL
),

-- 3. Join to a *narrow* projection of household_person_enriched
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
  LEFT JOIN (
    SELECT uuid, chp_area_id, household_id
    FROM {{ ref('household_person_enriched') }}
  ) hp
    ON hp.uuid = b.member_uuid
)

SELECT * FROM joined
