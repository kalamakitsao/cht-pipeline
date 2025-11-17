{{ config(
  materialized = 'incremental',
  unique_key = 'uuid',
  tags = ['staging','death','cadence_hourly'],
  on_schema_change = 'sync_all_columns',
  indexes = [
    {'columns': ['reported_date']},
    {'columns': ['patient_age_in_days']},
    {'columns': ['sex']},
    {'columns': ['reported_by_parent','reported_date']}
  ]
) }}

-- Only load new rows
WITH max_ts AS (
  {% if is_incremental() %}
    SELECT COALESCE(MAX(reported), '2020-01-01') AS max_ts FROM {{ this }}
  {% else %}
    SELECT '2020-01-01'::timestamp AS max_ts
  {% endif %}
),

base AS (
  SELECT
    d.*,
    (d.reported)::date AS reported_date
  FROM {{ source(env_var('POSTGRES_SCHEMA'), 'death_report') }} d
  JOIN {{ ref('dim_location') }} dl
    ON d.reported_by_parent = dl.location_id
   AND dl.level = 'chp area'
  JOIN max_ts m ON d.reported > m.max_ts
),

-- lightweight join to avoid expensive table scans
enriched AS (
  SELECT
    b.uuid,
    b.reported,
    b.reported_date,
    b.reported_by_parent,
    b.patient_id,
    b.patient_age_in_days,
    b.death_type,
    b.place_of_death,
    b.date_of_death,
    ps.sex
  FROM base b
  LEFT JOIN {{ ref('dim_patient_sex') }} ps
    ON b.patient_id = ps.uuid
)

SELECT * FROM enriched
