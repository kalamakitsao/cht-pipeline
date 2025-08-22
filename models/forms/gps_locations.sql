-- models/forms/adverse_event_follow_up_cha_ntd.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['reported']},
      {'columns': ['chp_area_id']}
    ]
  )
}}

{% set custom_fields %}
  data_record.uuid,
  data_record.reported,
  data_record.patient_id,
  data_record.form,
  data_record.saved_timestamp,
  data_record.contact_uuid AS reported_by,
  nullif(data_record.place_id, '') as household_id,
  data_record.parent_uuid as chp_area_id,

  -- Geolocation
  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'latitude') ~ '^-?\d{1,3}(\.\d+)?$'
    THEN ROUND((couchdb.doc -> 'geolocation' ->> 'latitude')::numeric, 6)
    ELSE NULL
  END AS latitude,
  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'longitude') ~ '^-?\d{1,3}(\.\d+)?$'
    THEN ROUND((couchdb.doc -> 'geolocation' ->> 'longitude')::numeric, 6)
    ELSE NULL
  END AS longitude,
  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'accuracy') ~ '^\d+(\.\d+)?$'
    THEN (couchdb.doc -> 'geolocation' ->> 'accuracy')::float
    ELSE NULL
  END AS location_accuracy,

  -- Timestamps
  CASE 
    WHEN couchdb.doc ->> 'reported_date' ~ '^\d+$'
    THEN TO_TIMESTAMP((couchdb.doc ->> 'reported_date')::bigint / 1000)
    ELSE NULL
  END AS reported_date
{% endset %}

{% set couchdb_table = env_var('POSTGRES_SCHEMA') ~ '.' ~ env_var('POSTGRES_TABLE') %}

SELECT
  {{ custom_fields }}
FROM {{ ref('data_record') }} data_record
JOIN {{ couchdb_table }} couchdb
  ON couchdb._id = data_record.uuid
WHERE data_record.contact_uuid is not null 
  and couchdb.doc -> 'geolocation' ->> 'accuracy' is not null

{% if is_incremental() %}
  AND data_record.saved_timestamp >= (SELECT MAX(saved_timestamp) FROM {{ this }})
{% endif %}