-- This file is part of the eCHIS CHT Pipeline project to generate form data for the NTD Campaigns
-- It extracts data from the adverse_event_follow_up_cha_ntd form

-- models/forms/adverse_event_follow_up_cha_ntd.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['reported']},
      {'columns': ['chp_area_id']},
      {'columns': ['source_id']},
      {'columns': ['facility_attend']},
      {'columns': ['patient_available']},
      {'columns': ['followup_outcome']}
    ]
  )
}}

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
  {'columns': ['facility_attend']},
  {'columns': ['patient_available']},
  {'columns': ['followup_outcome']}
] -%}

{% set custom_fields %}
  data_record.uuid,
  data_record.reported,
  data_record.patient_id,
  data_record.saved_timestamp,
  data_record.grandparent_uuid AS reported_by_parent_parent,

  {{ patient_age_columns() }},

  -- Identifiers and contact
  NULLIF(couchdb.doc ->> 'from', '') AS chw_phone_number,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' ->> 'source_id', '') AS source_id,
  NULLIF(couchdb.doc -> 'fields' ->> 'chw_area_id', '') AS chp_area_id,
  NULLIF(couchdb.doc -> 'fields' ->> 'chp_name', '') AS chp_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'chp_phone', '') AS chp_phone,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_name', '') AS patient_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_phone', '') AS patient_phone,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_gender', '') AS patient_gender,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_village', '') AS patient_village,
  NULLIF(couchdb.doc -> 'fields' ->> 'village_name', '') AS village_name,

  -- CHV details
  NULLIF(couchdb.doc -> 'fields' ->> 't_chv_name', '') AS chv_name,
  NULLIF(couchdb.doc -> 'fields' ->> 't_chv_phone', '') AS chv_phone,
  NULLIF(couchdb.doc -> 'fields' ->> 'chp_name_probe', '') AS chp_name_probe,
  NULLIF(couchdb.doc -> 'fields' ->> 'chp_phone_probe', '') AS chp_phone_probe,

  -- Adverse event details
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'reaction_start_date' ~ '^\d{4}-\d{2}-\d{2}$'
    THEN (couchdb.doc -> 'fields' ->> 'reaction_start_date')::date
    ELSE NULL
  END AS reaction_start_date,
  
  -- Adverse events reported (1-28)
  NULLIF(couchdb.doc -> 'fields' ->> 't_adverse_events', '') AS t_adverse_events,
  {% for i in range(1, 10 + 1) %}
    NULLIF(couchdb.doc -> 'fields' ->> 't_adverse_events_{{ i }}', '') AS t_adverse_events_{{ i }}{% if not loop.last %},{% endif %}
  {% endfor %},

  -- Follow-up fields
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event' ->> 'facility_attend', '') AS facility_attend,
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event' ->> 'patient_available', '') AS patient_available,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_followup_outcome', '') AS followup_outcome,

  -- Initial assessment
  CASE 
    WHEN couchdb.doc -> 'fields' -> 'init' ->> 'n_events' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'init' ->> 'n_events')::int
    ELSE NULL
  END AS n_events,
  CASE 
    WHEN couchdb.doc -> 'fields' -> 'init' ->> 'n_follow_up' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'init' ->> 'n_follow_up')::int
    ELSE NULL
  END AS n_follow_up,
  NULLIF(couchdb.doc -> 'fields' -> 'init' ->> 'availability', '') AS availability,
  NULLIF(couchdb.doc -> 'fields' -> 'init' ->> 'facility_visit', '') AS facility_visit,
  NULLIF(couchdb.doc -> 'fields' -> 'init' ->> 'generated_note_name_113', '') AS generated_note_name_113,

  -- Referral status
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'needs_referral' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'needs_referral')::boolean
    ELSE NULL
  END AS needs_referral,
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'needs_signoff' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'needs_signoff')::boolean
    ELSE NULL
  END AS needs_signoff,

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

SELECT
  {{ custom_fields }}
FROM {{ ref('data_record') }} data_record
JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb
  ON couchdb._id = data_record.uuid
WHERE data_record.form = 'adverse_event_follow_up_cha_ntd'

{% if is_incremental() %}
  AND data_record.saved_timestamp >= (SELECT MAX(saved_timestamp) FROM {{ this }})
{% endif %}