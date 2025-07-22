-- models/forms/report_adverse_event_ntd.sql

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['patient_uuid']},
      {'columns': ['patient_village']},
      {'columns': ['mild_events']},
      {'columns': ['severe_events']},
      {'columns': ['needs_referral']},
      {'columns': ['reported_date']}
    ]
  )
}}

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
  {'columns': ['patient_uuid']}, 
  {'columns': ['patient_village']},
  {'columns': ['mild_events']},
  {'columns': ['severe_events']},
  {'columns': ['needs_referral']},
  {'columns': ['adverse_event_type']}
] -%}

{% set custom_fields %}
  data_record.patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent,

  {{ patient_age_columns() }},

  -- Patient information
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_uuid', '') AS patient_uuid, 
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_name', '') AS patient_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_phone', '') AS patient_phone,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_village', '') AS patient_village,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_sex', '') AS patient_sex,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_age_display', '') AS patient_age_display,

  -- CHV information
  NULLIF(couchdb.doc -> 'fields' ->> 'chv_name', '') AS chv_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'chv_phone', '') AS chv_phone,
  NULLIF(couchdb.doc ->> 'from', '') AS reporter_phone,

  -- Adverse event flags
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'mild_events' IN ('true', 'false', 'yes', 'no')
    THEN LOWER(couchdb.doc -> 'fields' ->> 'mild_events') IN ('true', 'yes')
    ELSE NULL
  END AS mild_events,
  
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'severe_events' IN ('true', 'false', 'yes', 'no')
    THEN LOWER(couchdb.doc -> 'fields' ->> 'severe_events') IN ('true', 'yes')
    ELSE NULL
  END AS severe_events,
  
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'needs_referral' IN ('true', 'false', 'yes', 'no')
    THEN LOWER(couchdb.doc -> 'fields' ->> 'needs_referral') IN ('true', 'yes')
    ELSE NULL
  END AS needs_referral,

  -- Adverse event details
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event_group' ->> 'adverse_event', '') AS adverse_event_type,
  
  CASE 
    WHEN couchdb.doc -> 'fields' ->> 'unrelated_to_campaign' IN ('true', 'false', 'yes', 'no')
    THEN LOWER(couchdb.doc -> 'fields' ->> 'unrelated_to_campaign') IN ('true', 'yes')
    ELSE NULL
  END AS unrelated_to_campaign,

  -- Event timing
  CASE 
    WHEN couchdb.doc -> 'fields' -> 'adverse_event_group' ->> 'adverse_event_start_date' ~ '^\d{4}-\d{2}-\d{2}$'
    THEN (couchdb.doc -> 'fields' -> 'adverse_event_group' ->> 'adverse_event_start_date')::date
    ELSE NULL
  END AS adverse_event_start_date,

  -- Referral information
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event_group' ->> 'refer_patient_unrelated_event', '') AS referral_notes,

  -- Geolocation (with validation)
  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'latitude') ~ '^-?\d{1,3}\.\d+$'
    AND (couchdb.doc -> 'geolocation' ->> 'latitude')::numeric BETWEEN -90 AND 90
    THEN ROUND((couchdb.doc -> 'geolocation' ->> 'latitude')::numeric, 6)
    ELSE NULL
  END AS latitude,

  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'longitude') ~ '^-?\d{1,3}\.\d+$'
    AND (couchdb.doc -> 'geolocation' ->> 'longitude')::numeric BETWEEN -180 AND 180
    THEN ROUND((couchdb.doc -> 'geolocation' ->> 'longitude')::numeric, 6)
    ELSE NULL
  END AS longitude,

  -- Timestamps
  CASE 
    WHEN couchdb.doc ->> 'reported_date' ~ '^\d+$'
    THEN TO_TIMESTAMP((couchdb.doc ->> 'reported_date')::bigint / 1000)
    ELSE NULL
  END AS reported_date,

  -- Additional metadata
  NULLIF(couchdb.doc -> 'fields' ->> 'needs_signoff', '') AS needs_signoff,
  NULLIF(couchdb.doc -> 'fields' ->> 'visited_contact_uuid', '') AS visited_contact_uuid
{% endset %}

{{ cht_form_model('report_adverse_event_ntd', custom_fields, age_indexes + form_indexes) }}