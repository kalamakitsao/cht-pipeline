-- models/forms/adverse_event_follow_up_chp_smc.sql
-- Extracts data from adverse_event_follow_up_chp_smc form

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
  {'columns': ['facility_attend']},
  {'columns': ['patient_available']}
] -%}

{% set custom_fields %}
  data_record.patient_id AS patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent,

  {{ patient_age_columns() }},

  -- Adverse event details
  NULLIF(couchdb.doc -> 'fields' ->> 't_chv_name', '') AS chv_name,
  NULLIF(couchdb.doc -> 'fields' ->> 't_chv_phone', '') AS chv_phone,
  NULLIF(couchdb.doc -> 'fields' ->> 't_adverse_events', '') AS adverse_events_reported,
  NULLIF(couchdb.doc -> 'fields' ->> 'reaction_start_date', '') AS reaction_start_date,
  NULLIF(couchdb.doc -> 'fields' ->> 'c_adverse_events', '') AS chp_adverse_events,
  NULLIF(couchdb.doc -> 'fields' ->> 'c_react_start_date', '') AS chp_reaction_start_date,

  -- Outcome and follow-up
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event' ->> 'facility_attend', '') AS facility_attend,
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event' ->> 'patient_available', '') AS patient_available,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_followup_outcome', '') AS followup_outcome,

  -- Referral status
  NULLIF(couchdb.doc -> 'fields' ->> 'needs_referral', '') AS needs_referral,

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
  END AS longitude
{% endset %}

{{ cht_form_model('adverse_event_follow_up_chp_smc', custom_fields, age_indexes + form_indexes) }}