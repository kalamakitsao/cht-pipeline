-- models/forms/report_adverse_event_smc.sql
-- Extracts data from report_adverse_event_smc form

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
  {'columns': ['adverse_event_group.adverse_event']},
  {'columns': ['mild_events']},
  {'columns': ['severe_events']},
  {'columns': ['needs_referral']}
] -%}

{% set custom_fields %}
  data_record.patient_id AS patient_id,
  data_record.contact.parent.parent._id AS reported_by_parent_parent,

  {{ patient_age_columns() }},

  -- Patient and contact info
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_uuid', '') AS patient_uuid,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_name', '') AS patient_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_phone', '') AS patient_phone,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_village', '') AS patient_village,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_sex', '') AS patient_sex,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_date_of_birth', '') AS patient_date_of_birth,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_age_years', '') AS patient_age_in_years,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_age_in_months', '') AS patient_age_in_months,

  -- CHV info
  NULLIF(couchdb.doc -> 'fields' ->> 'chv_name', '') AS chv_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'chv_phone', '') AS chv_phone,

  -- Adverse event flags
  NULLIF(couchdb.doc -> 'fields' ->> 'mild_events', '') AS mild_events,
  NULLIF(couchdb.doc -> 'fields' ->> 'severe_events', '') AS severe_events,
  NULLIF(couchdb.doc -> 'fields' ->> 'needs_referral', '') AS needs_referral,

  -- Adverse event group
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event_group' ->> 'adverse_event', '') AS reported_adverse_event,
  NULLIF(couchdb.doc -> 'fields' -> 'adverse_event_group' ->> 'refer_patient', '') AS refer_patient,

  -- Geolocation (valid numeric check)
  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'latitude') ~ '^-?\d+(\.\d+)?$'
    THEN ROUND((couchdb.doc -> 'geolocation' ->> 'latitude')::numeric, 6)
    ELSE NULL
  END AS latitude,

  CASE
    WHEN (couchdb.doc -> 'geolocation' ->> 'longitude') ~ '^-?\d+(\.\d+)?$'
    THEN ROUND((couchdb.doc -> 'geolocation' ->> 'longitude')::numeric, 6)
    ELSE NULL
  END AS longitude
{% endset %}

{{ cht_form_model('report_adverse_event_smc', custom_fields, age_indexes + form_indexes) }}