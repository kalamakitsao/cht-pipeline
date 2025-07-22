-- This file is part of the eCHIS CHT Pipeline project to generate form data for the NTD Campaigns
-- It extracts data from the adverse_event_follow_up_chp_ntd form

{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['reported']},
      {'columns': ['source_id']}
    ]
  )
}}

SELECT
  data_record.uuid,
  data_record.patient_id as patient_id,
  data_record.saved_timestamp,
  data_record.reported,
  data_record.grandparent_uuid AS reported_by_parent_parent,
  doc -> 'fields' ->> 'chw_area_id' AS chp_area_id,
  doc -> 'fields' ->> 'visited_contact_uuid' AS visited_contact_uuid,

  -- Basic identifiers
  doc -> 'fields' -> 'inputs' ->> 'source_id' AS source_id,
  doc -> 'fields' ->> 'patient_sex' AS patient_sex,
  doc -> 'fields' ->> 'patient_date_of_birth' AS patient_date_of_birth,

  -- Adverse event fields (from CHP and follow-up)
  doc -> 'fields' -> 'adverse_event' ->> 'patient_available' AS patient_available,
  doc -> 'fields' -> 'adverse_event' ->> 'patient_condition' AS patient_condition,
  doc -> 'fields' ->> 'c_adverse_events' AS c_adverse_events,
  doc -> 'fields' ->> 'c_react_start_date' AS c_react_start_date,
  doc -> 'fields' -> 'inputs' ->> 't_adverse_events' AS t_adverse_events,
  doc -> 'fields' -> 'inputs' ->> 'reaction_start_date' AS reaction_start_date,

  -- CHW area ID


  -- Geolocation
  doc -> 'geolocation' ->> 'latitude' AS latitude,
  doc -> 'geolocation' ->> 'longitude' AS longitude,
  doc -> 'geolocation' ->> 'accuracy' AS gps_accuracy,
  doc -> 'geolocation' ->> 'altitude' AS gps_altitude,
  doc -> 'geolocation' ->> 'speed' AS gps_speed,
  doc -> 'geolocation' ->> 'heading' AS gps_heading

FROM {{ ref('data_record') }} data_record
JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb
  ON couchdb._id = data_record.uuid
WHERE data_record.form = 'adverse_event_follow_up_chp_ntd'

{% if is_incremental() %}
  AND data_record.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}