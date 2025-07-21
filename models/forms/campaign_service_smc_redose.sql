-- This file is part of the eCHIS CHT Pipeline project to generate form data for the NTD Campaigns
-- models/forms/campaign_service_smc_redose.sql

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
   {'columns': ['redose']},
   {'columns': ['redose_only']},
   {'columns': ['calc_pink_spaq']},
   {'columns': ['calc_green_spaq']}
] -%}

{% set custom_fields %}
  -- Patient information
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_id', '') AS patient_id,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_gender', '') AS patient_gender,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_date_of_birth', '') AS patient_date_of_birth,
  
  {{ patient_age_columns() }},
  
  -- Eligibility information
  NULLIF(couchdb.doc -> 'fields' ->> 'eligible', '') AS eligible,
  NULLIF(couchdb.doc -> 'fields' ->> 'over_one', '') AS over_one,
  NULLIF(couchdb.doc -> 'fields' ->> 'over_two', '') AS over_two,
  
  -- Treatment details
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'redose', '') AS redose,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'redose_only', '') AS redose_only,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'under_5_day1', '') AS under_5_day1,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'no_blister_packs_used', '') AS no_blister_packs_used,
  
  -- Medication quantities
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'calc_pink_spaq_blister_packs', '') AS calc_pink_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'calc_green_spaq_blister_packs', '') AS calc_green_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'pink_spaq_blister_packs_quantity_issued', '') AS pink_spaq_issued,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'green_spaq_blister_packs_quantity_issued', '') AS green_spaq_issued,
  
  -- Workflow information
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_sync', '') AS r_sync,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_redose_given', '') AS r_redose_given,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_service_outcome', '') AS r_service_outcome,
  
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
  
  -- Contact information
  NULLIF(couchdb.doc -> 'contact' -> 'parent' ->> '_id', '') AS chv_area_id,
  NULLIF(couchdb.doc -> 'fields' ->> 'chw_area_id', '') AS chw_area_id

{% endset %}

{{ cht_form_model('campaign_service_smc_redose', custom_fields, age_indexes + form_indexes) }}