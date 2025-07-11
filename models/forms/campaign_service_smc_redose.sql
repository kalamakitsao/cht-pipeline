-- models/forms/campaign_service_smc_redose.sql
-- Extracts data from campaign_service_smc_redose form

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
  {'columns': ['smc_treatment.redose']},
  {'columns': ['smc_treatment.no_blister_packs_used']},
  {'columns': ['group_summary.r_redose_given']}
] -%}

{% set custom_fields %}
  data_record.patient_id AS patient_id,
  data_record.contact.parent.parent._id AS reported_by_parent_parent,

  {{ patient_age_columns() }},

  -- Redose form specific fields
  NULLIF(couchdb.doc -> 'fields' -> 'eligible', '') AS eligible,
  NULLIF(couchdb.doc -> 'fields' -> 'over_one', '') AS over_one,
  NULLIF(couchdb.doc -> 'fields' -> 'over_two', '') AS over_two,
  
  NULLIF(couchdb.doc -> 'fields' -> 'patient_name', '') AS patient_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'chw_area_id', '') AS chw_area_id,

  -- SMC Treatment Fields
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'redose', '') AS redose,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'no_blister_packs_used', '') AS blister_packs_used,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'under_5_day1', '') AS under_5_day1,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'redose_only', '') AS redose_only,

  -- Consumption Fields
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'calc_pink_spaq_blister_packs', '') AS calc_pink_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'calc_green_spaq_blister_packs', '') AS calc_green_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'pink_spaq_blister_packs_quantity_issued', '') AS pink_spaq_issued,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'green_spaq_blister_packs_quantity_issued', '') AS green_spaq_issued,

  -- Redose referral status
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_redose_given', '') AS redose_given_status,

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

{{ cht_form_model('campaign_service_smc_redose', custom_fields, age_indexes + form_indexes) }}