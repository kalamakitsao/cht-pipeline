-- models/forms/campaign_service_smc.sql
-- Generates tables for the smc_campaign_service forms

{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [
   {'columns': ['caregiver_consent']},
   {'columns': ['smc_treatment_given']},
   {'columns': ['treatment_vomited']}
] -%}

{% set custom_fields %}
  data_record.patient_id as patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent,

  {{ patient_age_columns() }},
  
  -- Consent and eligibility
  NULLIF(couchdb.doc -> 'fields' -> 'sensitization' ->> 'caregiver_consent', '') AS caregiver_consent,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'fever', '') AS fever_status,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'rdt_results', '') AS rdt_result,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'danger_signs', '') AS danger_signs,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'spaq_allergy', '') AS spaq_allergy,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'taken_al', '') AS taken_al,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'cotrimazole', '') AS cotrimazole,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'has_smc_danger_signs', '') AS smc_danger_signs,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'have_paracetamol', '') AS has_paracetamol,
  NULLIF(couchdb.doc -> 'fields' -> 'eligibility' ->> 'end_workflow_eligibility', '') AS end_workflow_eligibility,
  
  -- Treatment details
  NULLIF(couchdb.doc -> 'fields' ->> 'danger_signs_referral_follow_up', '') AS danger_signs_referral_follow_up,
  NULLIF(couchdb.doc -> 'fields' ->> 'smc_treatment_given', '') AS smc_treatment_given,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'vomited', '') AS treatment_vomited,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'no_blister_packs_used', '') AS blister_packs_used,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'calc_blister_open', '') AS blister_open_method,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'redose', '') AS redose,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'received_spaq', '') AS received_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'conduct_malaria', '') AS conduct_malaria,
  NULLIF(couchdb.doc -> 'fields' -> 'smc_treatment' ->> 'when_give_smc_drugs', '') AS when_give_smc_drugs,
  -- Medication quantities
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'calc_pink_spaq_blister_packs', '') AS calc_pink_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'calc_green_spaq_blister_packs', '') AS calc_green_spaq,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'pink_spaq_blister_packs_quantity_issued', '') AS pink_spaq_issued,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'green_spaq_blister_packs_quantity_issued', '') AS green_spaq_issued,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'paracetamol_120_quantity_issued', '') AS paracetamol_120mg,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'paracetamol_500_quantity_issued', '') AS paracetamol_500mg,
  
  -- Referral and treatment status
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_refer_health_facility', '') AS referred_facility,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_smc_treatment_given', '') AS treatment_given_status,
  
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

{{ cht_form_model('campaign_service_smc', custom_fields, age_indexes + form_indexes) }}
