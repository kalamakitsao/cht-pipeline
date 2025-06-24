{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [
      {'columns': ['is_referred_immunization']},
      {'columns': ['is_referred_for_pnc_services']},
      {'columns': ['pnc_service_count']}]
-%}
{% set custom_fields %}
  data_record.patient_id,
  data_record.grandparent_uuid as reported_by_parent_parent,
  {{ patient_age_columns() }},
  (couchdb.doc->'fields'->'inputs'->>'source'::text) AS source,
  (couchdb.doc->'fields'->'inputs'->>'source_id'::text) AS source_id,
  (couchdb.doc->'fields'->>'place_of_birth_display'::text) AS place_of_birth_display,
  NULLIF(NULLIF(couchdb.doc->'fields'->>'postnatal_care_service_count', ''), 'NaN')::integer AS pnc_service_count,
  CASE
    WHEN (couchdb.doc->'fields'->>'is_immunization_defaulter'::text) = 'yes'::text THEN true
    ELSE false
  END AS is_immunization_defaulter,
  CASE
    WHEN (couchdb.doc->'fields'->>'needs_immunization_follow_up'::text) = 'yes'::text THEN true
    ELSE false
  END AS needs_immunization_follow_up,
  CASE
    WHEN (couchdb.doc->'fields'->>'needs_immunization_follow_up'::text) = 'yes'::text THEN true
    ELSE false
  END AS is_referred_immunization,  
  CASE
    WHEN (couchdb.doc->'fields'->>'needs_danger_signs_follow_up'::text) = 'yes'::text THEN true
    ELSE false
  END AS needs_danger_signs_follow_up, 
  CASE
    WHEN (couchdb.doc->'fields'->>'needs_missed_visit_follow_up'::text) = 'yes'::text THEN true
    ELSE false
  END AS needs_missed_visit_follow_up, 
  NULLIF(couchdb.doc->'fields'->>'needs_pnc_update_follow_up', '')::boolean AS is_referred_for_pnc_services,
  NULLIF(couchdb.doc->'fields'->>'is_referred', '')::boolean AS is_referred
{% endset %}

{{ cht_form_model('postnatal_care_service_newborn', custom_fields, age_indexes + form_indexes) }}
