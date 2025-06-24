{%- set age_indexes = patient_age_indexes() -%}
{% set custom_fields %}
  data_record.patient_id AS patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent,
  {{ patient_age_columns() }},
  CASE
    WHEN (couchdb.doc->'fields'->'family_planning'->>'is_on_fp') = 'yes' 
    THEN true
    ELSE false
  END AS is_fp_registration,
  CASE
    WHEN (couchdb.doc->'fields'->'family_planning'->>'is_on_fp') = 'no' OR 
         (couchdb.doc->'fields'->'family_planning'->>'is_on_fp') is null
    THEN true
    ELSE false
  END AS is_fp_follow_up,
  CASE
    WHEN 
      (couchdb.doc->'fields'->'referral_summary'->>'referred_for_refills') = 'yes' OR
      (couchdb.doc->'fields'->'referral_summary'->>'referred_for_fp_services') = 'yes'
    THEN true
    ELSE false
  END AS is_referred_for_fp_services,
  CASE
    WHEN 
      (couchdb.doc->'fields'->'family_planning'->>'is_on_fp') = 'no' AND
      (couchdb.doc->'fields'->'family_planning'->>'is_pregnant') = 'no'
    THEN true
    ELSE false
  END AS is_counselled_on_fp_services,
  NULLIF(couchdb.doc->'fields'->'family_planning'->>'refilled_today', '')::boolean AS is_provided_fp_commodities,
  CASE
    WHEN 
      (couchdb.doc->'fields'->'family_planning'->>'has_side_effects') = 'yes'
    THEN true
    ELSE false
  END AS has_side_effects,
  (couchdb.doc->'fields'->>'patient_sex') AS patient_sex,
  (couchdb.doc->'fields'->>'patient_date_of_birth') AS patient_date_of_birth,
  (couchdb.doc->'fields'->>'current_fp_method') AS current_fp_method,
  (couchdb.doc->'fields'->>'coalesced_fp_method') AS coalesced_fp_method,
  (couchdb.doc->'fields'->>'current_fp_method_display') AS current_fp_method_display
{% endset %}
{{ cht_form_model('family_planning', custom_fields, age_indexes) }}
