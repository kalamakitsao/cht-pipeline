{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [
      {'columns': ['is_referred_immunization']},
      {'columns': ['is_referred_vitamin_a']},
      {'columns': ['is_referred_growth_monitoring']}]
-%}
{% set custom_fields %}
  data_record.patient_id as patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent,

  {{ patient_age_columns() }},
  NULLIF(couchdb.doc->'fields'->'group_summary'->>'r_child_dewormed', '')::boolean AS is_dewormed,
  (couchdb.doc->'fields'->'group_vitamin_a'->>'vitamin_a_doses_given') = ANY (ARRAY['none', 'no']) AS is_referred_vitamin_a,
  NULLIF(couchdb.doc->'fields'->>'needs_immunization_follow_up', '')::boolean AS is_referred_immunization,
  NULLIF(couchdb.doc->'fields'->>'needs_growth_monitoring_follow_up', '')::boolean AS is_referred_growth_monitoring,
  NULLIF(couchdb.doc->'fields'->>'needs_follow_up', '')::boolean AS needs_follow_up
{% endset %}

{{ cht_form_model('immunization_service', custom_fields, age_indexes + form_indexes) }}
