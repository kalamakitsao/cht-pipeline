{%- set age_indexes = patient_age_indexes() -%}
{% set custom_fields %}
  data_record.patient_id AS patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent,
  {{ patient_age_columns() }},
  (couchdb.doc->'fields'->>'anc_defaulted')::boolean AS anc_defaulted,
  (couchdb.doc->'fields'->>'imm_defaulted')::boolean AS imm_defaulted,
  (couchdb.doc->'fields'->>'tb_defaulted')::boolean AS tb_defaulted,
  (couchdb.doc->'fields'->>'hei_defaulted')::boolean AS hei_defaulted,
  (couchdb.doc->'fields'->>'art_defaulted')::boolean AS art_defaulted,
  (couchdb.doc->'fields'->>'pnc_defaulted')::boolean AS pnc_defaulted,
  (couchdb.doc->'fields'->>'growth_monitoring_defaulted')::boolean AS growth_monitoring_defaulted,
  (couchdb.doc->'fields'->>'vit_a_and_deworming_defaulted')::boolean AS vit_a_and_deworming_defaulted,
  (couchdb.doc->'fields'->'group_follow_up'->>'referred_to_hf')::boolean AS is_referred,
  NULLIF(couchdb.doc->'fields'->>'needs_follow_up', '')::boolean AS needs_follow_up,
  (couchdb.doc->'fields'->'group_follow_up'->>'follow_up_date')::DATE AS follow_up_date,
  (couchdb.doc->'fields'->'group_follow_up'->>'client_available') AS client_available
{% endset %}
{{ cht_form_model('defaulter_follow_up', custom_fields, age_indexes) }}
