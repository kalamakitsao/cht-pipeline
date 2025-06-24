{%- set age_indexes = patient_age_indexes() -%}
{% set custom_fields %}
  data_record.patient_id,
  data_record.grandparent_uuid as reported_by_parent_parent,
  {{ patient_age_columns() }},
  NULLIF(couchdb.doc->'fields'->>'has_been_referred', '')::boolean AS has_been_referred,
  NULLIF(couchdb.doc->'fields'->>'marked_as_pregnant', '')::boolean AS is_new_pregnancy,
  NULLIF(couchdb.doc->'fields'->>'currently_pregnant', '')::boolean AS is_currently_pregnant,
  NULLIF(couchdb.doc->'fields'->'anc_visits'->>'has_started_anc', '')::boolean AS is_counselled_anc,
  NULLIF(couchdb.doc->'fields'->'anc_visits'->>'has_started_anc', '')::boolean AS has_started_anc,
  NULLIF(couchdb.doc->'fields'->'anc_visits'->>'anc_upto_date', '')::boolean AS is_anc_upto_date,
  NULLIF(couchdb.doc->'fields'->'anc_visits'->>'next_anc_visit_date', '')::date AS next_anc_visit_date,
  NULLIF(couchdb.doc->'fields'->'anc_visits'->>'updated_edd', '')::date AS updated_edd,
  NULLIF(couchdb.doc->'fields'->>'current_edd', '')::date AS current_edd
{% endset %}

{{ cht_form_model('pregnancy_home_visit', custom_fields, age_indexes) }}
