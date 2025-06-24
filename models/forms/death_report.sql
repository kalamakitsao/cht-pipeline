{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [{'columns': ['death_type']}] -%}
{% set custom_fields %}
  {{ patient_age_columns() }},
  (couchdb.doc->'fields'->'group_death'->>'date_of_death')::DATE AS date_of_death,
  couchdb.doc->'fields'->'group_death'->>'place_of_death' AS place_of_death,
  couchdb.doc->'fields'->>'death_type' AS death_type,
  data_record.patient_id,
  data_record.grandparent_uuid AS reported_by_parent_parent
{% endset %}
{{ cht_form_model('death_report', custom_fields, age_indexes + form_indexes) }}
