{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [
  {'columns': ['dead']},
  {'columns': ['source_id']},
  {'columns': ['date_of_death']},
  {'columns': ['confirmed_date_of_death']}]
-%}
{% set custom_fields %}
  {{ patient_age_columns() }},
  data_record.patient_id,
  couchdb.doc->'fields'->'group_review'->>'dead' AS dead,
  couchdb.doc->'fields'->'inputs'->>'source_id' AS source_id,
  couchdb.doc->'fields'->>'death_type' AS death_type,
  CASE WHEN (couchdb.doc->'fields'->'group_review'->>'date_of_death') IS NULL
            AND (couchdb.doc->'fields'->>'date_of_death') IS NULL 
        THEN NULL
        ELSE COALESCE(
              NULLIF(couchdb.doc->'fields'->'group_review'->>'date_of_death',''), 
              NULLIF(couchdb.doc->'fields'->>'date_of_death',''))::DATE
  END AS date_of_death,
  couchdb.doc->'fields'->'group_review'->>'place_of_death' AS place_of_death,
  (couchdb.doc->'fields'->'group_review'->'group_confirmation'->'fields'->>'date_of_death')::DATE AS confirmed_date_of_death,
  couchdb.doc->'fields'->'group_review'->'group_confirmation'->'fields'->>'needs_signoff' AS needs_signoff,
  couchdb.doc->'fields'->'group_review'->>'probable_cause_of_death' AS probable_cause_of_death,
  couchdb.doc->'fields'->'group_review'->>'specify_other_cause_of_death' AS specify_other_cause_of_death
{% endset %}
{{ cht_form_model('death_review', custom_fields, age_indexes + form_indexes) }}