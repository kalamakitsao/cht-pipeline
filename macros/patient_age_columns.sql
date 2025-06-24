{% macro patient_age_columns() %}
  NULLIF(NULLIF(couchdb.doc->'fields'->>'patient_age_in_years', ''), 'NaN')::integer AS patient_age_in_years,
  NULLIF(NULLIF(couchdb.doc->'fields'->>'patient_age_in_months', ''), 'NaN')::integer AS patient_age_in_months,
  NULLIF(NULLIF(couchdb.doc->'fields'->>'patient_age_in_days', ''), 'NaN')::integer AS patient_age_in_days
{% endmacro %}

{% macro patient_age_indexes() %}
  {{ return([
    {'columns': ['patient_age_in_years']},
    {'columns': ['patient_age_in_months']},
    {'columns': ['patient_age_in_days']}
  ])}}
{% endmacro %}
