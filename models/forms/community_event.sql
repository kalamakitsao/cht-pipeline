{%- set form_indexes = [
  {'columns': ['event_types']},
  {'columns': ['event_date']}]
-%}
{% set custom_fields %}
  couchdb.doc->'fields'->>'place_id' AS place_id,
  couchdb.doc->'fields'->'event_information'->>'event_types' AS event_types,
  couchdb.doc->'fields'->'event_information'->>'other_event' AS event_types_other,
  (couchdb.doc->'fields'->'event_information'->>'event_date')::DATE AS event_date
{% endset %}

{{ cht_form_model('community_event', custom_fields, form_indexes) }}