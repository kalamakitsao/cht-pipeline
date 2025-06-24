{%- set form_indexes = [{'columns': ['has_functional_latrine']}] -%}
{% set custom_fields %}
  data_record.place_id,
  data_record.grandparent_uuid as reported_by_parent_parent,
  NULLIF(couchdb.doc->'fields'->'group_wash'->>'has_functional_latrine', '')::boolean AS has_functional_latrine,
  NULLIF(couchdb.doc->'fields'->'group_wash'->>'has_functional_handwash_facility', '')::boolean AS has_functional_handwashing_facility,
  NULLIF(couchdb.doc->'fields'->'group_wash'->>'uses_safe_water', '')::boolean AS has_access_to_safe_water,
  NULLIF(couchdb.doc->'fields'->'group_wash'->>'has_functional_refuse_disposal_site', '')::boolean AS has_functional_refuse_disposal_facility,
  NULLIF(couchdb.doc->'fields'->'insurance'->>'has_upto_date_insurance', '')::boolean AS has_upto_date_insurance_cover,
  couchdb.doc->'fields'->'insurance'->>'insurance_cover' AS specific_insurance_cover,
  couchdb.doc->'fields'->'group_wash'->>'main_drinking_water_source' AS main_drinking_water_source,
  false AS is_new_visit
{% endset %}
{{ cht_form_model('wash', custom_fields, form_indexes) }}
