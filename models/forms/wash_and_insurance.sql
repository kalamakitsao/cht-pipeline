{%- set form_indexes = [{'columns': ['has_functional_latrine']}] -%}
{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=data_record_indexes() + form_indexes
  )
}}

SELECT
  {{ data_record_columns() }},
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
{{ data_record_join('wash') }}

UNION ALL

SELECT
  contact.uuid as uuid,
  contact.saved_timestamp,
  contact.uuid as reported_by,
	contact.parent_uuid as reported_by_parent,
  contact.reported,
  contact.uuid as place_id,
  couchdb.doc #>> '{parent,parent,_id}'::text[] AS reported_by_parent_parent,
  NULLIF(couchdb.doc->>'has_functional_latrine', '')::boolean AS has_functional_latrine,
  NULLIF(couchdb.doc->>'has_functional_handwash_facility', '')::boolean AS has_functional_handwashing_facility,
  NULLIF(couchdb.doc->>'uses_safe_water', '')::boolean AS has_access_to_safe_water,
  NULLIF(couchdb.doc->>'has_functional_refuse_disposal_site', '')::boolean AS has_functional_refuse_disposal_facility,
  NULLIF(couchdb.doc->>'has_insurance_cover', '')::boolean AS has_upto_date_insurance_cover,
  couchdb.doc->>'insurance' AS specific_insurance_cover,
  couchdb.doc->>'main_drinking_water_source' AS main_drinking_water_source,
  true AS is_new_visit
FROM {{ ref('contact') }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = contact.uuid
WHERE
  contact.contact_type = 'e_household'
{% if is_incremental() %}
  AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
