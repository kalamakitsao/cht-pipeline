{% macro moh_515_form_columns() %}
  couchdb.doc->'fields'->>'county_uuid' AS county_uuid,
  couchdb.doc->'fields'->>'county_name' AS county_name,
  couchdb.doc->'fields'->>'subcounty_uuid' AS subcounty_uuid,
  couchdb.doc->'fields'->>'branch_name' AS subcounty_name,
  couchdb.doc->'fields'->>'sub_county_supervisor_id' AS sub_county_supervisor_id,
  couchdb.doc->'fields'->>'chu_uuid' AS community_unit_uuid,
  couchdb.doc->'fields'->>'community_unit' AS community_unit,
  couchdb.doc->'fields'->>'cha_id' AS cha_uuid,
  couchdb.doc->'fields'->>'facility' AS facility,
  COALESCE(couchdb.doc->'fields'->>'input_mculcode', couchdb.doc->'fields'->>'mculcode') AS mculcode,
  couchdb.doc->'fields'->>'month' AS month,
  couchdb.doc->'fields'->>'year' AS year
{% endmacro %}
