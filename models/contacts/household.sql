{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['chv_area_id']}
    ]
  )
}}

SELECT
  contact.uuid AS uuid,
  contact.saved_timestamp,
  contact.name AS household_name,
  contact.reported,
  couchdb.doc ->> '_id'::text AS household_contact_id,
  contact.parent_uuid AS chv_area_id,
  chu.uuid AS chu_area_id, 
  NULLIF(couchdb.doc ->> 'uses_treated_water'::text, ''::text)::boolean AS uses_treated_water,
  NULLIF(couchdb.doc ->> 'has_functional_latrine'::text, ''::text)::boolean AS has_functional_latrine

FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
LEFT JOIN {{ ref('contact') }} chu ON chu.uuid = contact.parent_uuid
WHERE contact.contact_type = 'e_household'
{% if is_incremental() %}
  AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
