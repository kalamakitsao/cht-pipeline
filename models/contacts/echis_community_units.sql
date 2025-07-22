{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['chu_code']},
      {'columns': ['chu_name']},
    ]
  )
}}

SELECT
  contact.uuid,
  contact.saved_timestamp,
  couchdb.doc ->> 'code'::text AS chu_code,
  couchdb.doc ->> 'name'::text AS chu_name,
  contact.uuid AS contact_id,
  contact.name AS contact_name,
  couchdb.doc ->> 'phone'::text AS contact_phone,
  subcounty.uuid AS subcounty_id,
  subcounty.name AS subcounty_name,
  county.uuid AS county_id,
  county.name AS county_name
FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
LEFT JOIN {{ ref('contact') }} subcounty ON subcounty.uuid = contact.parent_uuid
LEFT JOIN {{ ref('contact') }} county ON county.uuid = subcounty.parent_uuid
WHERE contact.contact_type = 'c_community_health_unit'
UNION
SELECT
  contact.uuid,
  contact.saved_timestamp,
  couchdb.doc ->> 'chu_code'::text AS chu_code,
  couchdb.doc ->> 'chu_name'::text AS chu_name,
  couchdb.doc ->> 'contact_id'::text AS contact_id,
  couchdb.doc ->> 'contact_name'::text AS contact_name,
  couchdb.doc ->> 'contact_phone'::text AS contact_phone,
  couchdb.doc ->> 'subcounty_id'::text AS subcounty_id,
  couchdb.doc ->> 'subcounty_name'::text AS subcounty_name,
  couchdb.doc ->> 'county_id'::text AS county_id,
  couchdb.doc ->> 'county_name'::text AS county_name
FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = contact.uuid
WHERE contact.contact_type = 'ext_c_community_health_unit'
