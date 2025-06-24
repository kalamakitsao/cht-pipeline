
-- This is data for moh-515 CUs. 
-- It is similar to the echis_community_units model, but  more comprehensive.
-- It contains the following extra information
-- 1. county_focal_uuid,
-- 2. county_focal_name
-- 3. sub_county_focal_uuid
-- 4. sub_county_focal_name
-- 5. mchul_code
-- 6. link_facility_code,
-- 7. link_facility_name

{{
  config(
    materialized = 'incremental',
    unique_key = ['cu_uuid'],
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['county_uuid']},
      {'columns': ['county_focal_uuid']},
      {'columns': ['sub_county_uuid']},
      {'columns': ['sub_county_focal_uuid']},
      {'columns': ['cu_uuid']},
      {'columns': ['mchul_code']},
      {'columns': ['cha_uuid']}
    ]
  )
}}

WITH 
c AS (
  SELECT 
    cm.uuid as county_uuid, 
    cm.name as county_name,
    contact.uuid AS county_focal_uuid,
    contact.name AS county_focal_name
  FROM {{ ref("contact") }} cm
  INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = cm.uuid
  INNER JOIN {{ ref("contact") }} contact ON contact.uuid = (couchdb.doc #>> '{contact,_id}'::TEXT[])
  WHERE cm.contact_type = 'a_county'
),
sc AS (
  SELECT 
    c.*, 
    cm.uuid as sub_county_uuid, 
    cm.name as sub_county_name, 
    contact.uuid as sub_county_focal_uuid,
    contact.name as sub_county_focal_name
  FROM {{ ref("contact") }} cm
  INNER JOIN c on c.county_uuid = cm.parent_uuid
  INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = cm.uuid
  INNER JOIN {{ ref("contact") }} contact ON contact.uuid = (couchdb.doc #>> '{contact,_id}'::TEXT[])
  WHERE cm.contact_type = 'b_sub_county'
)
  SELECT 
    sc.*, 
    cm.uuid as cu_uuid, 
    cm.name as cu_name, 
    couchdb.doc ->> 'code'::TEXT AS mchul_code,
    couchdb.doc ->> 'link_facility_code'::TEXT AS link_facility_code,
    couchdb.doc ->> 'link_facility_name'::TEXT AS link_facility_name,
    contact.uuid as cha_uuid,
    contact.name as cha_name
  FROM {{ ref("contact") }} cm
  INNER JOIN sc on sc.sub_county_uuid = cm.parent_uuid
  INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = cm.uuid
  INNER JOIN {{ ref("contact") }} contact ON contact.uuid = (couchdb.doc #>> '{contact,_id}'::TEXT[])
  WHERE cm.contact_type = 'c_community_health_unit'
