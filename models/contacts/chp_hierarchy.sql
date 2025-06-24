{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['saved_timestamp']},
      {'columns': ['county_name']},
      {'columns': ['chu_name']},
      {'columns': ['chu_code']},
      {'columns': ['chp_area_uuid']},
    ]
  )
}}

SELECT
  chp_area.uuid,
  chp_area.saved_timestamp,
  county.name AS county_name,
  sub_county.name AS sub_county_name,
  chu.name AS chu_name,
  ecu.chu_code,
  ecu.uuid as chu_uuid,
  chp_area.uuid AS chp_area_uuid,
  chp_area.name AS chp_area_name,
  coalesce(couchdb.doc #>> '{contact,_id}'::text[], couchdb.doc ->> 'contact'::text) AS chp_id,
  person.phone as phone,
  chps.muted as chp_muted
FROM
  {{ref('contact')}} chp_area
  INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = chp_area.uuid
  JOIN {{ref('contact')}} chu ON chp_area.parent_uuid = chu.uuid
  JOIN {{ref('echis_community_units')}} ecu ON chu.uuid = ecu.uuid
  JOIN {{ref('contact')}} sub_county ON chu.parent_uuid = sub_county.uuid
  JOIN {{ref('contact')}} county ON sub_county.parent_uuid = county.uuid
  LEFT JOIN {{ref('person')}} person 
    ON coalesce(couchdb.doc #>> '{contact,_id}'::text[], couchdb.doc ->> 'contact'::text) = person.uuid
  LEFT JOIN {{ref('contact')}} chps 
    ON coalesce(couchdb.doc #>> '{contact,_id}'::text[], couchdb.doc ->> 'contact'::text) = chps.uuid
  WHERE chp_area.contact_type = 'd_community_health_volunteer_area'
