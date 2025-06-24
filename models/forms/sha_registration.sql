{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['reported']},
      {'columns': ['reported_by_parent']},
      {'columns': ['member_uuid']},
      {'columns': ['has_sha_registration']}, 
      {'columns': ['cert_seen_by_chp']}  
    ]
  )
}}

select 
  data_record.uuid as uuid,
  data_record.saved_timestamp,
  data_record.reported,
  data_record.parent_uuid as reported_by_parent,
  couchdb.doc->'fields'->>'hh_member_uuid' as member_uuid,
  couchdb.doc->'fields'->>'hh_member_name' as member_name,
  couchdb.doc->'fields'->>'hh_member_sex' as sex,
  couchdb.doc->'fields'->>'household_name' as household_name,
  case when 
    (couchdb.doc->'fields'->'group_insurance'->>'has_sha_registration' = 'yes'::text) then true 
    else false 
    end as has_sha_registration,
  couchdb.doc->'fields'->'group_insurance'->>'chp_seen_sha_certificate' as cert_seen_by_chp,
  couchdb.doc->'fields'->'group_insurance'->>'hh_member_sha_reg_willing' as member_willing_to_register,
  couchdb.doc->'fields'->'group_insurance'->>'date_when_hh_member_wants_sha_reg' as date_when_want_to_register
from {{ ref('data_record') }} data_record
inner join {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb on couchdb._id = data_record.uuid
where data_record.form = 'sha_insurance'

{% if is_incremental() %}
  and data_record.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}