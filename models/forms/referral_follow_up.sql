{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['patient_age_in_years']},
      {'columns': ['patient_age_in_months']},
      {'columns': ['fp_uuid']},
      {'columns': ['anc_uuid']},
      {'columns': ['defaulter_uuid']},
      {'columns': ['under_five_uuid']},
      {'columns': ['over_five_uuid']}
    ]
  )
}}

SELECT
  {{ data_record_columns() }},
  {{ patient_age_columns() }},
  data_record.patient_id,
  data_record.grandparent_uuid as reported_by_parent_parent,
  couchdb.doc->'fields'->'inputs'->>'source_id' as source_id,
  fp.uuid as fp_uuid,
  anc.uuid as anc_uuid,
  defaulter.uuid as defaulter_uuid,
  under_five.uuid as under_five_uuid,
  over_five.uuid as over_five_uuid,
  COALESCE(under_five.has_fever, over_five.has_fever, NULL::boolean) AS has_fever,
  fp.is_referred_for_fp_services AS was_referred_for_fp_services,
  anc.has_been_referred AS was_referred_for_anc_services,
  pnc.is_referred_for_pnc_services AS was_referred_for_pnc_services,
  defaulter.imm_defaulted AS was_defaulter_immunization,
  defaulter.art_defaulted AS was_defaulter_art,
  defaulter.hei_defaulted AS was_defaulter_hei,
  defaulter.tb_defaulted AS was_defaulter_tb,
  over_five.is_referred_tb AS was_referred_tb_case,
  under_five.needs_tb_referral AS was_referred_tb_contact,
  NULLIF(couchdb.doc->'fields'->>'is_available_and_completed_visit', '')::boolean AS is_available_and_completed_visit

FROM  {{ ref('data_record') }} data_record
  INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = data_record.uuid
  LEFT JOIN {{ ref('family_planning') }} fp ON couchdb.doc->'fields'->'inputs'->>'source_id' = fp.uuid
  LEFT JOIN {{ ref('pregnancy_home_visit') }} anc ON couchdb.doc->'fields'->'inputs'->>'source_id' = anc.uuid
  LEFT JOIN {{ ref('postnatal_care_service_newborn') }} pnc ON couchdb.doc->'fields'->'inputs'->>'source_id' = pnc.uuid
  LEFT JOIN {{ ref('defaulter_follow_up') }} defaulter ON couchdb.doc->'fields'->'inputs'->>'source_id' = defaulter.uuid
  LEFT JOIN {{ ref('u5_assessment') }} under_five ON couchdb.doc->'fields'->'inputs'->>'source_id' = under_five.uuid
  LEFT JOIN {{ ref('over_five_assessment') }} over_five ON couchdb.doc->'fields'->'inputs'->>'source_id' = over_five.uuid
WHERE
  data_record.form = 'referral_follow_up'

{% if is_incremental() %}
  AND data_record.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
