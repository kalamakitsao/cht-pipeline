-- This is data extracted from the contact forms for clients under 5. 

{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['patient_age_in_months']}
    ]
  )
}}

SELECT 
  patient.uuid as uuid,
  patient.uuid as patient_id,
  patient.saved_timestamp,
  patient.reported, 
  NULLIF(couchdb.doc->>'age_in_years', ''::text)::integer AS patient_age_in_years,
  NULLIF(couchdb.doc->>'age_in_months', ''::text)::integer AS patient_age_in_months,
  couchdb.doc->>'muac_color' AS muac_color,
  NULL::boolean AS has_fast_breathing,
  NULL::boolean AS has_chest_indrawing,
  NULL::boolean AS gave_amox,
  NULL::boolean AS has_diarrhoea,
  NULL::boolean AS gave_zinc,
  NULL::boolean AS gave_ors,
  NULL::boolean AS has_fever,
  NULL::integer AS fever_duration,
  NULL::text AS rdt_result,
  NULL::text AS repeat_rdt_result,
  NULL::boolean AS gave_al,
  NULL::boolean AS needs_tb_referral,
  NULL::boolean AS has_cough,
  NULL::boolean AS is_exclusively_breastfeeding,
  NULL::boolean AS has_uptodate_growth_monitoring,
  NULL::boolean AS referred_for_development_milestones,
  couchdb.doc->'meta'->>'created_by_person_uuid' AS reported_by,
  couchdb.doc->'meta'->>'created_by_place_uuid' AS reported_by_parent,
  couchdb.doc->'parent'->'parent'->'parent'->>'_id' AS reported_by_parent_parent 

FROM 
  {{ ref('patient_f_client') }} patient
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = patient.uuid
WHERE
  NULLIF(couchdb.doc->>'age_in_months', ''::text)::integer <= 60 

{% if is_incremental() %}
  AND patient.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
