{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['reported']},
      {'columns': ['parent_uuid']},
    ]
  )
}}

SELECT
  _id as uuid,
  patient.saved_timestamp,
  to_timestamp((NULLIF(doc ->> 'reported_date' :: text, '' :: text) :: bigint / 1000) :: double precision) AS reported,
  doc ->'meta'->> 'created_by_person_uuid' AS reported_by,
  doc -> 'parent' ->> '_id' AS parent_uuid,
  doc ->> 'age_in_years' AS age_in_years,
  doc ->> 'age_in_months' AS age_in_months,
  doc ->> 'muac_colour' as muac_colour,
  doc ->> 'n_refer_muac' as n_refer_muac,
  doc ->> 'imm_upto_date' AS imm_upto_date,
  doc ->> 'has_chronic_illness' AS has_chronic_illness,
  doc->'meta' ->>'created_by_place_uuid' AS reported_by_parent,
  doc #>> '{parent,parent,parent,_id}' AS grandparent_uuid
FROM
   {{ ref('patient_f_client') }} patient
   inner join {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON patient.uuid = couchdb._id
where patient.date_of_birth is not null and (NULLIF((doc ->'meta' ->>'created_by_place_uuid' :: text),'' :: text),'NaN' :: text) is not null

{% if is_incremental() %}
  AND patient.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
