{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=data_record_indexes() + [
      {'columns': ['form']},
      {'columns': ['household']},
      {'columns': ['chw']},
    ]
  )
}}

-- forms may have either place_id or patient_id
-- to get household id, either get the parent of patient
-- or get the household directly from place_id
-- this assumes patient.parent_uuid is also a household
SELECT
  {{ data_record_columns() }},
  data_record.form as form,
  household.uuid as household,
  data_record.contact_uuid as chw,
  data_record.grandparent_uuid as reported_by_parent_parent
FROM {{ ref('data_record') }} data_record
LEFT JOIN {{ ref('patient_f_client') }} patient ON data_record.patient_id = patient.uuid
INNER JOIN {{ ref('household') }}  household ON
  (data_record.place_id = household.uuid OR
  patient.household_id = household.uuid)

{% if is_incremental() %}
  WHERE data_record.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
