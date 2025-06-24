{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['date_of_birth']},
      {'columns': ['muted']},
      {'columns': ['chp_area_id']},
    ]
  )
}}

SELECT
  person.uuid,
  patient.saved_timestamp,
  person.name,
  person.reported,
  patient.sex,
  patient.date_of_birth,
  chp_area.parent_uuid AS chp_id,
  household.parent_uuid AS chp_area_id,
  chp_area.parent_uuid AS chu_id,
  person.muted,
  household.uuid AS household_id,
  household.reported AS household_reported_date
  FROM {{ ref('patient_f_client')}} patient
    JOIN {{ ref('contact')}} person ON patient.uuid = person.uuid
    JOIN {{ ref('contact')}} household ON household.uuid = person.parent_uuid
    JOIN {{ ref('contact')}} chp_area ON chp_area.uuid = household.parent_uuid
WHERE household.contact_type = ANY (ARRAY['e_household'::text, 'ext_e_household'::text])

{% if is_incremental() %}
  AND person.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
