{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
    ]
  )
}}

SELECT
  contact.uuid as uuid,
  contact.saved_timestamp as saved_timestamp,
  contact.name as name,
  contact.reported as reported,
  contact.parent_uuid AS household_id,
  couchdb.doc->>'sex' as sex,

  CAST(
    (
      {{ validate_date("COALESCE((couchdb.doc->>'dob_iso')::text, (couchdb.doc->>'date_of_birth')::text)") }}
    ) AS DATE
  ) AS date_of_birth,
  NULLIF(couchdb.doc ->> 'muted'::text, ''::text) AS muted
FROM {{ ref("contact") }} contact
INNER JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb ON couchdb._id = uuid
WHERE contact.contact_type = 'f_client'

{% if is_incremental() %}
  AND contact.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
