{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['user_id']},
      {'columns': ['username']},
      {'columns': ['place']}
    ]
  )
}}

SELECT
  document_metadata.uuid as uuid,
  document_metadata.saved_timestamp,
  (couchdb.doc->>'_id') AS user_id,
  (couchdb.doc->>'name') AS name,
  (couchdb.doc->>'phone') AS phone,
  (couchdb.doc->>'place') AS place,
  (couchdb.doc->>'roles') AS roles,
  (couchdb.doc->>'username') AS username,
  (couchdb.doc->>'imported_date') AS imported_date
FROM
  {{ ref('document_metadata') }} document_metadata
INNER JOIN
  {{ source('couchdb', env_var('POSTGRES_TABLE')) }} couchdb
  ON couchdb._id = document_metadata.uuid
WHERE
  document_metadata.doc_type IS NULL
  AND couchdb.doc->> 'username'::text<>''::text

{% if is_incremental() %}
  AND document_metadata.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}
