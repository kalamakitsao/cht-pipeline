
{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=[
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['reported']},
    ]
  )
}}

select
   person.uuid,
   person.saved_timestamp as saved_timestamp,
   contact.reported as reported,
   doc -> 'user_attribution' ->> 'tool' as version
from {{ ref('person')}} person
inner join {{ ref('contact')}} contact on contact.uuid = person.uuid
inner join {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb on couchdb._id = person.uuid
where couchdb.doc -> 'user_attribution' ->> 'tool' is not null

{% if is_incremental() %}
  AND person.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
{% endif %}