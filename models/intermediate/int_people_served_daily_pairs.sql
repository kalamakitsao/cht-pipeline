-- models/intermediate/int_people_served_daily_pairs.sql
{{ config(
    materialized='incremental',
    unique_key=['location_id', 'patient_id', 'reported_date'],
    incremental_strategy='delete+insert',
    on_schema_change='ignore',
    indexes=[
      {'columns': ['location_id', 'reported_date']},
      {'columns': ['reported_date']},
      {'columns': ['location_id', 'patient_id', 'reported_date'], 'unique': true},
      {'columns': ['last_seen_saved_timestamp']}
    ],
    tags=['people_served','cadence_twice_daily']
) }}

with source_rows as (

    select
        dr.parent_uuid as location_id,
        dr.patient_id,
        dr.reported::date as reported_date,
        max(dr.saved_timestamp) as last_seen_saved_timestamp
    from {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
    where dr.patient_id is not null
      and dr.parent_uuid is not null
      and dr.reported is not null
      {% if is_incremental() %}
      and dr.saved_timestamp > (
          select coalesce(max(last_seen_saved_timestamp), '1900-01-01'::timestamp)
          from {{ this }}
      )
      {% endif %}
    group by 1,2,3

)

select
    location_id,
    patient_id,
    reported_date,
    current_timestamp as last_updated,
    last_seen_saved_timestamp
from source_rows
