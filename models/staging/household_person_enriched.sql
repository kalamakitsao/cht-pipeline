{{ config(
    materialized = 'incremental',
    unique_key = 'uuid',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns',
    indexes = [
        {'columns': ['uuid'], 'unique': true},
        {'columns': ['household_id']},
        {'columns': ['chp_area_id']},
        {'columns': ['turns_one_on']},
        {'columns': ['sex', 'turns_one_on']},
        {'columns': ['chp_area_id', 'turns_one_on']},
        {'columns': ['muted']},
        {'columns': ['saved_timestamp']}
    ],
    tags = ['dim','sex','cadence_hourly']
) }}

with max_saved as (

    {% if is_incremental() %}
        select coalesce(max(saved_timestamp), '2020-01-01'::timestamp) as max_ts
        from {{ this }}
    {% else %}
        select '2020-01-01'::timestamp as max_ts
    {% endif %}

),

changed_patients as (

    select
        c.uuid::text as uuid,
        c.sex::text as sex,
        c.date_of_birth::date as date_of_birth,
        c.household_id::text as household_id,
        (c.muted is not null) as muted,
        c.saved_timestamp::timestamp as saved_timestamp,
        (c.date_of_birth::date + interval '1 year')::date as turns_one_on
    from {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} c
    join max_saved ms on true
    where c.uuid is not null
      and c.sex is not null
      and c.date_of_birth is not null
      {% if is_incremental() %}
      and c.saved_timestamp > ms.max_ts
      {% endif %}

),

changed_households as (

    {% if is_incremental() %}
    select
        c.uuid::text as uuid,
        c.sex::text as sex,
        c.date_of_birth::date as date_of_birth,
        c.household_id::text as household_id,
        (c.muted is not null) as muted,
        greatest(c.saved_timestamp, hh.saved_timestamp)::timestamp as saved_timestamp,
        (c.date_of_birth::date + interval '1 year')::date as turns_one_on
    from {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} c
    join {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} hh
      on c.household_id = hh.uuid
    join max_saved ms on true
    where c.uuid is not null
      and c.sex is not null
      and c.date_of_birth is not null
      and hh.saved_timestamp > ms.max_ts
    {% else %}
    select
        null::text as uuid,
        null::text as sex,
        null::date as date_of_birth,
        null::text as household_id,
        null::boolean as muted,
        null::timestamp as saved_timestamp,
        null::date as turns_one_on
    where false
    {% endif %}

),

source_rows as (

    select
        uuid,
        sex,
        date_of_birth,
        household_id,
        muted,
        saved_timestamp,
        turns_one_on
    from changed_patients

    union all

    select
        uuid,
        sex,
        date_of_birth,
        household_id,
        muted,
        saved_timestamp,
        turns_one_on
    from changed_households

),

dedup_source as (

    select distinct on (uuid)
        uuid,
        sex,
        date_of_birth,
        household_id,
        muted,
        saved_timestamp,
        turns_one_on
    from source_rows
    order by uuid, saved_timestamp desc

)

select
    s.uuid,
    s.sex,
    s.date_of_birth,
    hh.uuid as household_id,
    hh.chv_area_id as chp_area_id,
    s.muted,
    s.saved_timestamp,
    s.turns_one_on
from dedup_source s
join {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} hh
  on s.household_id = hh.uuid
