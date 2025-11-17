{{ config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'sync_all_columns',
    indexes = [
        {'columns': ['uuid'], 'type': 'hash'},
        {'columns': ['turning_one_period_id', 'chp_area_id']},
        {'columns': ['turns_one_on']},
        {'columns': ['household_id']},
        {'columns': ['chp_area_id']},
        {'columns': ['muted']}
    ],
    tags = ['dim','sex','cadence_hourly']
) }}

-- Limit incremental loads
with max_saved as (
    {% if is_incremental() %}
        select coalesce(max(saved_timestamp), '2020-01-01'::timestamp) as max_ts
        from {{ this }}
    {% else %}
        select '2020-01-01'::timestamp as max_ts
    {% endif %}
),

source as (
    select
        c.uuid,
        c.sex,
        c.date_of_birth::date as date_of_birth,
        hh.uuid as household_id,
        hh.chv_area_id as chp_area_id,
        c.muted,
        c.saved_timestamp,
        (c.date_of_birth::date + interval '1 year')::date as turns_one_on
    from {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} c
    join {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} hh
      on c.household_id = hh.uuid
    join max_saved ms on true
    where c.uuid is not null
      and c.sex is not null
      {% if is_incremental() %}
      and c.saved_timestamp > ms.max_ts
      {% endif %}
),

-- Map the turns_one_on date to dim_period
enriched as (
    select
        s.*,
        dp.period_id as turning_one_period_id
    from source s
    left join {{ ref('dim_period') }} dp
      on s.turns_one_on between dp.start_date and dp.end_date
)

select *
from enriched
