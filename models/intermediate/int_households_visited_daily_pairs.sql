{{ config(
    materialized='incremental',
    unique_key=['location_id', 'household_id', 'reported_date'],
    incremental_strategy='delete+insert',
    on_schema_change='ignore',
    indexes=[
      {'columns': ['location_id', 'household_id', 'reported_date'], 'unique': true},
      {'columns': ['reported_date']},
      {'columns': ['location_id', 'reported_date']},
      {'columns': ['household_id']},
      {'columns': ['last_seen_saved_timestamp']}
    ],
    tags=['kpi','households_visited','cadence_twice_daily']
) }}

with watermark as (

    {% if is_incremental() %}
    select coalesce(max(last_seen_saved_timestamp), '2020-01-01'::timestamp) as max_ts
    from {{ this }}
    {% else %}
    select '2020-01-01'::timestamp as max_ts
    {% endif %}

),

valid_households as (

    select
        c.uuid as household_id
    from {{ source(env_var('POSTGRES_SCHEMA'), 'contact') }} c
    where c.uuid is not null
      and c.contact_type = 'e_household'
      and c.muted is null

),

source_rows as (

    select
        hv.reported_by_parent as location_id,
        hv.household as household_id,
        hv.reported::date as reported_date,
        max(hv.saved_timestamp) as last_seen_saved_timestamp
    from {{ source(env_var('POSTGRES_SCHEMA'), 'household_visit') }} hv
    join valid_households vh
      on hv.household = vh.household_id
    join {{ ref('mv_location_hierarchy') }} chps
      on hv.reported_by_parent = chps.chp_area_id
    join watermark w
      on true
    where hv.household is not null
      and hv.reported_by_parent is not null
      and hv.reported is not null
      {% if is_incremental() %}
      and hv.saved_timestamp > w.max_ts
      {% endif %}
    group by
        hv.reported_by_parent,
        hv.household,
        hv.reported::date

)

select
    location_id,
    household_id,
    reported_date,
    current_timestamp as last_updated,
    last_seen_saved_timestamp
from source_rows
