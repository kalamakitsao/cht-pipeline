{{ config(
    materialized='incremental',
    unique_key=['patient_id','period_id'],
    tags=['cadence_hourly']
) }}

with src as (
    select
        uuid as patient_id,
        chp_area_id,
        sex,
        turning_one_period_id as period_id,
        saved_timestamp
    from {{ ref('household_person_enriched') }}
    where muted is null
      and turning_one_period_id is not null
      {% if is_incremental() %}
      and saved_timestamp > (
          select coalesce(max(saved_timestamp),'2020-01-01'::timestamp)
          from {{ this }}
      )
      {% endif %}
)

select * from src
