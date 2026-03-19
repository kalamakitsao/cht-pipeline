{{ config(
    materialized = "incremental",
    unique_key = "uuid",
    incremental_strategy = "delete+insert",
    on_schema_change = "ignore",
    indexes = [
        {'columns': ['uuid'], 'unique': true},
        {'columns': ['patient_id']},
        {'columns': ['report_date']},
        {'columns': ['chp_area_id']},
        {'columns': ['location_id']},
        {'columns': ['saved_timestamp']}
    ],
    tags = ['cadence_hourly']
) }}

with watermark as (

    {% if is_incremental() %}
    select coalesce(max(saved_timestamp), '2020-01-01'::timestamp) as max_ts
    from {{ this }}
    {% else %}
    select '2020-01-01'::timestamp as max_ts
    {% endif %}

),

immunization_base as (
    select
        i.uuid,
        i.saved_timestamp,
        i.reported_by_parent as location_id,
        i.reported::date as report_date,
        i.patient_id,
        i.patient_age_in_months,
        i.has_measles_9,
        nullif(i.imm_schedule_upto_date, '')::boolean as imm_schedule_upto_date,
        nullif(i.needs_immunization_referral, '')::boolean as needs_immunization_referral,
        nullif(i.needs_deworming_follow_up, '')::boolean as needs_deworming_follow_up,
        nullif(i.needs_growth_monitoring_referral, '')::boolean as needs_growth_monitoring_referral
    from {{ source(env_var('POSTGRES_SCHEMA'), 'immunization_status') }} i
    join watermark w on true
    where i.uuid is not null
      and i.patient_id is not null
      and i.reported is not null
      {% if is_incremental() %}
      and i.saved_timestamp > w.max_ts
      {% endif %}
),

children_base as (
    select
        uuid as patient_id,
        sex,
        chp_area_id
    from {{ ref('household_person_enriched') }}
    where muted is distinct from true
      and turns_one_on is not null
)

select
    i.uuid,
    i.saved_timestamp,
    i.location_id,
    i.report_date,
    i.patient_id,
    c.sex,
    i.patient_age_in_months,
    i.has_measles_9,
    i.imm_schedule_upto_date,
    i.needs_immunization_referral,
    i.needs_deworming_follow_up,
    i.needs_growth_monitoring_referral,
    c.chp_area_id
from immunization_base i
join children_base c
  on i.patient_id = c.patient_id
