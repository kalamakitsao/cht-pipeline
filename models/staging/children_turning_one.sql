{{ config(
    materialized = 'table',
    indexes = [
        {'columns': ['patient_id', 'period_id'], 'unique': true},
        {'columns': ['chp_area_id', 'period_id']},
        {'columns': ['turns_one_on']},
        {'columns': ['sex', 'period_id']},
        {'columns': ['patient_id']}
    ],
    tags = ['kpi','immunization','cadence_hourly']
) }}

with eligible_people as (
    select
        uuid as patient_id,
        chp_area_id,
        sex,
        turns_one_on
    from {{ ref('household_person_enriched') }}
    where turns_one_on is not null
      and muted is distinct from true
),

eligible_periods as (
    select
        period_id,
        period_id_name,
        start_date,
        end_date
    from {{ ref('dim_period') }}
    where period_id_name not in ('today')
)

select
    p.patient_id,
    p.chp_area_id,
    p.sex,
    p.turns_one_on,
    dp.period_id
from eligible_people p
join eligible_periods dp
  on p.turns_one_on between dp.start_date and dp.end_date
