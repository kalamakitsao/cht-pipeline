{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','u5', 'cadence_hourly'],
  on_schema_change = 'ignore'
) }}

with eligible_periods as (
  select
    period_id,
    period_id_name,
    start_date,
    end_date + interval '1 day' as stop_date
  from {{ ref('dim_period') }}
),

bounds as (
  select
    min(start_date) as min_start_date,
    max(stop_date)  as max_stop_date
  from eligible_periods
),

base as (
  select
    e.reported_by_parent as location_id,
    e.reported_date,
    e.patient_id,
    e.sex,
    e.has_diarrhoea,
    e.has_fever,
    e.has_pneumonia,
    case
      when e.muac_color in ('red', 'yellow') then true
      when e.muac_color = 'green' then false
      else null
    end as has_malnutrition,
    e.has_malaria,
    e.referred_for_development_milestones,
    e.has_been_referred,
    e.rdt_result,
    coalesce(e.gave_amox, false) as gave_amox,
    coalesce(e.gave_zinc, false) as gave_zinc,
    coalesce(e.gave_ors,  false) as gave_ors,
    coalesce(e.gave_al,   false) as gave_al
  from {{ ref('under_five_assessment_enriched') }} e
  cross join bounds b
  where e.reported_date >= b.min_start_date
    and e.reported_date <  b.max_stop_date
),

expanded as (
  select
    location_id,
    reported_date,
    patient_id,
    unnest(array[
      'u5_assessed',

      case when has_diarrhoea then 'u5_diarrhea_cases' end,
      case when has_pneumonia then 'u5_pneumonia_cases' end,
      case when has_malnutrition then 'u5_malnutrition_cases' end,
      case when has_malaria then 'u5_confirmed_malaria_cases' end,
      case
        when has_fever
         and not has_malaria
         and (not has_pneumonia or not has_diarrhoea)
        then 'u5_suspected_malaria_cases'
      end,

      case when has_fever and has_been_referred then 'referred_for_malaria' end,
      case when has_pneumonia and has_been_referred then 'referred_for_pneumonia' end,
      case when has_malnutrition and has_been_referred then 'referred_for_malnutrition' end,
      case when has_diarrhoea and has_been_referred then 'referred_for_diarrhoea' end,
      case when has_been_referred then 'u5_referred' end,
      case when has_been_referred and sex = 'male' then 'u5_referred_male' end,
      case when has_been_referred and sex = 'female' then 'u5_referred_female' end,

      case when referred_for_development_milestones then 'referred_for_development_milestones' end,
      case when referred_for_development_milestones and sex = 'male' then 'male_referred_for_development_milestones' end,
      case when referred_for_development_milestones and sex = 'female' then 'female_referred_for_development_milestones' end,

      case when (gave_ors or gave_zinc or gave_al or gave_amox) then 'u5_treated' end,
      case when gave_al then 'u5_treated_malaria' end,
      case when (gave_ors or gave_zinc) then 'u5_treated_diarrhoea' end,
      case when (gave_amox and has_pneumonia) then 'u5_treated_pneumonia' end,

      case
        when (rdt_result is not null and rdt_result <> 'not_done')
        then 'u5_tested_malaria'
      end
    ]) as metric_id
  from base
),

mapped as (
  select
    e.location_id,
    p.period_id,
    e.metric_id,
    e.patient_id
  from expanded e
  join eligible_periods p
    on e.reported_date >= p.start_date
   and e.reported_date <  p.stop_date
  where e.metric_id is not null
),

agg as (
  select
    location_id,
    period_id,
    metric_id,
    count(patient_id) as value
  from mapped
  group by 1,2,3
)

select
  location_id,
  period_id,
  metric_id,
  value,
  current_timestamp as last_updated
from agg
where value > 0
