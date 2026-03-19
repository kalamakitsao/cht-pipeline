-- models/fact/metrics/fact_over_five_metrics_rolling_year.sql
{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','ncd','cadence_twice_daily'],
  on_schema_change = 'ignore'
) }}

with eligible_periods as (
  select
    period_id,
    period_id_name,
    start_date,
    end_date + interval '1 day' as stop_date
  from {{ ref('dim_period') }}
  where period_id_name not in ('today', 'all_time')
),

bounds as (
  select
    min(start_date) as min_start_date,
    max(stop_date) as max_stop_date
  from eligible_periods
),

base as (
  select
    e.reported_by_parent as location_id,
    e.reported_date,
    e.patient_id,
    e.sex,
    e.screened_for_diabetes,
    e.is_referred_diabetes,
    e.screened_for_hypertension,
    e.is_referred_hypertension,
    e.screened_for_mental_health,
    e.is_referred_mental_health,
    e.has_been_referred
  from {{ ref('over_five_assessment_enriched') }} e
  cross join bounds b
  where e.reported_date >= b.min_start_date
    and e.reported_date <  b.max_stop_date
),

events_expanded as (
  select
    b.location_id,
    b.reported_date,
    unnest(array[
      case when b.screened_for_diabetes      is true then 'screenings_diabetes' end,
      case when b.screened_for_hypertension  is true then 'screenings_hypertension' end,
      case when b.screened_for_mental_health is true then 'screenings_mental_health' end,

      case when b.screened_for_diabetes      is true and b.sex = 'male'   then 'screenings_diabetes_male' end,
      case when b.screened_for_diabetes      is true and b.sex = 'female' then 'screenings_diabetes_female' end,

      case when b.screened_for_hypertension  is true and b.sex = 'male'   then 'screenings_hypertension_male' end,
      case when b.screened_for_hypertension  is true and b.sex = 'female' then 'screenings_hypertension_female' end,

      case when b.screened_for_mental_health is true and b.sex = 'male'   then 'screenings_mental_health_male' end,
      case when b.screened_for_mental_health is true and b.sex = 'female' then 'screenings_mental_health_female' end
    ]) as metric_id
  from base b
),

events_mapped as (
  select
    e.location_id,
    p.period_id,
    e.metric_id
  from events_expanded e
  join eligible_periods p
    on e.reported_date >= p.start_date
   and e.reported_date <  p.stop_date
  where e.metric_id is not null
),

events_agg as (
  select
    location_id,
    period_id,
    metric_id,
    count(*) as value
  from events_mapped
  group by 1,2,3
),

people_expanded as (
  select
    b.location_id,
    b.reported_date,
    b.patient_id,
    unnest(array[
      case when b.screened_for_diabetes      is true then 'screened_diabetes' end,
      case when b.screened_for_diabetes      is true and b.sex = 'male'   then 'screened_diabetes_male' end,
      case when b.screened_for_diabetes      is true and b.sex = 'female' then 'screened_diabetes_female' end,

      case when b.screened_for_hypertension  is true then 'screened_hypertension' end,
      case when b.screened_for_hypertension  is true and b.sex = 'male'   then 'screened_hypertension_male' end,
      case when b.screened_for_hypertension  is true and b.sex = 'female' then 'screened_hypertension_female' end,

      case when b.screened_for_mental_health is true then 'screened_mental_health' end,
      case when b.screened_for_mental_health is true and b.sex = 'male'   then 'screened_mental_health_male' end,
      case when b.screened_for_mental_health is true and b.sex = 'female' then 'screened_mental_health_female' end,

      case when b.is_referred_diabetes       is true then 'referred_diabetes' end,
      case when b.is_referred_diabetes       is true and b.sex = 'male'   then 'referred_diabetes_male' end,
      case when b.is_referred_diabetes       is true and b.sex = 'female' then 'referred_diabetes_female' end,

      case when b.is_referred_hypertension   is true then 'referred_hypertension' end,
      case when b.is_referred_hypertension   is true and b.sex = 'male'   then 'referred_hypertension_male' end,
      case when b.is_referred_hypertension   is true and b.sex = 'female' then 'referred_hypertension_female' end,

      case when b.is_referred_mental_health  is true then 'referred_mental_health' end,
      case when b.is_referred_mental_health  is true and b.sex = 'male'   then 'referred_mental_health_male' end,
      case when b.is_referred_mental_health  is true and b.sex = 'female' then 'referred_mental_health_female' end,

      case when b.has_been_referred          is true then 'over_5_referred' end,
      case when b.has_been_referred          is true and b.sex = 'male'   then 'over_5_referred_male' end,
      case when b.has_been_referred          is true and b.sex = 'female' then 'over_5_referred_female' end,

      'over_5_assessments'
    ]) as metric_id
  from base b
),

people_mapped as (
  select
    e.location_id,
    p.period_id,
    e.metric_id,
    e.patient_id
  from people_expanded e
  join eligible_periods p
    on e.reported_date >= p.start_date
   and e.reported_date <  p.stop_date
  where e.metric_id is not null
),

people_agg as (
  select
    location_id,
    period_id,
    metric_id,
    count(distinct patient_id) as value
  from people_mapped
  group by 1,2,3
)

select
  location_id,
  period_id,
  metric_id,
  value,
  current_timestamp as last_updated
from (
  select * from events_agg
  union all
  select * from people_agg
) s
where value > 0
