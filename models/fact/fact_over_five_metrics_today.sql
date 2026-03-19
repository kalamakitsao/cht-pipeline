-- models/fact/metrics/fact_over_five_metrics_today.sql
{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','ncd','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

with today_period as (
  select
    period_id,
    end_date as start_date,
    end_date + interval '1 day' as stop_date
  from {{ ref('dim_period') }}
  where period_id_name = 'today'
),

base as (
  select
    e.reported_by_parent  as location_id,
    tp.period_id,
    e.patient_id,
    e.sex,
    e.screened_for_diabetes,
    e.screened_for_hypertension,
    e.screened_for_mental_health
  from {{ ref('over_five_assessment_enriched') }} e
  cross join today_period tp
  where e.reported_date >= tp.start_date
    and e.reported_date <  tp.stop_date
),

base_dedup as (
  select distinct
    location_id,
    period_id,
    patient_id,
    sex,
    screened_for_diabetes,
    screened_for_hypertension,
    screened_for_mental_health
  from base
),

events_expanded as (
  select
    b.location_id,
    b.period_id,
    unnest(array[
      case when b.screened_for_diabetes      is true then 'screenings_diabetes' end,
      case when b.screened_for_diabetes      is true and b.sex = 'male'   then 'screenings_diabetes_male' end,
      case when b.screened_for_diabetes      is true and b.sex = 'female' then 'screenings_diabetes_female' end,

      case when b.screened_for_hypertension  is true then 'screenings_hypertension' end,
      case when b.screened_for_hypertension  is true and b.sex = 'male'   then 'screenings_hypertension_male' end,
      case when b.screened_for_hypertension  is true and b.sex = 'female' then 'screenings_hypertension_female' end,

      case when b.screened_for_mental_health is true then 'screenings_mental_health' end,
      case when b.screened_for_mental_health is true and b.sex = 'male'   then 'screenings_mental_health_male' end,
      case when b.screened_for_mental_health is true and b.sex = 'female' then 'screenings_mental_health_female' end
    ]) as metric_id
  from base_dedup b
),

people_expanded as (
  select
    b.location_id,
    b.period_id,
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
      case when b.screened_for_mental_health is true and b.sex = 'female' then 'screened_mental_health_female' end
    ]) as metric_id
  from base_dedup b
),

events_agg as (
  select
    location_id,
    period_id,
    metric_id,
    count(*) as value
  from events_expanded
  where metric_id is not null
  group by 1,2,3
),

people_agg as (
  select
    location_id,
    period_id,
    metric_id,
    count(distinct patient_id) as value
  from people_expanded
  where metric_id is not null
  group by 1,2,3
),

all_agg as (
  select * from events_agg
  union all
  select * from people_agg
)

select
  location_id,
  period_id,
  metric_id,
  value,
  current_timestamp as last_updated
from all_agg
where value > 0
