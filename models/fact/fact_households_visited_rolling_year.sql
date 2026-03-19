{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  indexes=[
    {'columns': ['location_id','period_id','metric_id'], 'unique': true},
    {'columns': ['period_id']},
    {'columns': ['location_id']},
    {'columns': ['metric_id']}
  ],
  tags=['kpi','households_visited','cadence_twice_daily'],
  on_schema_change='ignore'
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
    min(start_date)::date as min_start_date,
    max(stop_date)::date as max_stop_date
  from eligible_periods
),

base as (
  select
    location_id,
    household_id,
    reported_date
  from {{ ref('int_households_visited_daily_pairs') }}
  cross join bounds b
  where reported_date >= b.min_start_date
    and reported_date <  b.max_stop_date
),

mapped as (
  select
    b.location_id,
    p.period_id,
    b.household_id
  from base b
  join eligible_periods p
    on b.reported_date >= p.start_date
   and b.reported_date <  p.stop_date
),

agg as (
  select
    location_id,
    period_id,
    count(distinct household_id)::bigint as value
  from mapped
  group by 1, 2
)

select
  location_id,
  period_id,
  'hh_visited' as metric_id,
  value,
  current_timestamp as last_updated
from agg
where value > 0
