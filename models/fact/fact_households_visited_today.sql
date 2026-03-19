-- models/fact/metrics/households_visited_today.sql
{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','households_visited','cadence_hourly'],
  on_schema_change='ignore'
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
    hv.reported_by_parent as location_id,
    tp.period_id,
    hv.household as household_id
  from {{ source(env_var('POSTGRES_SCHEMA'), 'household_visit') }} hv
  cross join today_period tp
  where hv.household is not null
    and hv.reported >= tp.start_date
    and hv.reported < tp.stop_date
),

agg as (
  select
    location_id,
    period_id,
    count(distinct household_id) as value
  from base
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
