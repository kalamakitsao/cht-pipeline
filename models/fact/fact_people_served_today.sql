{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','people_served','cadence_hourly']
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
    dr.parent_uuid as location_id,
    tp.period_id,
    dr.patient_id
  from {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
  cross join today_period tp
  where dr.patient_id is not null
    and dr.reported >= tp.start_date
    and dr.reported < tp.stop_date
),

agg as (
  select
    location_id,
    period_id,
    count(distinct patient_id) as value
  from base
  group by 1, 2
)

select
  location_id,
  period_id,
  'people_served' as metric_id,
  value,
  current_timestamp as last_updated
from agg
