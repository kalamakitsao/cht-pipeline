{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','people_served','cadence_twice_daily']
) }}

with bounds as (
  select (CURRENT_DATE - interval '1 year') as start_d, (CURRENT_DATE - interval '1 day') as end_d
),
base as (
  select
    dr.parent_uuid as location_id,
    dr.reported::date as report_date,
    dr.patient_id
  from {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
  join bounds b on true
  where dr.patient_id is not null
    and dr.reported::date between b.start_d and b.end_d
),
mapped as (
  select b.location_id, pd.period_id, b.patient_id
  from base b
  join {{ ref('dim_period_date_map') }} pd on pd.date = b.report_date WHERE period_id_name <> 'all_time'
),
agg as (
  select location_id, period_id, count(distinct patient_id) as value
  from mapped
  group by 1,2
)
select
  location_id,
  period_id,
  'people_served' as metric_id,
  value,
  current_timestamp as last_updated
from agg
