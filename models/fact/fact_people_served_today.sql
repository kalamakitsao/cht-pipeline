{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','people_served','cadence_hourly']
) }}

with base as (
  select
    dr.parent_uuid as location_id,
    dr.reported::date as report_date,
    dr.patient_id
  from {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
  where dr.patient_id is not null
    and dr.reported::date = CURRENT_DATE         -- TODAY only
),
mapped as (
  select b.location_id, pd.period_id, b.patient_id
  from base b
  join {{ ref('dim_period_date_map') }} pd on pd.date = b.report_date WHERE pd.period_id_name = 'today'
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
