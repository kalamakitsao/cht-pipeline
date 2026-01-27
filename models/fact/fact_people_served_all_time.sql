-- models/fact/metrics/people_served_all_time.sql
{{ config(
  materialized='incremental',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','people_served','cadence_daily']
) }}

with base as (
  select
    dr.parent_uuid as location_id,
    dr.reported::date as report_date,
    dr.patient_id
  from {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
    join {{ source(env_var('POSTGRES_SCHEMA'), 'contact') }} c on dr.patient_id = c.uuid 
  where dr.patient_id is not null and c.muted is null
),
mapped as (
  select b.location_id, pd.period_id, b.patient_id
  from base b
  join {{ ref('dim_period_date_map') }} pd on pd.date = b.report_date where period_id_name = 'all_time'
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
