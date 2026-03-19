-- models/fact/metrics/people_served_all_time.sql
{{ config(
  materialized='table',
  unique_key=['location_id','period_id','metric_id'],
  indexes=[
    {'columns': ['location_id','period_id','metric_id'], 'unique': true},
    {'columns': ['metric_id']},
    {'columns': ['period_id']},
    {'columns': ['location_id']}
  ],
  tags=['kpi','people_served','cadence_daily']
) }}

with all_time_period as (
  select period_id
  from {{ ref('dim_period') }}
  where period_id_name = 'all_time'
),

agg as (
  select
    location_id,
    count(*)::bigint as value,
    max(last_updated) as last_updated
  from {{ ref('int_people_served_pairs_all_time') }}
  group by 1
)

select
  a.location_id,
  p.period_id,
  'people_served' as metric_id,
  a.value,
  a.last_updated
from agg a
cross join all_time_period p
