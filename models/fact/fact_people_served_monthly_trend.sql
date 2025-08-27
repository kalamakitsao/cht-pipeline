-- models/fact/metrics/fact_people_served_monthly_trend.sql
{{ config(
  materialized='incremental',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','people_served','cadence_weekly']
) }}

select
  dr.parent_uuid as location_id,
  date_trunc('month', reported) as period_start,
  'people_served' as metric_id,
  count(distinct patient_id) as value
from from {{ source(env_var('POSTGRES_SCHEMA'), 'data_record') }} dr
where dr.patient_id is not null
AND reported >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
group by location_id,period_start,metric_id
