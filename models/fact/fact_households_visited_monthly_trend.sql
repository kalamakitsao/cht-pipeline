-- models/fact/metrics/fact_households_visited_monthly_trend.sql
{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['location_id','period_start','metric_id'],
  tags=['kpi','households_visited','cadence_weekly'],
  on_schema_change='ignore'
) }}

select
  hv.reported_by_parent AS location_id,
  date_trunc('month', hv.reported) as period_start,
  'hh_visited' AS metric_id,
  count(distinct household) as value
from {{ source(env_var('POSTGRES_SCHEMA'), 'household_visit') }} hv
where hv.household IS NOT NULL
AND reported >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
group by location_id,period_start,metric_id
