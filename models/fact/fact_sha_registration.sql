-- models/fact/metrics/fact_sha_registration.sql

-- depends_on: {{ ref('dim_period') }}
{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','sha','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

with eligible_periods as (
  select
    period_id,
    start_date,
    end_date + interval '1 day' as stop_date
  from {{ ref('dim_period') }}
),

base as (
  select
    e.location_id,
    e.reported_date,
    e.has_sha_registration
  from {{ ref('sha_registration_enriched') }} e
),

with_period as (
  select
    b.location_id,
    p.period_id,
    b.has_sha_registration
  from base b
  join eligible_periods p
    on b.reported_date >= p.start_date
   and b.reported_date <  p.stop_date
),

agg as (
  select
    wp.location_id,
    wp.period_id,
    count(*)::bigint as households_assessed_sha,
    count(*) filter (
      where wp.has_sha_registration is true
    )::bigint as households_with_sha
  from with_period wp
  group by wp.location_id, wp.period_id
),

metrics as (
  select
    a.location_id,
    a.period_id,
    m.metric_id,
    m.value
  from agg a
  cross join lateral (
    values
      ('households_assessed_sha', a.households_assessed_sha),
      ('households_with_sha', a.households_with_sha)
  ) as m(metric_id, value)
)

select
  location_id,
  period_id,
  metric_id,
  value,
  current_timestamp as last_updated
from metrics
where value > 0
