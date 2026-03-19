-- models/fact/metrics/fact_pnc_newborn.sql

-- depends_on: {{ ref('dim_period') }}

{{ config(
  materialized = 'table',
  unique_key = ['location_id','period_id','metric_id'],
  tags = ['kpi','pnc','newborn','cadence_hourly'],
  on_schema_change = 'ignore'
) }}

with non_all_time_periods as (
  select
    period_id,
    start_date,
    end_date + interval '1 day' as stop_date
  from {{ ref('dim_period') }}
  where period_id_name <> 'all_time'
),

all_time_period as (
  select
    period_id
  from {{ ref('dim_period') }}
  where period_id_name = 'all_time'
),

non_all_time_bounds as (
  select
    min(start_date) as min_start_date,
    max(stop_date) as max_stop_date
  from non_all_time_periods
),

base_recent as (
  select
    e.location_id,
    e.reported_date,
    e.patient_id,
    e.is_referred_immunization,
    e.needs_danger_signs_follow_up,
    e.needs_missed_visit_follow_up,
    e.place_of_birth_display,
    e.pnc_service_count,
    (e.is_referred or e.is_referred_for_pnc_services or e.is_referred_immunization) as any_referred
  from {{ ref('postnatal_care_service_newborn_enriched') }} e
  cross join non_all_time_bounds b
  where e.patient_id is not null
    and e.reported_date >= b.min_start_date
    and e.reported_date <  b.max_stop_date
),

recent_with_period as (
  select
    b.location_id,
    p.period_id,
    b.patient_id,
    b.is_referred_immunization,
    b.needs_danger_signs_follow_up,
    b.needs_missed_visit_follow_up,
    b.place_of_birth_display,
    b.pnc_service_count,
    b.any_referred
  from base_recent b
  join non_all_time_periods p
    on b.reported_date >= p.start_date
   and b.reported_date <  p.stop_date
),

recent_agg as (
  select
    wp.location_id,
    wp.period_id,
    count(distinct wp.patient_id) filter (where wp.is_referred_immunization) as newborn_referred_immunization,
    count(distinct wp.patient_id) filter (where wp.needs_danger_signs_follow_up) as newborn_needs_danger_signs_follow_up,
    count(distinct wp.patient_id) filter (where wp.needs_missed_visit_follow_up) as newborn_needs_missed_visit_follow_up,
    count(distinct wp.patient_id) filter (where wp.place_of_birth_display ilike '%home%') as newborn_home_delivery,
    count(distinct wp.patient_id) filter (
      where wp.pnc_service_count is not null and wp.pnc_service_count > 0
    ) as newborn_pnc_service_count,
    count(distinct wp.patient_id) filter (where wp.any_referred) as newborn_any_referred
  from recent_with_period wp
  group by wp.location_id, wp.period_id
),

base_all_time as (
  select
    e.location_id,
    tp.period_id,
    e.patient_id,
    e.is_referred_immunization,
    e.needs_danger_signs_follow_up,
    e.needs_missed_visit_follow_up,
    e.place_of_birth_display,
    e.pnc_service_count,
    (e.is_referred or e.is_referred_for_pnc_services or e.is_referred_immunization) as any_referred
  from {{ ref('postnatal_care_service_newborn_enriched') }} e
  cross join all_time_period tp
  where e.patient_id is not null
),

all_time_agg as (
  select
    b.location_id,
    b.period_id,
    count(distinct b.patient_id) filter (where b.is_referred_immunization) as newborn_referred_immunization,
    count(distinct b.patient_id) filter (where b.needs_danger_signs_follow_up) as newborn_needs_danger_signs_follow_up,
    count(distinct b.patient_id) filter (where b.needs_missed_visit_follow_up) as newborn_needs_missed_visit_follow_up,
    count(distinct b.patient_id) filter (where b.place_of_birth_display ilike '%home%') as newborn_home_delivery,
    count(distinct b.patient_id) filter (
      where b.pnc_service_count is not null and b.pnc_service_count > 0
    ) as newborn_pnc_service_count,
    count(distinct b.patient_id) filter (where b.any_referred) as newborn_any_referred
  from base_all_time b
  group by b.location_id, b.period_id
),

agg as (
  select * from recent_agg
  union all
  select * from all_time_agg
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
      ('newborn_referred_immunization',        a.newborn_referred_immunization),
      ('newborn_needs_danger_signs_follow_up', a.newborn_needs_danger_signs_follow_up),
      ('newborn_needs_missed_visit_follow_up', a.newborn_needs_missed_visit_follow_up),
      ('newborn_home_delivery',                a.newborn_home_delivery),
      ('newborn_pnc_service_count',            a.newborn_pnc_service_count),
      ('newborn_any_referred',                 a.newborn_any_referred)
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
