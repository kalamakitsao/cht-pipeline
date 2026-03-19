{{ config(
  materialized = 'table',
  unique_key = ['location_id', 'period_id', 'metric_id'],
  tags = ['kpi', 'pregnancy', 'cadence_hourly'],
  on_schema_change = 'ignore'
) }}

with eligible_periods as (
  select
    period_id,
    period_id_name,
    start_date,
    end_date + interval '1 day' as stop_date
  from {{ ref('dim_period') }}
),

today_ref as (
  select
    end_date::date as today_date
  from {{ ref('dim_period') }}
  where period_id_name = 'today'
),

phv_base as (
  select
    phv.reported_by_parent as location_id,
    phv.reported,
    phv.patient_id,
    phv.patient_age_in_years,
    phv.is_currently_pregnant,
    phv.is_new_pregnancy,
    phv.has_been_referred,
    phv.has_started_anc,
    phv.is_anc_upto_date,
    phv.current_edd
  from {{ source(env_var('POSTGRES_SCHEMA'), 'pregnancy_home_visit') }} phv
  where phv.patient_id is not null
),

phv_with_period as (
  select
    p.location_id,
    d.period_id,
    p.patient_id,
    p.patient_age_in_years,
    p.is_currently_pregnant,
    p.is_new_pregnancy,
    p.has_been_referred,
    p.has_started_anc,
    p.is_anc_upto_date,
    p.current_edd
  from phv_base p
  join eligible_periods d
    on p.reported >= d.start_date
   and p.reported <  d.stop_date
),

unnested_phv_metrics as (
  select
    p.location_id,
    p.period_id,
    p.patient_id,
    unnest(array[
      case when p.is_currently_pregnant or p.is_new_pregnancy then 'currently_pregnant' end,
      case when p.is_new_pregnancy then 'new_pregnancies' end,
      case when p.is_new_pregnancy and p.patient_age_in_years between 10 and 19 then 'new_teen_pregnancies' end,
      case when (p.is_new_pregnancy or p.is_currently_pregnant) and p.patient_age_in_years between 10 and 19 then 'teen_pregnancies' end,
      'pregnant_women_visited',
      case when p.has_been_referred is true then 'pregnant_women_referred' end,
      case when p.has_been_referred is true and p.is_anc_upto_date is false then 'pregnant_women_referred_anc' end,
      case when p.is_new_pregnancy is true and p.has_been_referred is true and p.has_started_anc is false then 'new_pregnant_women_referred_anc' end,
      case when p.is_new_pregnancy is true and p.has_been_referred is true and p.has_started_anc is false and p.patient_age_in_years between 10 and 19 then 'new_teen_pregnant_women_referred_anc' end,
      case
        when p.is_new_pregnancy is true
         and p.current_edd is not null
         and (((p.current_edd::date - t.today_date) / 7.0 - 40) * -1) between 0 and 12
        then 'first_trimester_pregnancies'
      end
    ]) as metric_id
  from phv_with_period p
  cross join today_ref t
),

phv_kpis as (
  select
    location_id,
    period_id,
    metric_id,
    count(distinct patient_id) as value,
    current_timestamp as last_updated
  from unnested_phv_metrics
  where metric_id is not null
  group by 1,2,3
),

pnc_base as (
  select
    pnc.reported_by_parent as location_id,
    pnc.reported,
    pnc.patient_id,
    pnc.place_of_delivery,
    pnc.pnc_service_count,
    pnc.date_of_delivery,
    pnc.is_referred_for_pnc_services
  from {{ source(env_var('POSTGRES_SCHEMA'), 'postnatal_care_service') }} pnc
  where pnc.patient_id is not null
),

pnc_with_period as (
  select
    p.location_id,
    d.period_id,
    p.patient_id,
    p.place_of_delivery,
    p.pnc_service_count,
    p.date_of_delivery,
    p.is_referred_for_pnc_services
  from pnc_base p
  join eligible_periods d
    on p.reported >= d.start_date
   and p.reported <  d.stop_date
),

unnested_pnc_metrics as (
  select
    location_id,
    period_id,
    patient_id,
    unnest(array[
      case when pnc_service_count = 1 then 'deliveries' end,
      case when place_of_delivery = 'health_facility' then 'skilled_birth_attendance' end,
      case when is_referred_for_pnc_services is true then 'referred_pnc' end
    ]) as metric_id
  from pnc_with_period
),

pnc_kpis as (
  select
    location_id,
    period_id,
    metric_id,
    count(distinct patient_id) as value,
    current_timestamp as last_updated
  from unnested_pnc_metrics
  where metric_id is not null
  group by 1,2,3
),

latest_phv_with_edd as (
  select distinct on (patient_id)
    patient_id,
    reported_by_parent as location_id,
    current_edd
  from {{ source(env_var('POSTGRES_SCHEMA'), 'pregnancy_home_visit') }}
  where is_currently_pregnant = true
    and current_edd is not null
    and patient_id is not null
  order by patient_id, reported desc
),

pregnancy_window as (
  select
    patient_id,
    location_id,
    current_edd,
    current_edd - interval '9 months' as pregnancy_start
  from latest_phv_with_edd
),

actively_pregnant_women as (
  select
    pw.location_id,
    p.period_id,
    'actively_pregnant_women' as metric_id,
    count(distinct pw.patient_id) as value,
    current_timestamp as last_updated
  from pregnancy_window pw
  join eligible_periods p
    on pw.pregnancy_start < p.stop_date
   and pw.current_edd + interval '1 day' > p.start_date
  group by 1,2
),

latest_delivery as (
  select
    patient_id,
    max(date_of_delivery) as last_delivery_date
  from {{ source(env_var('POSTGRES_SCHEMA'), 'postnatal_care_service') }}
  where patient_id is not null
    and date_of_delivery is not null
  group by patient_id
),

repeat_pregnancies as (
  select
    p.location_id,
    d.period_id,
    'repeat_pregnancies' as metric_id,
    count(distinct p.patient_id) as value,
    current_timestamp as last_updated
  from phv_base p
  join latest_delivery ld
    on ld.patient_id = p.patient_id
  join eligible_periods d
    on p.reported >= d.start_date
   and p.reported <  d.stop_date
  where p.is_new_pregnancy is true
    and p.reported::date between ld.last_delivery_date + interval '1 day'
                            and ld.last_delivery_date + interval '12 months'
  group by 1,2
)

select * from phv_kpis
union all
select * from pnc_kpis
union all
select * from actively_pregnant_women
union all
select * from repeat_pregnancies
