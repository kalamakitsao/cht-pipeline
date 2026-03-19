{{ config(
    materialized = 'table',
    indexes = [
        {'columns': ['chp_area_id', 'period_id'], 'unique': true}
    ],
    tags = ['kpi','immunization','cadence_hourly']
) }}

select
    chp_area_id,
    period_id,
    count(*)::bigint as children_turning_one,
    count(*) filter (where sex = 'female')::bigint as female_turning_one,
    count(*) filter (where sex = 'male')::bigint as male_turning_one,
    current_timestamp as last_updated
from {{ ref('children_turning_one') }}
group by 1,2
