-- models/fact/metrics/fact_referrals_monthly_trend.sql
{{ config(
  materialized='incremental',
  unique_key=['location_id','period_id','metric_id'],
  tags=['kpi','people_served','cadence_weekly']
) }}

WITH combined_referrals AS (
    SELECT
        reported_by_parent AS location_id,
        DATE(reported) AS report_date,
        patient_id
    FROM {{ ref('under_five_assessment_enriched') }}
    WHERE has_been_referred = TRUE
    AND reported >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'

    UNION ALL

    SELECT
        reported_by_parent AS location_id,
        DATE(reported) AS report_date,
        patient_id
    FROM {{ ref('over_five_assessment_enriched') }}
    WHERE has_been_referred = TRUE
    AND reported >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
)

select
  r.location_id,
  date_trunc('month', r.reported) as period_start,
  'total_referrals' AS metric_id,
  count(distinct patient_id) as value
from combined_referrals r
group by location_id,period_start,metric_id
