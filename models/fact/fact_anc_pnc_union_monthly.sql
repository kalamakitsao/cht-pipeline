{{ config(materialized='view', tags=['kpi','cadence_daily']) }}

SELECT * FROM {{ ref('fact_pregnancy_metrics_monthly_trend') }}
UNION ALL
SELECT * FROM {{ ref('fact_pnc_newborn_monthly_trend') }}
