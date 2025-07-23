-- models/fact/metrics/agg_chp_reporting_compliance_metrics_rolling.sql

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id', 'snapshot_date'],
    tags = ['daily_refresh'],
    on_schema_change = 'ignore'
) }}

SELECT
    location_id,
    period_id,
    metric_id,
    MAX(value) AS value,
    CURRENT_DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_period_patient_map_chp_compliance') }}
GROUP BY 1, 2, 3
