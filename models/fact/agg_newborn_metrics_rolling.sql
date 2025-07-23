{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id', 'snapshot_date'],
    on_schema_change = 'append_new_columns'
) }}

SELECT
    location_id,
    period_label,
    metric_id,
    COUNT(DISTINCT patient_id) AS value
    CURRENT_DATE AS snapshot_date,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_period_patient_map_newborn') }}
GROUP BY 1, 2, 3
