-- models/fact/rolling/agg_fully_immunised_metrics_rolling.sql

{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_label', 'metric_id', 'snapshot_date'],
    on_schema_change = 'ignore'
) }}

SELECT
  period_label,
  location_id,
  metric_id,
  COUNT(DISTINCT patient_id) AS value,
  CURRENT_DATE AS snapshot_date,
  CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('fact_period_patient_map_fully_immunised') }}
GROUP BY 1, 2, 3
