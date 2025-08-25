-- models/fact/metrics/chps_enrolled.sql
-- Add logic to count enrolled CHPs per period
{{ config(
    materialized = 'table',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH 
periods AS (
    SELECT period_id
    FROM {{ ref('dim_period') }}
)


SELECT
    c.location_id,
    p.period_id,
    'chps_expected' AS metric_id,
    c.expected_chps AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM {{ ref('chp_projections') }} c
CROSS JOIN periods p
