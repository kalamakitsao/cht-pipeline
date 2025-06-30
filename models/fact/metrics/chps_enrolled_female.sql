-- models/fact/metrics/chps_enrolled.sql
-- Add logic to count enrolled CHPs per period
{{ config(
    materialized = 'incremental',
    unique_key = ['location_id', 'period_id', 'metric_id'],
    on_schema_change = 'ignore'
) }}

WITH chps AS (
    SELECT location_id
    FROM {{ ref('dim_location') }}
    WHERE level = 'chp area'
),

periods AS (
    SELECT period_id
    FROM {{ ref('dim_period') }}
)

SELECT
    c.location_id,
    p.period_id,
    'chps_enrolled' AS metric_id,
    1 AS value,
    CURRENT_TIMESTAMP AS last_updated
FROM chps c
CROSS JOIN periods p
