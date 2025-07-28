{{ config(
    materialized = 'table',
    tags = ['period', 'date_mapping'],
    post_hook = "CREATE INDEX IF NOT EXISTS idx_period_date_map ON {{ this }} (period_id, date)"
) }}

WITH dim_period AS (
    SELECT * FROM {{ ref('dim_period') }}
),

dates_expanded AS (
    SELECT
        period_id_name AS period_id,
        generate_series(start_date, end_date, interval '1 day')::date AS date
    FROM dim_period
)

SELECT
    period_id,
    date
FROM dates_expanded
