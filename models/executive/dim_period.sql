{{ config(
    materialized='table',
    full_refresh=true,
    tags=["period", "daily_refresh"],
    post_hook="ANALYZE {{ this }}"
) }}

WITH periods AS (
    SELECT 'Today' AS label, 'daily' AS period_type, CURRENT_DATE AS start_date, CURRENT_DATE AS end_date
    UNION ALL
    SELECT 'Yesterday', 'daily', CURRENT_DATE - INTERVAL '1 day', CURRENT_DATE - INTERVAL '1 day'
    UNION ALL
    SELECT 'Last 7 Days', 'daily', CURRENT_DATE - INTERVAL '7 days', CURRENT_DATE
    UNION ALL
    SELECT 'Last 1 Month', 'monthly', CURRENT_DATE - INTERVAL '1 month', CURRENT_DATE
    UNION ALL
    SELECT 'Last 3 Months', 'monthly', CURRENT_DATE - INTERVAL '3 months', CURRENT_DATE
    UNION ALL
    SELECT 'Last 6 Months', 'monthly', CURRENT_DATE - INTERVAL '6 months', CURRENT_DATE
    UNION ALL
    SELECT 'Last 1 Year', 'yearly', CURRENT_DATE - INTERVAL '1 year', CURRENT_DATE
    UNION ALL
    SELECT 'This Week', 'weekly', DATE_TRUNC('week', CURRENT_DATE), CURRENT_DATE
    UNION ALL
    SELECT 'Last Week', 'weekly',
           DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week'),
           DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day'
    UNION ALL
    SELECT 'This Month', 'monthly', DATE_TRUNC('month', CURRENT_DATE), CURRENT_DATE
    UNION ALL
    SELECT 'Last Month', 'monthly',
           DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'),
           DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day'
    UNION ALL
    SELECT 'This Quarter', 'quarterly', DATE_TRUNC('quarter', CURRENT_DATE), CURRENT_DATE
    UNION ALL
    SELECT 'Last Quarter', 'quarterly',
           DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months'),
           DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day'
    UNION ALL
    SELECT 'Year to Date (YTD)', 'yearly',
           DATE_TRUNC('year', CURRENT_DATE), CURRENT_DATE
)

SELECT
    row_number() OVER () AS period_id, -- or use a surrogate key later
    *
FROM periods
