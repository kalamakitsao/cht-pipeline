-- models/fact/fact_aggregate.sql
{{ config(
    materialized='table',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_metric_id ON {{ this }} (metric_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_period_id ON {{ this }} (period_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_aggregate_location_id ON {{ this }} (location_id)"
    ]
) }}

SELECT * FROM {{ ref('population') }}
UNION ALL
SELECT * FROM {{ ref('u5_conditions') }}
UNION ALL
SELECT * FROM {{ ref('sha_metrics') }}
UNION ALL
SELECT * FROM {{ ref('chps_enrolled') }}
UNION ALL
SELECT * FROM {{ ref('households_registered') }}
UNION ALL
SELECT * FROM {{ ref('under_1_immunised') }}
UNION ALL
SELECT * FROM {{ ref('people_served') }}
UNION ALL
SELECT * FROM {{ ref('active_chps') }}
UNION ALL
SELECT * FROM {{ ref('referrals') }}
UNION ALL
SELECT * FROM {{ ref('deaths') }}
UNION ALL
SELECT * FROM {{ ref('pregnancy_metrics') }}
UNION ALL
SELECT * FROM {{ ref('households_visited') }}
UNION ALL
SELECT * FROM {{ ref('ncd_metrics') }}
UNION ALL
SELECT * FROM {{ ref('chps_with_households') }}