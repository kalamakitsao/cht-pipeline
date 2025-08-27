-- models/fact/metrics/households_visited_union.sql
{{ config(materialized='view', tags=['kpi','cadence_hourly']) }}

SELECT * FROM {{ ref('fact_households_visited_today') }}
UNION ALL
SELECT * FROM {{ ref('fact_households_visited_rolling_year') }}
UNION ALL
SELECT * FROM {{ ref('fact_households_visited_all_time') }}
