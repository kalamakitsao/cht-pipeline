{{ config(
    materialized='table',
    indexes=[
      {'columns': ['location_id','household_id'], 'unique': true},
      {'columns': ['location_id']}
    ],
    tags=['kpi','households_visited','cadence_daily']
) }}

SELECT
  location_id,
  household_id,
  MAX(last_updated) AS last_updated
FROM {{ ref('int_households_visited_daily_pairs') }}
GROUP BY 1,2
