-- Create or edit the campaign dates for the SMC program
-- forms/smc_cycle_dates.sql

{{
  config(
    materialized = 'table',
    unique_key = 'cycle_name',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['cycle_name'], 'type': 'hash'},
      {'columns': ['start_date']},
      {'columns': ['end_date']}
    ]
  )
}}
{{
  config(
    materialized='table'
  )
}}

SELECT 
    'SMC' campaign_name,
    '2025' as campaign_year,
    'Cycle 1' as cycle_name,
    '2025-06-05'::date as start_date,
    '2025-07-02'::date as end_date,
    'Turkana' as target_counties

UNION ALL

SELECT 
    'SMC',
    '2025',
    'Cycle 2',
    '2025-07-03'::date,
    '2025-07-15'::date,
    'Turkana' 

UNION ALL

SELECT 
    'SMC',
    '2025',
    'Cycle 3',
    '2025-07-28'::date,
    '2025-08-15'::date,
    'Turkana'

UNION ALL

SELECT 
    'SMC',
    '2025',
    'Cycle 4',
    '2025-08-20'::date,
    '2025-09-15'::date,
    'Turkana' 

UNION ALL

SELECT 
    'SMC',
    '2025',
    'Cycle 5',
    '2025-09-16'::date,
    '2025-10-15'::date,
    'Turkana' 