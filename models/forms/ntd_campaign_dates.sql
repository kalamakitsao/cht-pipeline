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

SELECT 
    'NTD' as campaign_name,
    '2024_01' as cycle_name,
    '2024-12-11'::date as start_date,
    '2025-02-10'::date as end_date,
    ARRAY['Kakamega'] as target_counties

UNION ALL

SELECT 
    'NTD' as campaign_name,
    '2025_01' as cycle_name,
    '2025-06-01'::date as start_date,
    '2025-09-30'::date as end_date,
    ARRAY['Kakamega','Siaya','Bungoma','Transnzoia','Vihiga County'] as target_counties

UNION ALL