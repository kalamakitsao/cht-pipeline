{{ config(
    materialized = 'table',
    indexes = [{'columns': ['uuid'], 'type': 'hash'}],
    tags = ['dim', 'sex']
) }}

SELECT
  uuid,
  sex
FROM {{ ref('patient_f_client') }}
WHERE uuid IS NOT NULL
  AND sex IS NOT NULL
