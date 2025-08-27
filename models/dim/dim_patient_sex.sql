{{ config(
    materialized = 'table',
    indexes = [{'columns': ['uuid'], 'type': 'hash'}],
    tags = ['dim', 'sex','cadence_hourly']
) }}

SELECT
  uuid,
  sex,
  saved_timestamp
FROM {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }}
WHERE uuid IS NOT NULL
  AND sex IS NOT NULL
{% if is_incremental() %}
AND saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}