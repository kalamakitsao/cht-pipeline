{{ config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'sync_all_columns',
    indexes = [
        {'columns': ['uuid'], 'type': 'hash'},
        {'columns': ['sex']},
        {'columns': ['date_of_birth']},
        {'columns': ['household_id']},
        {'columns': ['chp_area_id']},
        {'columns': ['muted']}
    ],
    tags = ['dim', 'sex']
) }}

WITH source AS (
    SELECT
        c.uuid,
        c.sex,
        c.date_of_birth,
        hh.uuid AS household_id,
        hh.chv_area_id AS chp_area_id,
        c.muted,
        c.saved_timestamp
    FROM {{ source(env_var('POSTGRES_SCHEMA'), 'patient_f_client') }} c
    JOIN {{ source(env_var('POSTGRES_SCHEMA'), 'household') }} hh ON c.household_id = hh.uuid
    WHERE c.uuid IS NOT NULL
      AND c.sex IS NOT NULL
)

SELECT
    uuid,
    sex,
    date_of_birth,
    household_id,
    chp_area_id,
    muted,
    saved_timestamp
FROM source

{% if is_incremental() %}
WHERE saved_timestamp > (
    SELECT COALESCE(MAX(saved_timestamp), '2020-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
