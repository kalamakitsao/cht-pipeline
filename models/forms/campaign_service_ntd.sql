-- This file is part of the eCHIS CHT Pipeline project to generate form data for the NTD Campaigns
-- It extracts data from the campaign_service_ntd form
-- models/forms/campaign_service_ntd.sql

{{
  config(
    materialized = 'incremental',
    unique_key = 'uuid',
    on_schema_change = 'append_new_columns',
    indexes = [
      {'columns': ['uuid'], 'type': 'hash'},
      {'columns': ['saved_timestamp']},
      {'columns': ['reported']},
      {'columns': ['chp_area_id']},
      {'columns': ['source_id']}
    ]
  )
}}

{%- set age_indexes = patient_age_indexes() -%}

{%- set form_indexes = [
  {'columns': ['patient_id']},
  {'columns': ['sth_condition']},
  {'columns': ['trachoma_condition']},
  {'columns': ['consented']},
  {'columns': ['received_sth']}
] -%}

{% set custom_fields %}
  data_record.uuid,
  data_record.saved_timestamp,
  data_record.reported,

  -- Identifiers
  NULLIF(couchdb.doc ->> 'from', '') AS chw_phone_number,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' ->> 'source_id', '') AS source_id,
  NULLIF(couchdb.doc -> 'fields' ->> 'chw_area_id', '') AS chp_area_id,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' ->> 'name', '') AS patient_name,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_id', '') AS patient_id,
  NULLIF(couchdb.doc ->> '_id', '') AS doc_id,
  NULLIF(couchdb.doc ->> '_rev', '') AS doc_rev,
  NULLIF(couchdb.doc ->> 'form', '') AS form_name,
  NULLIF(couchdb.doc ->> 'type', '') AS doc_type,

  -- Demographics
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_gender', '') AS patient_gender,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'patient_date_of_birth' ~ '^\d{4}-\d{2}-\d{2}$'
    THEN (couchdb.doc -> 'fields' ->> 'patient_date_of_birth')::date
    ELSE NULL
  END AS patient_date_of_birth,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'patient_age_in_years' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'patient_age_in_years')::int
    ELSE NULL
  END AS age_in_years,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'patient_age_in_months' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'patient_age_in_months')::int
    ELSE NULL
  END AS age_in_months,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'patient_age_in_days' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'patient_age_in_days')::int
    ELSE NULL
  END AS age_in_days,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'patient_age_in_weeks' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'patient_age_in_weeks')::int
    ELSE NULL
  END AS age_in_weeks,
  NULLIF(couchdb.doc -> 'fields' ->> 'patient_age_display', '') AS age_display,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' ->> 'sex', '') AS contact_sex,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' ->> 'upi', '') AS contact_upi,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' ->> 'patient_id', '') AS contact_patient_id,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'inputs' -> 'contact' ->> 'date_of_birth' ~ '^\d{4}-\d{2}-\d{2}$'
    THEN (couchdb.doc -> 'fields' -> 'inputs' -> 'contact' ->> 'date_of_birth')::date
    ELSE NULL
  END AS contact_date_of_birth,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' -> 'parent' -> 'parent' -> 'contact' ->> 'name', '') AS parent_contact_name,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' -> 'parent' -> 'parent' -> 'contact' ->> 'phone', '') AS parent_contact_phone,

  -- Conditions & Consent
  NULLIF(couchdb.doc -> 'fields' ->> 'sch', '') AS sch_condition,
  NULLIF(couchdb.doc -> 'fields' ->> 'sth', '') AS sth_condition,
  NULLIF(couchdb.doc -> 'fields' ->> 'trachoma', '') AS trachoma_condition,
  NULLIF(couchdb.doc -> 'fields' ->> 'consented', '') AS consented,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'sthSelected' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'sthSelected')::boolean
    ELSE NULL
  END AS sth_selected,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'schSelected' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'schSelected')::boolean
    ELSE NULL
  END AS sch_selected,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'trachomaSelected' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'trachomaSelected')::boolean
    ELSE NULL
  END AS trachoma_selected,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'sthConsented' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'sthConsented')::boolean
    ELSE NULL
  END AS sth_consented,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'schConsented' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'schConsented')::boolean
    ELSE NULL
  END AS sch_consented,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'trachomaConsented' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'trachomaConsented')::boolean
    ELSE NULL
  END AS trachoma_consented,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'givenConsent' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'givenConsent')::boolean
    ELSE NULL
  END AS given_consent,
  NULLIF(couchdb.doc -> 'fields' -> 'treatment' ->> 'consent_sth', '') AS consent_sth,
  NULLIF(couchdb.doc -> 'fields' -> 'treatment' ->> 'received_sth', '') AS received_sth,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'has_tracho_signs' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'has_tracho_signs')::boolean
    ELSE NULL
  END AS has_tracho_signs,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'terminateSchisto' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'terminateSchisto')::boolean
    ELSE NULL
  END AS terminate_schisto,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'onlySchistosomiasis' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'onlySchistosomiasis')::boolean
    ELSE NULL
  END AS only_schistosomiasis,


  -- Initial Assessment
  NULLIF(couchdb.doc -> 'fields' -> 'init' ->> 'sick', '') AS init_sick,

  -- Eligibility
  NULLIF(couchdb.doc -> 'fields' ->> 'eligible', '') AS eligible,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'over_3' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'over_3')::int
    ELSE NULL
  END AS over_3,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'btw_1_3' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'btw_1_3')::int
    ELSE NULL
  END AS btw_1_3,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'over_one' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'over_one')::int
    ELSE NULL
  END AS over_one,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'over_two' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'over_two')::int
    ELSE NULL
  END AS over_two,


  -- Medication and Treatment
  NULLIF(couchdb.doc -> 'fields' ->> 'med_sch', '') AS med_sch,
  NULLIF(couchdb.doc -> 'fields' ->> 'med_sth', '') AS med_sth,
  NULLIF(couchdb.doc -> 'fields' ->> 'med_ab_sth', '') AS med_ab_sth,
  NULLIF(couchdb.doc -> 'fields' ->> 'medication', '') AS medication,
  NULLIF(couchdb.doc -> 'fields' ->> 'treatments', '') AS treatments,
  NULLIF(couchdb.doc -> 'fields' ->> 'med_trachoma_tab', '') AS med_trachoma_tab,
  NULLIF(couchdb.doc -> 'fields' -> 'treatment' ->> 'purpose', '') AS treatment_purpose,
  NULLIF(couchdb.doc -> 'fields' -> 'treatment' ->> 'delivery', '') AS treatment_delivery,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'treatment' ->> 'pregnant' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' -> 'treatment' ->> 'pregnant')::boolean
    ELSE NULL
  END AS treatment_pregnant,
  NULLIF(couchdb.doc -> 'fields' -> 'treatment' ->> 'sensitization', '') AS treatment_sensitization,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'marked_pregnant' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'marked_pregnant')::boolean
    ELSE NULL
  END AS marked_pregnant,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'is_breast_feeding' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'is_breast_feeding')::boolean
    ELSE NULL
  END AS is_breast_feeding,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'is_currently_pregnant' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'is_currently_pregnant')::boolean
    ELSE NULL
  END AS is_currently_pregnant,
  NULLIF(couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'sth_drug', '') AS sth_drug,
  NULLIF(couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'n_albendazole', '') AS n_albendazole,
  NULLIF(couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'n_albendazole_fear', '') AS n_albendazole_fear,
  NULLIF(couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'albendazole_quantity', '') AS albendazole_quantity,
  NULLIF(couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'albendazole_swallowed', '') AS albendazole_swallowed,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'albendazole_tabs_count_no' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'albendazole_tabs_count_no')::int
    ELSE NULL
  END AS albendazole_tabs_count_no,
  NULLIF(couchdb.doc -> 'fields' -> 'soil_treatment_over_15' ->> 'reason_albendazole_not_swallowed', '') AS reason_albendazole_not_swallowed,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'treated_with_albendazole' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'treated_with_albendazole')::boolean
    ELSE NULL
  END AS treated_with_albendazole,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'treated_with_mebendazole' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'treated_with_mebendazole')::boolean
    ELSE NULL
  END AS treated_with_mebendazole,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'treated_with_tetracyline' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'treated_with_tetracyline')::boolean
    ELSE NULL
  END AS treated_with_tetracyline,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'treated_with_praziquantel' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'treated_with_praziquantel')::boolean
    ELSE NULL
  END AS treated_with_praziquantel,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'treated_with_azithromycine_oral' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'treated_with_azithromycine_oral')::boolean
    ELSE NULL
  END AS treated_with_azithromycine_oral,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'treated_with_azithromycine_tabs' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'treated_with_azithromycine_tabs')::boolean
    ELSE NULL
  END AS treated_with_azithromycine_tabs,

  -- Stock counts
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'albendazole_stock_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'albendazole_stock_count')::int
    ELSE NULL
  END AS albendazole_stock_count,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'mebendazole_stock_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'mebendazole_stock_count')::int
    ELSE NULL
  END AS mebendazole_stock_count,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'azithromycin_stock_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'azithromycin_stock_count')::int
    ELSE NULL
  END AS azithromycin_stock_count,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'praziquantel_stock_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'praziquantel_stock_count')::int
    ELSE NULL
  END AS praziquantel_stock_count,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'tetracycline_stock_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'tetracycline_stock_count')::int
    ELSE NULL
  END AS tetracycline_stock_count,
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'azithromycin_powder_stock_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' ->> 'azithromycin_powder_stock_count')::int
    ELSE NULL
  END AS azithromycin_powder_stock_count,

  -- Additional doc fields
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'form', '') AS additional_doc_form,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'type', '') AS additional_doc_type,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'place_id', '') AS additional_doc_place_id,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'chv_area_id', '') AS additional_doc_chv_area_id,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'content_type', '') AS additional_doc_content_type,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'emitting_form', '') AS additional_doc_emitting_form,
  NULLIF(couchdb.doc -> 'fields' -> 'additional_doc' ->> 'created_by_doc', '') AS additional_doc_created_by_doc,

  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'praziquantel_quantity_issued' ~ '^-?\d+\.?\d*$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'praziquantel_quantity_issued')::numeric
    ELSE NULL
  END AS praziquantel_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_albendazole_tabs_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_albendazole_tabs_count')::int
    ELSE NULL
  END AS total_albendazole_tabs_count,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_mebendazole_tabs_count' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_mebendazole_tabs_count')::int
    ELSE NULL
  END AS total_mebendazole_tabs_count,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'azithromycin_250_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'azithromycin_250_quantity_issued')::int
    ELSE NULL
  END AS azithromycin_250_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'campaigns_tetra_eye_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'campaigns_tetra_eye_quantity_issued')::int
    ELSE NULL
  END AS campaigns_tetra_eye_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'campaigns_albendazole_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'campaigns_albendazole_quantity_issued')::int
    ELSE NULL
  END AS campaigns_albendazole_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'campaigns_mebendazole_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'campaigns_mebendazole_quantity_issued')::int
    ELSE NULL
  END AS campaigns_mebendazole_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_azithromycin_250_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_azithromycin_250_quantity_issued')::int
    ELSE NULL
  END AS total_azithromycin_250_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'azithromycin_oral_suspension_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'azithromycin_oral_suspension_quantity_issued')::int
    ELSE NULL
  END AS azithromycin_oral_suspension_quantity_issued,
  CASE
    WHEN couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_azithromycin_oral_suspension_quantity_issued' ~ '^\d+$'
    THEN (couchdb.doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'total_azithromycin_oral_suspension_quantity_issued')::int
    ELSE NULL
  END AS total_azithromycin_oral_suspension_quantity_issued,

  -- CHU Info
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' -> 'parent' -> 'parent' ->> 'chu_code', '') AS chu_code,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' -> 'parent' -> 'parent' ->> 'chu_name', '') AS chu_name,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' -> 'parent' -> 'parent' ->> 'link_facility_code', '') AS link_facility_code,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'contact' -> 'parent' -> 'parent' ->> 'link_facility_name', '') AS facility_name,

  -- Sign-off and metadata
  CASE
    WHEN couchdb.doc -> 'fields' ->> 'needs_signoff' IN ('true', 'false')
    THEN (couchdb.doc -> 'fields' ->> 'needs_signoff')::boolean
    ELSE NULL
  END AS needs_signoff,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_sth', '') AS r_sth_summary,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_sync', '') AS r_sync_summary,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'submit', '') AS submit_summary,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_service', '') AS r_service_summary,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_summary', '') AS r_summary,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_instruction', '') AS r_instruction,
  NULLIF(couchdb.doc -> 'fields' -> 'group_summary' ->> 'r_patient_info', '') AS r_patient_info,

  -- Location and Timestamps
  NULLIF(couchdb.doc -> 'fields' -> 'meta' ->> 'instanceID', '') AS instance_id,
  NULLIF(couchdb.doc -> 'fields' -> 'meta' ->> 'deprecatedID', '') AS deprecated_id,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'meta' -> 'location' ->> 'lat', '') AS location_lat,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'meta' -> 'location' ->> 'long', '') AS location_long,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'meta' -> 'location' ->> 'error', '') AS location_error,
  NULLIF(couchdb.doc -> 'fields' -> 'inputs' -> 'meta' -> 'location' ->> 'message', '') AS location_message,
  NULLIF(couchdb.doc -> 'geolocation' ->> 'code', '') AS geolocation_code,
  NULLIF(couchdb.doc -> 'geolocation' ->> 'message', '') AS geolocation_message,
  CASE
    WHEN couchdb.doc ->> 'reported_date' ~ '^\d+$'
    THEN TO_TIMESTAMP((couchdb.doc ->> 'reported_date')::bigint / 1000)
    ELSE NULL
  END AS reported_date,
  CASE
    WHEN couchdb.doc -> 'form_version' ->> 'time' ~ '^\d+$'
    THEN TO_TIMESTAMP((couchdb.doc -> 'form_version' ->> 'time')::bigint / 1000)
    ELSE NULL
  END AS form_version_time,
  NULLIF(couchdb.doc -> 'form_version' ->> 'sha256', '') AS form_version_sha256,
  NULLIF(couchdb.doc -> 'geolocation_log' -> 0 -> 'recording' ->> 'code', '') AS geolocation_log_code,
  NULLIF(couchdb.doc -> 'geolocation_log' -> 0 -> 'recording' ->> 'message', '') AS geolocation_log_message,
  CASE
    WHEN couchdb.doc -> 'geolocation_log' -> 0 ->> 'timestamp' ~ '^\d+$'
    THEN TO_TIMESTAMP((couchdb.doc -> 'geolocation_log' -> 0 ->> 'timestamp')::bigint / 1000)
    ELSE NULL
  END AS geolocation_log_timestamp,

  -- Other IDs
  NULLIF(couchdb.doc -> 'fields' ->> 'visited_contact_uuid', '') AS visited_contact_uuid,
  NULLIF(couchdb.doc -> 'contact' ->> '_id', '') AS contact_doc_id,
  NULLIF(couchdb.doc -> 'contact' -> 'parent' ->> '_id', '') AS contact_parent_id,
  NULLIF(couchdb.doc -> 'contact' -> 'parent' -> 'parent' ->> '_id', '') AS contact_grandparent_id,
  NULLIF(couchdb.doc -> 'contact' -> 'parent' -> 'parent' -> 'parent' ->> '_id', '') AS contact_great_grandparent_id,
  NULLIF(couchdb.doc -> 'contact' -> 'parent' -> 'parent' -> 'parent' -> 'parent' ->> '_id', '') AS contact_great_great_grandparent_id
{% endset %}

SELECT
  {{ custom_fields }}
FROM {{ ref('data_record') }} data_record
JOIN {{ env_var('POSTGRES_SCHEMA') }}.{{ env_var('POSTGRES_TABLE') }} couchdb
  ON couchdb._id = data_record.uuid
WHERE data_record.form = 'campaign_service_ntd'

{% if is_incremental() %}
  AND data_record.saved_timestamp >= (SELECT MAX(saved_timestamp) FROM {{ this }})
{% endif %}