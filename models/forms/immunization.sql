{{
  config(
    materialized = 'incremental',
    unique_key='uuid',
    on_schema_change='append_new_columns',
    indexes=data_record_indexes()
  )
}}

-- combines immunization records from under 5 assessments and immunization forms
-- immunization details from immunization form
SELECT 
  {{ data_record_columns() }},
    {{ patient_age_columns() }},
	data_record.patient_id,
    (NULLIF((couchdb.doc#>> '{fields,group_summary,r_child_dewormed}'::text[]), ''::text))::boolean AS is_dewormed,
    ((couchdb.doc#>> '{fields,group_vitamin_a,vitamin_a_doses_given}'::text[]) = ANY (ARRAY['none'::text, 'no'::text])) AS is_referred_vitamin_a,
    (NULLIF((couchdb.doc#>> '{fields,needs_immunization_follow_up}'::text[]), ''::text))::boolean AS is_referred_immunization,
    (NULLIF((couchdb.doc#>> '{fields,needs_growth_monitoring_follow_up}'::text[]), ''::text))::boolean AS is_referred_growth_monitoring,
    grandparent_uuid
  {{ data_record_join('immunization_service') }}
UNION ALL
 SELECT 
  {{ data_record_columns() }},
    {{ patient_age_columns() }},
    data_record.patient_id,
    (NULLIF((couchdb.doc#>> '{fields,group_summary_no_danger_signs,r_dewormed_child}'::text[]), ''::text))::boolean AS is_dewormed,
    ((couchdb.doc#>> '{fields,vit_a_group,vit_a_received}'::text[]) = ANY (ARRAY['none'::text, 'no'::text])) AS is_referred_vitamin_a,
    (NULLIF((couchdb.doc#>> '{fields,needs_immunization_referral}'::text[]), ''::text))::boolean AS is_referred_immunization,
    (NULLIF((couchdb.doc#>> '{fields,needs_growth_monitoring_referral}'::text[]), ''::text))::boolean AS is_referred_growth_monitoring,
    grandparent_uuid
{{ data_record_join('u5_assessment') }}
UNION ALL
 SELECT
    {{ data_record_columns() }},
    {{ patient_age_columns() }},
    data_record.patient_id,
    NULL::boolean AS is_dewormed,
    NULL::boolean AS is_referred_vitamin_a,
    (NULLIF((couchdb.doc#>> '{fields,needs_immunization_follow_up}'::text[]), ''::text))::boolean AS is_referred_immunization,
    NULL::boolean AS is_referred_growth_monitoring,
    grandparent_uuid
  {{ data_record_join('postnatal_care_service_newborn') }}
UNION ALL
SELECT 
    data_record.uuid AS uuid,
    data_record.saved_timestamp,
    reported_by,
    reported_by_parent,
    reported,
   	age_in_years::integer as patient_age_in_years,
   	age_in_months::integer AS patient_age_in_months, 
   	NULL::integer AS patient_age_in_days, 
    uuid as patient_id,
    NULL::boolean AS is_dewormed,
    NULL::boolean AS is_referred_vitamin_a,
        CASE
            WHEN imm_upto_date::text = 'no' THEN true
            ELSE false
        END AS is_referred_immunization,
    NULL::boolean AS is_referred_growth_monitoring,
    grandparent_uuid
   FROM {{ ref('contact_screening_at_registration') }}  data_record
  WHERE (NULLIF(data_record.age_in_months::text, ''::text))::integer <= 60
  {% if is_incremental() %}
    AND data_record.saved_timestamp >= {{ max_existing_timestamp('saved_timestamp') }}
  {% endif %}
 UNION ALL
 SELECT 
  {{ data_record_columns() }},
  {{ patient_age_columns() }},
    data_record.patient_id,
    ((couchdb.doc->'fields' ->> 'is_dewormed'::text))::boolean AS is_dewormed,
    ((couchdb.doc->'fields' ->> 'is_referred_vitamin_a'::text))::boolean AS is_referred_vitamin_a,
    ((couchdb.doc->'fields' ->> 'is_referred_immunization'::text))::boolean AS is_referred_immunization,
    ((couchdb.doc->'fields' ->> 'is_referred_growth_monitoring'::text))::boolean AS is_referred_growth_monitoring,
    grandparent_uuid
  {{ data_record_join('immunization') }}

