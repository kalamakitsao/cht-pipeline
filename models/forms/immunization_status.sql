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
{% set immunization_service_columns %}
  {{ patient_age_columns() }},
  data_record.patient_id,
  	--case when position('bcg' in (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text)  > 0 then'complete' else'incomplete' end as has_bcg,
  	case when (couchdb.doc->'fields'->'group_vaccines'->> 'vaccines_given')::text  like '%bcg%' or couchdb.doc->'fields' ->> 'has_bcd' = '1' then 'complete' else 'incomplete' end as has_bcg,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%ipv%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as  has_ipv,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%opv_0%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_opv_0,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%opv_1%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end ashas_opv_1,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%opv_2%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_opv_2,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%opv_3%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_opv_3,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%pcv_1%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_pcv_1,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%pcv_2%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_pcv_2,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%pcv_3%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_pcv_3,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%rota_1%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_rota_1,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%rota_2%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_rota_2,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%rota_3%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_rota_3,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%vitamin_a%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_vit_a,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%penta_1%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_penta_1,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%penta_2%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end ashas_penta_2,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%penta_3%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_penta_3,
	couchdb.doc->'fields' ->> 'has_vitamin_a_12_months' AS vit_a_12,
	couchdb.doc->'fields' ->> 'has_vitamin_a_18_months' AS vit_a_18,
	couchdb.doc->'fields' ->> 'has_vitamin_a_24_months' AS vit_a_24,
	couchdb.doc->'fields' ->> 'has_vitamin_a_30_months' AS vit_a_30,
	couchdb.doc->'fields' ->> 'has_vitamin_a_36_months' AS vit_a_36,
	couchdb.doc->'fields' ->> 'has_vitamin_a_42_months' AS vit_a_42,
	couchdb.doc->'fields' ->> 'has_vitamin_a_48_months' AS vit_a_48,
	couchdb.doc->'fields' ->> 'has_vitamin_a_54_months' AS vit_a_54,
	couchdb.doc->'fields' ->> 'has_vitamin_a_60_months' AS vit_a_60,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%measles_6%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_measles_6,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%measles_9%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_measles_9,
	case when (couchdb.doc->'fields' ->'group_vaccines'->> 'vaccines_given')::text  like '%measles_18%' or couchdb.doc->'fields' ->> 'has_ipv' = '1' then 'complete' else 'incomplete' end as has_measles_18,
	couchdb.doc->'fields' -> 'group_vaccines'->> 'vaccines_given' as vaccines_given,
	couchdb.doc->'fields' ->> 'needs_deworming_follow_up' AS needs_deworming_follow_up,
	couchdb.doc->'fields' ->> 'needs_growth_monitoring_referral' AS needs_growth_monitoring_referral,
	couchdb.doc->'fields' -> 'group_summary' ->> 'r_imm_not_uptodate' AS imm_schedule_upto_date,
	(NULLIF(couchdb.doc->'fields' #>> '{follow_up_date}'::text[],''))::date as follow_up_date,
	couchdb.doc->'fields' -> 'immunization_screening' ->> 'due_count' AS due_count,
	couchdb.doc->'fields' -> 'group_vaccines' ->> 'source_of_immunization_information' AS imm_datasource,
	couchdb.doc->'fields' ->> 'needs_immunization_follow_up' AS needs_immunization_referral,
	data_record.form as form,
	data_record.grandparent_uuid AS reported_by_parent_parent
{% endset %}
	-- immunization details from under 5 screening forms

{% set u5_assessment_columns %}
  {{ patient_age_columns() }},
  data_record.patient_id,
  case when (couchdb.doc->'fields'->'immunization_screening'->>'vaccines')::text  like '%bcg%' or couchdb.doc->'fields'->>'bcg'<>'' then 'complete' else 'incomplete' end as has_bcg,
  	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%ipv%' or couchdb.doc->'fields'->>'ipv'<>'' then 'complete' else 'incomplete' end as  has_ipv,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%opv0%' or couchdb.doc->'fields'->>'opv0'<>'' then 'complete' else 'incomplete' end as   has_opv_0,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%opv1%' or couchdb.doc->'fields'->>'opv1'<>'' then 'complete' else 'incomplete' end as   has_opv_1,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%opv2%' or couchdb.doc->'fields'->>'opv2'<>'' then 'complete' else 'incomplete' end as   has_opv_2,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%opv3%' or couchdb.doc->'fields'->>'opv3'<>'' then 'complete' else 'incomplete' end as   has_opv_3,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%pcv1%' or couchdb.doc->'fields'->>'pcv1'<>'' then 'complete' else 'incomplete' end as   has_pcv_1,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%pcv2%' or couchdb.doc->'fields'->>'pcv2'<>'' then 'complete' else 'incomplete' end as   has_pcv_2,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%pcv3%' or couchdb.doc->'fields'->>'pcv3'<>'' then 'complete' else 'incomplete' end as   has_pcv_3,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%rota1%' or couchdb.doc->'fields'->>'rota1'<>'' then 'complete' else 'incomplete' end as   has_rota_1,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%rota2%' or couchdb.doc->'fields'->>'rota2'<>'' then 'complete' else 'incomplete' end as   has_rota_2,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%rota3%' or couchdb.doc->'fields'->>'rota3'<>'' then 'complete' else 'incomplete' end as   has_rota_3,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%vit_a%' or couchdb.doc->'fields'->>'vit_a'<>'' then 'complete' else 'incomplete' end as   has_vit_a,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%penta1%' or couchdb.doc->'fields'->>'penta1'<>'' then 'complete' else 'incomplete' end as   has_penta_1,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%penta2%' or couchdb.doc->'fields'->>'penta2'<>'' then 'complete' else 'incomplete' end as   has_penta_2,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%penta3%' or couchdb.doc->'fields'->>'penta3'<>'' then 'complete' else 'incomplete' end as   has_penta_3,
	couchdb.doc->'fields'->>'vit_a_12' as has_vit_a_12,
	couchdb.doc->'fields'->>'vit_a_18' as has_vit_a_18,
	couchdb.doc->'fields'->>'vit_a_24' as has_vit_a_24,
	couchdb.doc->'fields'->>'vit_a_30' as has_vit_a_30,
	couchdb.doc->'fields'->>'vit_a_36' as has_vit_a_36,
	couchdb.doc->'fields'->>'vit_a_42' as has_vit_a_42,
	couchdb.doc->'fields'->>'vit_a_48' as has_vit_a_48,
	couchdb.doc->'fields'->>'vit_a_54' as has_vit_a_54,
	couchdb.doc->'fields'->>'vit_a_60' as has_vit_a_60,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%measles_6%' or couchdb.doc->'fields'->>'measles_6'<>'' then 'complete' else 'incomplete' end as  has_measles_6,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%measles_9%' or couchdb.doc->'fields'->>'measles_9'<>'' then 'complete' else 'incomplete' end as  has_measles_9,
	case when couchdb.doc->'fields'->'immunization_screening'->>'vaccines'::text  like '%measles_18%' or couchdb.doc->'fields'->>'measles_18'<>'' then 'complete' else 'incomplete' end as  has_measles_18,
	couchdb.doc->'fields'->'immunization_screening'->>'vaccines' as vaccines_given,
	case when(couchdb.doc->'fields'->'deworming'->>'deworming_upto_date')::text='no'::text then'yes' 
		when(couchdb.doc->'fields'->'deworming'->>'deworming_upto_date')::text='yes'::text then'no' 
		else null 
		end as needs_deworming_follow_up,
	couchdb.doc->'fields'->>'needs_growth_monitoring_referral' as needs_growth_monitoring_referral,
	couchdb.doc->'fields'->'immunization_screening'->>'imm_schedule_upto_date' as imm_schedule_upto_date,
	(NULLIF(couchdb.doc->'fields'#>>'{data,_follow_up_date}'::text[],''))::date as follow_up_date,
	couchdb.doc->'fields'->'immunization_screening'->>'due_count' AS due_count,
	couchdb.doc->'fields'->'immunization_screening'->>'imm_datasource' AS imm_datasource,
	couchdb.doc->'fields'->>'needs_immunization_referral' AS needs_immunization_referral,
	data_record.form as form,
	data_record.grandparent_uuid AS reported_by_parent_parent
{% endset %}

{{ cht_form_multi([
  { 'form_name': 'immunzation_service', 'form_columns': immunization_service_columns },
  { 'form_name': 'u5_assessment', 'form_columns': u5_assessment_columns}
]) }}
