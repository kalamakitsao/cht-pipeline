{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [
      {'columns': ['fever_duration']},
      {'columns': ['given_al']},
      {'columns': ['has_fever']},
      {'columns': ['rdt_result']},
      {'columns': ['repeat_rdt_result']}
    ]
-%}
{% set custom_fields %}
  data_record.patient_id,
  {{ patient_age_columns() }},
  data_record.grandparent_uuid as reported_by_parent_parent,
  -- group diabetes
  CASE
  		WHEN 
        (couchdb.doc->'fields'->'group_diabetes'->>'is_diabetic') = 'yes' 
      THEN true
  		ELSE false
  END AS is_diabetic,
  (couchdb.doc->'fields'->'group_diabetes'->>'is_diabetic') IS NOT NULL AS screened_for_diabetes,
  couchdb.doc->'fields'->'group_diabetes'->>'diabetes_symptoms' AS diabetes_symptoms,
  CASE
		WHEN (couchdb.doc->'fields'-> 'group_diabetes'->>'is_on_diabetes_medication') = 'yes' 
    THEN true
	  ELSE false
	END AS is_on_diabetes_medication,	
  CASE
      WHEN 
        ((couchdb.doc->'fields'->'group_diabetes'->>'is_on_diabetes_medication' = 'no')) OR 
        ((couchdb.doc->'fields'->'group_diabetes'->>'diabetes_symptoms' <> '') AND (couchdb.doc->'fields'->'group_diabetes'->>'diabetes_symptoms' <> 'none'))
      THEN true
      ELSE false
  END AS is_referred_diabetes,
  (couchdb.doc->'fields'->>'needs_diabetes_referral') is not null as needs_diabetes_referral,

  -- group hypertension
  CASE
  		WHEN 
        (couchdb.doc->'fields'->'group_hypertension'->>'is_hypertensive') = 'yes' 
      THEN true
  		ELSE false
  END AS is_hypertensive,
  (couchdb.doc->'fields'->'group_hypertension'->>'is_hypertensive') IS NOT NULL AS screened_for_hypertension,
  (couchdb.doc->'fields'->'group_hypertension'->>'hypertension_symptoms') AS hypertension_symptoms,
  CASE
		WHEN (couchdb.doc->'fields'->'group_hypertension'->>'is_on_hypertension_medication') = 'yes' 
    THEN true
	  ELSE false
	END AS is_on_hypertension_medication,	
  CASE
      WHEN 
        ((couchdb.doc->'fields'->'group_hypertension'->>'is_on_hypertension_medication') = 'no' ) OR 
        ((couchdb.doc->'fields'->'group_hypertension'->>'hypertension_symptoms') <> '' AND (couchdb.doc->'fields'->'group_hypertension'->>'hypertension_symptoms') <> 'none') OR
        couchdb.doc->'fields'->'group_hypertension'->>'bp_label' = 'LOW'::text OR
        couchdb.doc->'fields'->'group_hypertension'->>'bp_label' = 'HIGH'::text 
      THEN true
      ELSE false
  END AS is_referred_hypertension,
  CASE
  		WHEN 
        (couchdb.doc->'fields'->'group_hypertension'->>'needs_hypertension_referral' ) = 'yes' 
      THEN true
  		ELSE false
  END AS needs_hypertension_referral,
  
  --group mental_health
  couchdb.doc->'fields'->'group_mental_health'->>'mental_signs' as mental_signs,
  couchdb.doc->'fields'-> 'group_mental_health'->>'observed_mental_signs' as observed_mental_signs,
  CASE
    WHEN 
      (couchdb.doc->'fields'->'group_mental_health'->>'mental_signs}' <> 'none' AND couchdb.doc->'fields'->'group_mental_health'->>'mental_signs' is not null) OR
      (couchdb.doc->'fields'->'group_mental_health'->>'observed_mental_signs'<> 'none' AND couchdb.doc->'fields' ->'group_mental_health'->>'observed_mental_signs' is not null)
    THEN true
    ELSE false
  END AS is_mental_health,
  CASE
      WHEN (couchdb.doc->'fields'->'group_mental_health'->>'mental_signs' is not null) OR 
        (couchdb.doc->'fields'->'group_mental_health'->>'observed_mental_signs' is not null)
      THEN true
        ELSE false
  END AS screened_for_mental_health,
  NULLIF(couchdb.doc->'fields'->>'needs_mental_health_referral', ''::text)::boolean AS is_referred_mental_health,

  -- group fever
  NULLIF(couchdb.doc->'fields'->'group_fever'->>'has_fever', ''::text)::boolean AS has_fever,
  NULLIF(couchdb.doc->'fields'->'group_fever'->>'fever_duration', ''::text)::integer AS fever_duration,
  NULLIF(couchdb.doc->'fields'->>'needs_fever_referral', ''::text)::boolean AS needs_fever_referral,

  -- group malaria
  couchdb.doc->'fields'->'group_malaria'->>'rdt_result' AS rdt_result,
  couchdb.doc->'fields'->'group_malaria'->>'repeat_rdt_result' AS repeat_rdt_result,
  NULLIF(couchdb.doc->'fields'->>'needs_malaria_referral', ''::text)::boolean AS needs_malaria_referral,

  -- group summary
  CASE
		when couchdb.doc->'fields'->'group_summary'->>'given_al'= 'yes' THEN true
	    ELSE false
	END AS given_al,
  
  -- group tb
  couchdb.doc->'fields'->'group_tb'->>'tb_symptoms' AS tb_symptoms,
  CASE
		WHEN 
			couchdb.doc->'fields'->'group_tb'->>'is_on_tb_treatment' ='yes' 
			THEN true 
			else false
	END AS _is_on_tb_treatment, 
  CASE
		WHEN 
			(couchdb.doc->'fields'->'group_tb'->>'tb_symptoms' IS NOT NULL AND 
				NULLIF(couchdb.doc->'fields'->'group_tb'->>'tb_symptoms','') <>'none' AND 
				couchdb.doc->'fields'->'group_tb'->>'is_on_tb_treatment' ='no')
		THEN true 
		ELSE false
	END AS is_referred_tb, 
  NULLIF(couchdb.doc->'fields'->>'needs_tb_referral',''::text)::boolean AS needs_tb_referral,

  -- sgbv 
  (couchdb.doc->'fields'->> 'needs_sgbv_referral')::boolean AS needs_sgbv_referral,
  (couchdb.doc->'fields'->> 'has_been_referred')::boolean AS has_been_referred

{% endset %}
{{ cht_form_model('over_five_assessment', custom_fields, age_indexes + form_indexes) }}
