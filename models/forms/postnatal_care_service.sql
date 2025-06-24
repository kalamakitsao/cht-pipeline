{%- set age_indexes = patient_age_indexes() -%}
{%- set form_indexes = [
      {'columns': ['date_of_delivery']},
      {'columns': ['place_of_delivery']},
      {'columns': ['pregnancy_id']}
    ]
-%}
{% set custom_fields %}
  data_record.patient_id,
  data_record.grandparent_uuid as reported_by_parent_parent,
  {{ patient_age_columns() }},
  (couchdb.doc-> 'fields'->>'pregnancy_id') AS pregnancy_id,
  (couchdb.doc-> 'fields'->>'group_pnc_visit,date_of_delivery')::date AS date_of_delivery,
  couchdb.doc->'fields'->>'place_of_birth' AS place_of_delivery,
  (couchdb.doc->'fields'->>'postnatal_care_service_count')::integer AS pnc_service_count,
  NULLIF(couchdb.doc->'fields'->>'needs_pnc_update_follow_up', '')::boolean AS is_referred_for_pnc_services,
  NULLIF(couchdb.doc->'fields'->>'needs_subsequent_visit','')::boolean AS needs_subsequent_visit,
  NULLIF(couchdb.doc->'fields'->>'needs_danger_signs_follow_up', '')::boolean AS needs_danger_signs_follow_up,
  NULLIF(couchdb.doc->'fields'->>'needs_mental_health_follow_up', '')::boolean AS needs_mental_health_follow_up,
  NULLIF(couchdb.doc->'fields'->>'needs_missed_home_visit_follow_up','')::boolean AS needs_missed_home_visit_follow_up,
  CASE
		WHEN 
			couchdb.doc->'fields'->>'is_in_postnatal_care' ='yes' 
			THEN true 
			else false
	END AS is_in_postnatal_care,
  CASE
		WHEN 
			couchdb.doc->'fields'->>'is_in_pnc' ='yes' 
			THEN true 
			else false
	END AS is_in_pnc,
  CASE
		WHEN 
			couchdb.doc->'fields'->'group_pregnancy_status'->>'has_delivered' ='yes'
			THEN true 
			else false
	END AS has_delivered
{% endset %}

{{ cht_form_model('postnatal_care_service', custom_fields, age_indexes + form_indexes) }}
