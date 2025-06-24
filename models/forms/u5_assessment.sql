{%- set age_indexes = patient_age_indexes() -%}
{% set custom_fields %}
  {{ patient_age_columns() }},
  couchdb.doc->'fields'->'malnutrition_screening'->>'muac_color' AS muac_color,
  CASE
    WHEN (couchdb.doc->'fields'->'u5_assessment'->>'u5_has_fast_breathing') = 'true' OR (couchdb.doc->'fields'->'u2_month_assessment'->>'u2_has_fast_breathing') = 'true' THEN true
    ELSE false
  END AS has_fast_breathing,
  CASE
    WHEN (couchdb.doc->'fields'->'u5_assessment'->>'chest_indrawing') = 'yes' OR (couchdb.doc->'fields'->'u2_month_assessment'->>'chest_indrawing') = 'yes' THEN true
    ELSE false
  END AS has_chest_indrawing,
  CASE
    WHEN (couchdb.doc->'fields'->'group_summary_no_danger_signs'->>'r_given_amox') = 'yes' OR (couchdb.doc->'fields'->'group_summary_danger_signs'->>'r_dt_given_amox') = 'yes' THEN true
    ELSE false
  END AS gave_amox,
NULLIF(couchdb.doc->'fields'->'u5_assessment'->>'has_diarrhoea', '')::boolean AS has_diarrhoea,
  CASE
    WHEN (couchdb.doc->'fields'->'group_summary_no_danger_signs'->>'r_given_zinc') = 'yes' OR (couchdb.doc->'fields'->'group_summary_danger_signs'->>'r_dt_given_zinc') = 'yes' THEN true
    ELSE false
  END AS gave_zinc,
  CASE
    WHEN (couchdb.doc->'fields'->'group_summary_no_danger_signs'->>'r_given_ors') = 'yes' OR (couchdb.doc->'fields'->'group_summary_danger_signs'->>'r_dt_given_ors') = 'yes' THEN true
    ELSE false
  END AS gave_ors,
  CASE
    WHEN (couchdb.doc->'fields'->'u2_month_assessment'->>'has_fever') = 'yes' OR (couchdb.doc->'fields'->'u5_assessment'->>'has_fever') = 'yes' THEN true
    ELSE false
  END AS has_fever,
  CASE
    WHEN (couchdb.doc->'fields'->'u5_assessment'->>'fever_duration') = 'more_14' THEN '14'
    ELSE NULLIF(couchdb.doc->'fields'->'u5_assessment'->>'fever_duration', '')
  END::integer AS fever_duration,
  couchdb.doc->'fields'->'malaria_screening'->>'malaria_test_result' AS rdt_result,
  couchdb.doc->'fields'->'malaria_screening'->>'repeat_malaria_test_result' AS repeat_rdt_result,
  NULLIF(couchdb.doc->'fields'->'group_summary_no_danger_signs'->>'r_given_al', '')::boolean AS gave_al,
  NULLIF(couchdb.doc->'fields'->>'needs_tb_referral', '')::boolean AS needs_tb_referral,
  CASE
      WHEN (couchdb.doc->'fields'->'u5_assessment'->>'has_cough') = 'yes' THEN true
      ELSE false
  END AS has_cough,
  NULLIF(couchdb.doc->'fields'->'nutrition_screening'->>'is_breastfeeding_exclusively', '')::boolean AS is_exclusively_breastfeeding,
  NULLIF(couchdb.doc->'fields'->'growth_and_monitoring'->>'is_participating_growth_monitoring', '')::boolean AS has_uptodate_growth_monitoring,
  NULLIF(couchdb.doc->'fields'->'growth_and_monitoring'->>'is_showing_delayed_development_milestones', '')::boolean AS referred_for_development_milestones,
  (couchdb.doc->'fields'->>'has_been_referred')::boolean AS has_been_referred,
  data_record.patient_id,
  data_record.grandparent_uuid as reported_by_parent_parent
{% endset %}
{{ cht_form_model('u5_assessment', custom_fields, age_indexes) }}
