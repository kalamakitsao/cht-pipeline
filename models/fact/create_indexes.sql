-- models/maintenance/create_indexes.sql

{{ config(
    materialized = 'ephemeral',
    tags = ['indexing']
) }}

-- Indexes for v1.contact
CREATE INDEX IF NOT EXISTS idx_contact_saved_timestamp ON v1.contact (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_contact_reported ON v1.contact (reported);
CREATE INDEX IF NOT EXISTS idx_contact_name ON v1.contact (name);
CREATE INDEX IF NOT EXISTS idx_contact_contact_type ON v1.contact (contact_type);
CREATE INDEX IF NOT EXISTS idx_contact_active ON v1.contact (active);
CREATE INDEX IF NOT EXISTS idx_contact_notes ON v1.contact (notes);
CREATE INDEX IF NOT EXISTS idx_contact_uuid ON v1.contact (uuid);
CREATE INDEX IF NOT EXISTS idx_contact_muted ON v1.contact (muted);
CREATE INDEX IF NOT EXISTS idx_contact_parent_uuid ON v1.contact (parent_uuid);

-- Indexes for v1.patient_f_client
CREATE INDEX IF NOT EXISTS idx_patient_f_client_reported ON v1.patient_f_client (reported);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_date_of_birth ON v1.patient_f_client (date_of_birth);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_saved_timestamp ON v1.patient_f_client (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_sex ON v1.patient_f_client (sex);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_uuid ON v1.patient_f_client (uuid);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_muted ON v1.patient_f_client (muted);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_name ON v1.patient_f_client (name);
CREATE INDEX IF NOT EXISTS idx_patient_f_client_household_id ON v1.patient_f_client (household_id);

-- Indexes for v1.household
CREATE INDEX IF NOT EXISTS idx_household_reported ON v1.household (reported);
CREATE INDEX IF NOT EXISTS idx_household_saved_timestamp ON v1.household (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_household_uses_treated_water ON v1.household (uses_treated_water);
CREATE INDEX IF NOT EXISTS idx_household_has_functional_latrine ON v1.household (has_functional_latrine);
CREATE INDEX IF NOT EXISTS idx_household_chu_area_id ON v1.household (chu_area_id);
CREATE INDEX IF NOT EXISTS idx_household_household_contact_id ON v1.household (household_contact_id);
CREATE INDEX IF NOT EXISTS idx_household_household_name ON v1.household (household_name);
CREATE INDEX IF NOT EXISTS idx_household_uuid ON v1.household (uuid);
CREATE INDEX IF NOT EXISTS idx_household_chv_area_id ON v1.household (chv_area_id);

-- Indexes for v1.household_visit
CREATE INDEX IF NOT EXISTS idx_household_visit_saved_timestamp ON v1.household_visit (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_household_visit_reported ON v1.household_visit (reported);
CREATE INDEX IF NOT EXISTS idx_household_visit_reported_by_parent ON v1.household_visit (reported_by_parent);
CREATE INDEX IF NOT EXISTS idx_household_visit_form ON v1.household_visit (form);
CREATE INDEX IF NOT EXISTS idx_household_visit_household ON v1.household_visit (household);
CREATE INDEX IF NOT EXISTS idx_household_visit_chw ON v1.household_visit (chw);
CREATE INDEX IF NOT EXISTS idx_household_visit_uuid ON v1.household_visit (uuid);
CREATE INDEX IF NOT EXISTS idx_household_visit_reported_by_parent_parent ON v1.household_visit (reported_by_parent_parent);
CREATE INDEX IF NOT EXISTS idx_household_visit_reported_by ON v1.household_visit (reported_by);

-- (Continue with pregnancy_home_visit, postnatal_care_service, sha_registration, fact_aggregate...)
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_current_edd ON v1.pregnancy_home_visit (current_edd);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_saved_timestamp ON v1.pregnancy_home_visit (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_is_counselled_anc ON v1.pregnancy_home_visit (is_counselled_anc);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_has_started_anc ON v1.pregnancy_home_visit (has_started_anc);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_is_anc_upto_date ON v1.pregnancy_home_visit (is_anc_upto_date);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_next_anc_visit_date ON v1.pregnancy_home_visit (next_anc_visit_date);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_updated_edd ON v1.pregnancy_home_visit (updated_edd);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_reported ON v1.pregnancy_home_visit (reported);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_patient_age_in_years ON v1.pregnancy_home_visit (patient_age_in_years);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_patient_age_in_months ON v1.pregnancy_home_visit (patient_age_in_months);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_patient_age_in_days ON v1.pregnancy_home_visit (patient_age_in_days);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_has_been_referred ON v1.pregnancy_home_visit (has_been_referred);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_is_new_pregnancy ON v1.pregnancy_home_visit (is_new_pregnancy);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_is_currently_pregnant ON v1.pregnancy_home_visit (is_currently_pregnant);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_reported_by ON v1.pregnancy_home_visit (reported_by);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_reported_by_parent ON v1.pregnancy_home_visit (reported_by_parent);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_uuid ON v1.pregnancy_home_visit (uuid);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_patient_id ON v1.pregnancy_home_visit (patient_id);
CREATE INDEX IF NOT EXISTS idx_pregnancy_home_visit_reported_by_parent_parent ON v1.pregnancy_home_visit (reported_by_parent_parent);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_saved_timestamp ON v1.postnatal_care_service (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_is_referred_for_pnc_services ON v1.postnatal_care_service (is_referred_for_pnc_services);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_needs_subsequent_visit ON v1.postnatal_care_service (needs_subsequent_visit);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_needs_danger_signs_follow_up ON v1.postnatal_care_service (needs_danger_signs_follow_up);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_needs_mental_health_follow_up ON v1.postnatal_care_service (needs_mental_health_follow_up);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_needs_missed_home_visit_follow_up ON v1.postnatal_care_service (needs_missed_home_visit_follow_up);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_is_in_postnatal_care ON v1.postnatal_care_service (is_in_postnatal_care);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_is_in_pnc ON v1.postnatal_care_service (is_in_pnc);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_reported ON v1.postnatal_care_service (reported);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_patient_age_in_years ON v1.postnatal_care_service (patient_age_in_years);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_patient_age_in_months ON v1.postnatal_care_service (patient_age_in_months);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_patient_age_in_days ON v1.postnatal_care_service (patient_age_in_days);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_date_of_delivery ON v1.postnatal_care_service (date_of_delivery);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_pnc_service_count ON v1.postnatal_care_service (pnc_service_count);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_reported_by ON v1.postnatal_care_service (reported_by);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_reported_by_parent ON v1.postnatal_care_service (reported_by_parent);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_place_of_delivery ON v1.postnatal_care_service (place_of_delivery);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_patient_id ON v1.postnatal_care_service (patient_id);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_reported_by_parent_parent ON v1.postnatal_care_service (reported_by_parent_parent);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_pregnancy_id ON v1.postnatal_care_service (pregnancy_id);
CREATE INDEX IF NOT EXISTS idx_postnatal_care_service_uuid ON v1.postnatal_care_service (uuid);
CREATE INDEX IF NOT EXISTS idx_fact_aggregate_period_id ON v1.fact_aggregate (period_id);
CREATE INDEX IF NOT EXISTS idx_fact_aggregate_value ON v1.fact_aggregate (value);
CREATE INDEX IF NOT EXISTS idx_fact_aggregate_last_updated ON v1.fact_aggregate (last_updated);
CREATE INDEX IF NOT EXISTS idx_fact_aggregate_location_id ON v1.fact_aggregate (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_aggregate_metric_id ON v1.fact_aggregate (metric_id);
CREATE INDEX IF NOT EXISTS idx_sha_registration_reported ON v1.sha_registration (reported);
CREATE INDEX IF NOT EXISTS idx_sha_registration_saved_timestamp ON v1.sha_registration (saved_timestamp);
CREATE INDEX IF NOT EXISTS idx_sha_registration_has_sha_registration ON v1.sha_registration (has_sha_registration);
CREATE INDEX IF NOT EXISTS idx_sha_registration_member_name ON v1.sha_registration (member_name);
CREATE INDEX IF NOT EXISTS idx_sha_registration_sex ON v1.sha_registration (sex);
CREATE INDEX IF NOT EXISTS idx_sha_registration_household_name ON v1.sha_registration (household_name);
CREATE INDEX IF NOT EXISTS idx_sha_registration_cert_seen_by_chp ON v1.sha_registration (cert_seen_by_chp);
CREATE INDEX IF NOT EXISTS idx_sha_registration_member_willing_to_register ON v1.sha_registration (member_willing_to_register);
CREATE INDEX IF NOT EXISTS idx_sha_registration_uuid ON v1.sha_registration (uuid);
CREATE INDEX IF NOT EXISTS idx_sha_registration_date_when_want_to_register ON v1.sha_registration (date_when_want_to_register);
CREATE INDEX IF NOT EXISTS idx_sha_registration_reported_by_parent ON v1.sha_registration (reported_by_parent);
CREATE INDEX IF NOT EXISTS idx_sha_registration_member_uuid ON v1.sha_registration (member_uuid);
