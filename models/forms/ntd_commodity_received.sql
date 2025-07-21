-- NTD campaigns commodities

{% set form_indexes = [
    {'columns': ['uuid'], 'unique': true},
    {'columns': ['saved_timestamp']},
    {'columns': ['reported']},
    {'columns': ['chp_area_id']},
    {'columns': ['source_id']}
] %}

{% set custom_fields %}
  doc ->> 'source_id' AS source_id,
  doc ->> 'type' AS type,
  doc ->> 'user_id' AS user_id,
  doc ->> 'contact' AS contact_id,
  doc ->> 'reported_date' AS reported_timestamp,

  -- CHV Info
  doc -> 'fields' ->> 'chw_area_id' AS chp_area_id,
  doc -> 'fields' ->> 'chw_name' AS chw_name,

  -- Commodities Received Direct
  -- Use a CASE statement to ensure the value is numeric before casting
  CASE
    WHEN NULLIF(doc -> 'fields' ->> 'rs_mebendazole', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' ->> 'rs_mebendazole', ''))::integer
    ELSE NULL
  END AS rs_mebendazole,
  CASE
    WHEN NULLIF(doc -> 'fields' ->> 'rs_albendazole', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' ->> 'rs_albendazole', ''))::integer
    ELSE NULL
  END AS rs_albendazole,
  CASE
    WHEN NULLIF(doc -> 'fields' ->> 'rs_ivermectin', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' ->> 'rs_ivermectin', ''))::integer
    ELSE NULL
  END AS rs_ivermectin,
  CASE
    WHEN NULLIF(doc -> 'fields' ->> 'rs_praziquantel', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' ->> 'rs_praziquantel', ''))::integer
    ELSE NULL
  END AS rs_praziquantel,
  CASE
    WHEN NULLIF(doc -> 'fields' ->> 'rs_water_guard', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' ->> 'rs_water_guard', ''))::integer
    ELSE NULL
  END AS rs_water_guard,

  -- Commodities Received (Additional Doc)
  CASE
    WHEN NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'praziquantel_supplied', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'praziquantel_supplied', ''))::integer
    ELSE NULL
  END AS praziquantel_supplied,
  CASE
    WHEN NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'ivermectin_supplied', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'ivermectin_supplied', ''))::integer
    ELSE NULL
  END AS ivermectin_supplied,
  CASE
    WHEN NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'mebendazole_supplied', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'mebendazole_supplied', ''))::integer
    ELSE NULL
  END AS mebendazole_supplied,
  CASE
    WHEN NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'albendazole_supplied', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'albendazole_supplied', ''))::integer
    ELSE NULL
  END AS albendazole_supplied,
  CASE
    WHEN NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'water_guard_supplied', '') ~ '^[0-9]+$' THEN (NULLIF(doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'water_guard_supplied', ''))::integer
    ELSE NULL
  END AS water_guard_supplied,

  -- Presence Flags (These are fine as they only check for NULL, not numeric content)
  (doc -> 'fields' ->> 'rs_mebendazole') IS NOT NULL AS has_rs_mebendazole,
  (doc -> 'fields' ->> 'rs_albendazole') IS NOT NULL AS has_rs_albendazole,
  (doc -> 'fields' ->> 'rs_ivermectin') IS NOT NULL AS has_rs_ivermectin,
  (doc -> 'fields' ->> 'rs_praziquantel') IS NOT NULL AS has_rs_praziquantel,
  (doc -> 'fields' ->> 'rs_water_guard') IS NOT NULL AS has_rs_water_guard,
  (doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'praziquantel_supplied') IS NOT NULL AS has_praziquantel_supplied,
  (doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'ivermectin_supplied') IS NOT NULL AS has_ivermectin_supplied,
  (doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'mebendazole_supplied') IS NOT NULL AS has_mebendazole_supplied,
  (doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'albendazole_supplied') IS NOT NULL AS has_albendazole_supplied,
  (doc -> 'fields' -> 'additional_doc' -> 'fields' ->> 'water_guard_supplied') IS NOT NULL AS has_water_guard_supplied
{% endset %}

{{ cht_form_model('commodity_received', custom_fields, form_indexes) }}