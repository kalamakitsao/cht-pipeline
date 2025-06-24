{% macro upsert_dim_period() %}
    {% set sql %}
    CREATE TABLE IF NOT EXISTS dim_period (
        period_id SERIAL PRIMARY KEY,
        label TEXT UNIQUE,
        period_type TEXT,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL
    );

    INSERT INTO dim_period (label, period_type, start_date, end_date)
    VALUES
      ('Today', 'daily', CURRENT_DATE, CURRENT_DATE),
      ('Yesterday', 'daily', CURRENT_DATE - INTERVAL '1 day', CURRENT_DATE - INTERVAL '1 day'),
      ('Last 7 Days', 'daily', CURRENT_DATE - INTERVAL '7 days', CURRENT_DATE),
      ('Last 1 Month', 'monthly', CURRENT_DATE - INTERVAL '1 month', CURRENT_DATE),
      ('Last 3 Months', 'monthly', CURRENT_DATE - INTERVAL '3 months', CURRENT_DATE),
      ('Last 6 Months', 'monthly', CURRENT_DATE - INTERVAL '6 months', CURRENT_DATE),
      ('Last 1 Year', 'yearly', CURRENT_DATE - INTERVAL '1 year', CURRENT_DATE),
      ('This Week', 'weekly', DATE_TRUNC('week', CURRENT_DATE), CURRENT_DATE),
      ('Last Week', 'weekly', DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 week'), DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1 day'),
      ('This Month', 'monthly', DATE_TRUNC('month', CURRENT_DATE), CURRENT_DATE),
      ('Last Month', 'monthly', DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month'), DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day'),
      ('This Quarter', 'quarterly', DATE_TRUNC('quarter', CURRENT_DATE), CURRENT_DATE),
      ('Last Quarter', 'quarterly', DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months'), DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1 day'),
      ('Year to Date (YTD)', 'yearly', DATE_TRUNC('year', CURRENT_DATE), CURRENT_DATE)
    ON CONFLICT (label) DO UPDATE
      SET start_date = EXCLUDED.start_date,
          end_date = EXCLUDED.end_date,
          period_type = EXCLUDED.period_type;
    {% endset %}

    {% do run_query(sql) %}
    {{ log("dim_period table upserted successfully", info=True) }}
{% endmacro %}
