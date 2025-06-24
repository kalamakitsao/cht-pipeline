{% macro generate_period_cte(start_interval, end_date, interval_unit) %}
WITH period_CTE AS (
  SELECT 
    generate_series(
      -- Start date: Truncate the current date minus the specified interval to the start of the month
      date_trunc('month', now() - INTERVAL '{{ start_interval }}'), 
      -- End date: Specified end date
      {{ end_date }},            
      -- Interval: Specified interval unit
      INTERVAL '{{ interval_unit }}'
    )::DATE AS period_start
),
{% endmacro %}
