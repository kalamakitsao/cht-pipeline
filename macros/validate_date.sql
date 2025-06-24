{% macro validate_date(date_column) %}
  (
    CASE
      -- First check if the date_column is in the correct format 'YYYY-MM-DD'
      WHEN {{ date_column }} ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$'
      THEN
        CASE
          -- Handle months with 30 days (April, June, September, November)
          WHEN SUBSTRING({{ date_column }} FROM 6 FOR 2) IN ('04', '06', '09', '11') 
            AND SUBSTRING({{ date_column }} FROM 9 FOR 2)::int <= 30
          THEN to_date({{ date_column }}, 'YYYY-MM-DD')
          
          -- Handle February, leap year and non-leap year cases
          WHEN SUBSTRING({{ date_column }} FROM 6 FOR 2) = '02'
          THEN
            CASE
              -- Check for leap year: divisible by 4, but not by 100 unless also divisible by 400
              WHEN (SUBSTRING({{ date_column }} FROM 1 FOR 4)::int % 4 = 0 
                    AND (SUBSTRING({{ date_column }} FROM 1 FOR 4)::int % 100 != 0 
                         OR SUBSTRING({{ date_column }} FROM 1 FOR 4)::int % 400 = 0))
                    AND SUBSTRING({{ date_column }} FROM 9 FOR 2)::int <= 29
              THEN to_date({{ date_column }}, 'YYYY-MM-DD')
              -- Non-leap year case: February can have only up to 28 days
              WHEN SUBSTRING({{ date_column }} FROM 9 FOR 2)::int <= 28
              THEN to_date({{ date_column }}, 'YYYY-MM-DD')
              ELSE NULL  -- Invalid day in February
            END
          
          -- Handle months with 31 days (January, March, May, July, August, October, December)
          WHEN SUBSTRING({{ date_column }} FROM 6 FOR 2) IN ('01', '03', '05', '07', '08', '10', '12') 
            AND SUBSTRING({{ date_column }} FROM 9 FOR 2)::int <= 31
          THEN to_date({{ date_column }}, 'YYYY-MM-DD')
          ELSE NULL
        END
      ELSE
        NULL
    END
  )
{% endmacro %}
