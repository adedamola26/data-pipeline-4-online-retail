WITH datetime_cte AS (  
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['InvoiceDate']) }} as date_key,
    InvoiceDate as datetime_id,
    PARSE_DATETIME('%m/%d/%Y %I:%M %p', InvoiceDate) AS date_part,
  FROM {{ source('retail', 'raw_invoices') }}
  WHERE InvoiceDate IS NOT NULL
)
SELECT
  date_key,
  datetime_id,
  date_part as datetime,
  EXTRACT(YEAR FROM date_part) AS year,
  EXTRACT(MONTH FROM date_part) AS month,
  EXTRACT(DAY FROM date_part) AS day,
  EXTRACT(HOUR FROM date_part) AS hour,
  EXTRACT(MINUTE FROM date_part) AS minute,
  EXTRACT(DAYOFWEEK FROM date_part) AS weekday
FROM datetime_cte