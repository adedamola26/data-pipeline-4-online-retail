SELECT
  dt.year,
  dt.month,
  COUNT(DISTINCT fi.invoice_key) AS num_invoices,
  SUM(fi.total_price) AS total_revenue
FROM {{ ref('fct_invoice_line_value') }} fi
JOIN {{ ref('dim_datetime') }} dt ON fi.date_key = dt.date_key
GROUP BY dt.year, dt.month
ORDER BY dt.year, dt.month