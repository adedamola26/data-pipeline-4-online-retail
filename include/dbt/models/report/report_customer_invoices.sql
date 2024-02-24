SELECT
  dc.country,
  dc.iso,
  COUNT(fi.invoice_key) AS total_invoices,
  SUM(fi.total_price) AS total_revenue
FROM {{ ref('fct_invoice_line_value') }} fi
JOIN {{ ref('dim_invoice') }} di ON fi.invoice_key = di.invoice_key
JOIN {{ ref('dim_customer') }} dc ON di.customer_key = dc.customer_key
GROUP BY dc.country, dc.iso
ORDER BY total_revenue DESC
LIMIT 10