SELECT
  p.product_key,
  p.stock_code,
  p.description,
  SUM(fi.quantity) AS total_quantity_sold
FROM {{ ref('fct_invoice_line_value') }} fi
JOIN {{ ref('dim_product') }} p ON fi.product_key = p.product_key
GROUP BY p.product_key, p.stock_code, p.description
ORDER BY total_quantity_sold DESC
LIMIT 10