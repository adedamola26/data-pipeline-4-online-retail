WITH fct_invoices_cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['invoiceno']) }} as invoice_key,
        {{ dbt_utils.generate_surrogate_key(['InvoiceDate']) }} as date_key,
        {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'UnitPrice']) }} as product_key,
        Quantity AS quantity,
        Quantity * UnitPrice AS total_price
    FROM {{ source('retail', 'raw_invoices') }}
    WHERE Quantity > 0


)
SELECT
    di.invoice_key,
    dt.date_key,
    dp.product_key,
    quantity,
    total_price
FROM fct_invoices_cte fi
INNER JOIN {{ ref('dim_datetime') }} dt ON fi.date_key = dt.date_key
INNER JOIN {{ ref('dim_product') }} dp ON fi.product_key = dp.product_key
INNER JOIN {{ ref('dim_invoice') }} di ON fi.invoice_key = di.invoice_key