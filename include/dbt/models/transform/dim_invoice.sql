with temp_1 as (
    select
        distinct
            {{ dbt_utils.generate_surrogate_key(['invoiceno']) }} as invoice_key,
            invoiceno,
            InvoiceDate,
            {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_key
    from {{ source('retail', 'raw_invoices') }} 
)
select 
    invoice_key,
    invoiceno,
    invoicedate,
    dc.customer_key
from temp_1 t
inner join {{ ref('dim_customer') }} dc on t.customer_key = dc.customer_key