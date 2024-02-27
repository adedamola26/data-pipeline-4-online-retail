# End-to-End Data Pipeline For An E-rerail Company's Customer Transaction Dataset

In this project, I built a a data pipeline that extracts data from Kaggle and prepares it, at the end of the pipeline, for creating a dashboard.
This pipeline consists of several other automated tasks including data preprocessing and running data quality checks.

## Workflow

The picture below shows the relationships between the tasks that make up the pipeline.
![pipeline](https://github.com/adedamola26/data-pipeline-4-online-retail/assets/122896944/44fb8c23-2615-4e82-9f03-e6e20b3b0aa2)


## Result

You can skim the [description of the dataset](#dataset) and/or the [ERD](#erd) before watching the video for a better follow-along. 

https://github.com/adedamola26/data-pipeline-4-online-retail/assets/122896944/8232c1af-ae46-4137-8d0b-c3af0d277d87
## Tools used
Here are the tools employed in this project along with their respective functions:

- _Airflow_: for orchestrating the pipeline
- _Kaggle API_: for extracting the desired dataset from Kaggle
- _Astro CLI_: for setting-up Airflow and testing the DAG
- _Docker_: for developing and maintaining containers that host my Airflow instance
- _Google Cloud Storage (GCS)_: for storing the CSV file on the cloud
- _Google BigQuery(BQ)_: data warehouse/data mart for performing data analysis
- _Soda_: for running data quality checks at multiple points along the pipeline
- _dbt_: for implementing [dimensional model](#dimensional-model)
- _Cosmos_: for integrating dbt with Airflow
- _Metabase_: for building the dashboard
- _Python_: for writing DAG script
- _SQL_: for creating new tables and running analysis in BigQuery

## Dataset

According to [its author](#references), the dataset is

> is a transnational dataset which contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.
> The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

It contains 541,909 rows and 8 features. 135,080 rows contain a null value or more. The dataset contains 25,800 unique invoices with each row of the dataset representing a unique invoice line.

### Features and their descriptions

_InvoiceNo_

> a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'C', it indicates a cancellation

_StockCode_

> a 5-digit integral number uniquely assigned to each distinct product

_Description_

> product name

_Quantity_

> the quantities of each product (item) per transaction

_InvoiceDate_

> the day and time when each transaction was generated

_UnitPrice_

> product price per unit (£)

_CustomerID_

> a 5-digit integral number uniquely assigned to each customer

_Country_

> the name of the country where each customer resides

## ERD
![ERD](https://github.com/adedamola26/data-pipeline-4-online-retail/assets/122896944/0bd9898a-bf89-4437-b1a7-778cb77741ab)

__PK__- Primary Key

__FK__- Foreign Key

_PS_: The above ERD was not implemented. It was simply designed to give a sense of what the source system would look like if it were normalized to 3NF. 

**Highlights**:

- _Customer_ID_ alone cannot uniquely identify each _Customer_ because several _Customer_ID_s have multiple combinations with different countries. Hence, the composite _PK_ in _Customer_.
- For the _Product_ entity, _StockCode_ is not a candidate key because several products with the same _StockCode_ contain different descriptions and/or prices. Likewise, a combination of _StockCode_ and _Description_ is not a candidate key because several other products with the same _StockCode_ and _Description_ contain different prices. Hence, the composite _PK_.

## Dimensional Model
![Dimensional-Model](https://github.com/adedamola26/data-pipeline-4-online-retail/assets/122896944/7c9a58c5-f95d-494e-aa61-a4a0cb278909)

__PK__- Primary Key/Surrogate Key

__NK__- Natural Key

**Highlights**:

- The fact table stores the total price and quantity recorded on each invoice line.
- Each invoice is owned by one customer and a table join can be performed to link an invoice line to the customer.

## Methodology

The following Airflow `chain` function describes the workflow of the tasks that prepare the data for reporting.

```
chain(
	[download_dataset, create_empty_bq_dataset],
	[preprocess_date_field, create_country_table],
	upload_csv_to_gcs,
	gcs_to_bigquery,
	check_load(),
        transform,
        check_transform(),
        report,
        check_report()
    )
```

### Chain breakdown

I arranged the first four tasks the way I did to test if there could be any improvement in the DAG's runtime. The result: no significant improvement noticed. Nevertheless, I stuck with the solution.

`download_dataset` and `create_empty_bq_dataset` can successfuly run in parallel because they are independent of each other. `create_country_table` is downstream only to `create_empty_bq_dataset` while `preprocess_date_field` is downstream only to `download_dataset`. Once the first four tasks run successfully, `upload_csv_to_gcs` gets queued and each subsequent task is downstream to the previous one.

To learn more about the function of each task, keep reading.

#### download_dataset

This task is a `PythonOperator` that downloads the dataset from Kaggle to my local machine.

```
download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable= _download_dataset,
    )
```

Here's the callable that the `PythonOperator` implements.

```
def _download_dataset():
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate() # Kaggle API is set in .env file
    api.dataset_download_files(
    dataset = 'tunguz/online-retail',
    path = '/usr/local/airflow/include/dataset',
    force = True,
    unzip = True
    )
```
`KaggleApi` was locally imported to [reduce the DAG's loading time](#references).

#### preprocess_date_field

The [task for loading the CSV from GCS to BigQuery](#gcs_to_bigquery) kept returning an error that concerned a problem with parsing the _InvoiceDate_ column.
For this reason, I created this `PythonOperator` task that converts the column's datatype into a string while preserving the `datetime` of the invoice line. This made the loading process into BigQuery successful.

Also, 43 invoices contain invoice lines with different timestamps. This may be due to the way the company's system processes each invoice (one line at a time) since the difference in the timestamps for all 43 invoices is just one minute.

_Here's a snippet of a `groupby` function call on 'InvoiceDate' and 'InvoiceNo'_

![invoicedate-timestamp-min-diff](https://github.com/adedamola26/data-pipeline-4-online-retail/assets/122896944/7de2444f-39d2-498d-b3db-6997ac89ec7f)

Because of this, processing [_dim_invoices_](include/dbt/models/transform/dim_product.sql) results in non-unique _invoice_key_ surrogate keys.

To ensure unique surrogate keys in the invoice dimension, we'll set the _InvoiceDate_ for each line to be the maximum datetime within that specific invoice (here's [an alternative](#a---alternative-to-achieve-unique-invoice_key)).

Here's the callable that implements this task

```
def _preprocess_date_field():
	import pandas as pd
	df = pd.read_csv('/usr/local/airflow/include/dataset/Online_Retail.csv', encoding='iso-8859-1')

	df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

	"""
	43 invoices have invoice lines with different timestamps.
	The difference in the time stamps is one minute.
	We will take the maximum timestamp for each invoice.
	"""

	df['InvoiceDate'] = df.groupby('InvoiceNo')['InvoiceDate'].transform('max')

	df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%m/%d/%Y %I:%M %p')

	df.to_csv('/usr/local/airflow/include/dataset/new_online_retail.csv', index=False)
```

This task is performed before uploading the dataset to GCS.

#### create_empty_bq_dataset

This task creates an empty dataset in BigQuery that stores all the tables we'll load into BigQuery.

```
create_empty_bq_dataset = BigQueryCreateEmptyDatasetOperator(
	task_id="create_empty_bq_dataset",
	dataset_id="retail",
	gcp_conn_id="gcp"
)
```

#### create_country_table
Completing this task loads a new (_country_) table into BigQuery. It runs an [SQL script](include/table/country.sql) (containing DDL and DML statements) that creates, inserts data into and alters the _country_ table (containing ISO information that was used to plot a geospatial heatmap). The creation and insertion statements were sourced [externally](#references). The DML statements were included because I wanted to delete two columns and rename another without doing the work manually. 

```
create_country_table = BigQueryExecuteQueryOperator(
        task_id='create_country_table',
        sql=read_country_sql(),
        use_legacy_sql=False,
        gcp_conn_id="gcp",
    )
```
Here's the function it calls.

```
def read_country_sql():
	with open('/usr/local/airflow/include/table/country.sql', 'r') as f:
	    return f.read()
```
_I doubt this task would ideally be included in a pipeline. Nevertheless, it was included so I could get my hands dirty with as many diverse Operators as I could._

#### upload_csv_to_gcs

This `LocalFilesystemToGCSOperator` task uploads the CSV to GCS.

```
upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_csv_to_gcs',
        src = "/usr/local/airflow/include/dataset/new_online_retail.csv",
        dst = "raw/new_online_retail.csv",
        bucket = "ade_online_retail",
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv'
    )
```

_PS_: `gcp` is the ID of the connection (configured in the Airflow Web UI) between Airflow and a Google Cloud service account.
The service account has admin priviledges for BigQuery and GCS.

#### gcs_to_bigquery

This task is responsible for loading the CSV from GCS into BigQuery.

```
gcs_to_bigquery= GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        bucket='ade_online_retail',
        source_objects=['raw/new_online_retail.csv'],
        destination_project_dataset_table='online-retail-dp.retail.raw_invoices',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id="gcp",
        encoding='iso-8859-1'
    )
```

#### check_load

This task uses Soda to check that the tables loaded into BigQuery, up till this point, meet [these criteria](include/soda/checks/sources).
The task is carried out in a Soda virtual environment ([configured here](Dockerfile)) to avoid dependency conflicts with Airflow.

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```

The function returns a called [check function](include/soda/check_function.py).

#### transform

This task is handled by dbt and implements the [dimensional model](#dimensional-model) and loads [the tables](include/dbt/models/transform) into BigQuery.

```
transform = DbtTaskGroup(
        group_id = "transform",
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select = ['path:models/transform']
        )
    )
```

#### check_transform

This task checks that the each model meets their [respective criteria](include/soda/checks/transform).

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```

Like with [check_load](#check_load), it runs in the Soda _venv_ and returns a [check function call](include/soda/check_function.py).

#### report

This task creates tables ([defined here](include/dbt/models/report)) that will be used to generate plots for the dashboard.

```
report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )
```

#### check_report

This checks that each report meets its [respective criteria](include/soda/checks/report).

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```

Similar to the other check tasks, it returns a call on the [check function](include/soda/check_function.py) and runs in the Soda _venv_.

# Endnote

If you've got any feedback for me, please feel free to connect with me on [LinkedIn](https://www.linkedin.com/in/adedamolade/).

Thank you for reading!

## References

Online retail. (2021, April 12). Kaggle. https://www.kaggle.com/datasets/tunguz/online-retail

Best Practices — Airflow documentation. http://apache-airflow-docs.s3-website.eu-central-1.amazonaws.com/docs/apache-airflow/stable/best-practices.html#top-level-python-code

Lamberti, M. (2023, August 8). Data Engineer Project: An end-to-end airflow data pipeline with BigQuery, DBT soda, and more!. YouTube. https://www.youtube.com/watch?v=DzxtCxi4YaA&t=1556s&ab_channel=DatawithMarc

Data Warehouse Fundamentals for beginners | Udemy. Data Warehouse Fundamentals for Beginners. https://www.udemy.com/course/data-warehouse-fundamentals-for-beginners/

Kimball, R., & Ross, M. (2013). The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling. John Wiley & Sons.

# Appendix

### A - Alternative To Achieve Unique `invoice_key`

As [earlier stated](#preprocess_date_field), we could replace the content of [_dim_invoices.sql_](include/dbt/models/transform/dim_invoice.sql) with the following:

```
with temp_1 as (
    select
        {{ dbt_utils.generate_surrogate_key(['invoiceno']) }} as invoice_key,
        invoiceno,
        InvoiceDate,
	PARSE_DATETIME('%m/%d/%Y %I:%M %p', InvoiceDate) AS date_part,
        {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_key,
        row_number() over (partition by invoiceno order by date_part desc) as row_num
    from {{ source('retail', 'raw_invoices') }}
)
select
    invoice_key,
    invoiceno,
    invoicedate,
    dc.customer_key
from temp_1 t
inner join {{ ref('dim_customer') }} dc on t.customer_key = dc.customer_key
where t.row_num = 1;
```

This way, except the change made to _InvoiceDate_'s datatype, _raw_invoices_ will contain the same data as the dataset from the source system.
