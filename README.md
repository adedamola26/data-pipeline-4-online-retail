# End-to-End Data Pipeline For An E-rerail Company's Customer Transaction Dataset

In this project, I build a a data pipeline that extracts data from Kaggle and prepares it, at the end of the pipeline, for creating a dashboard.
This pipeline consists of several other automated tasks including data preprocessing and running data quality checks.

## Tools used

Here are the tools employed in this project along with their respective functions:

- _Airflow_: for orchestrating the pipeline
- _Kaggle API_: for extracting the desired dataset from Kaggle
- _Astro CLI_: for setting-up Airflow and testing the DAG
- _Docker_: for developing and maintaining containers that host my Airflow instance
- _Google Cloud Storage (GCS)_: for storing the CSV file on the cloud
- _Google BigQuery(BQ)_: data warehouse/data mart for performing data analysis
- _Soda_: for running data quality checks at multiple points along the pipeline
- _dbt_: for building data models
- _Cosmos_: for integrating dbt with Airflow
- _Metabase_: for building the dashboard
- _Python_: for writing DAG script
- _SQL_: for creating new tables and running analysis in BigQuery

## Dataset

According to UCI Machine Learning Repository (main author of dataset), the dataset

> is a transactional dataset which contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.
> The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

It contains 541,909 rows and 8 features. 406,829 rows contain non-null values. The dataset contains 25,800 unique invoices with each row of the dataset representing a unique invoice line.

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

## The Workflow

The picture below shows the sequence of tasks that make up the pipeline.
![picture of the pipeline](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/figures/pipeline.png)

## ERD for source tables

![ERD](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/figures/ERD.png)

_PK_- Primary Key, _FK_- Foreign Key

**Highlights**:

- `Customer_ID` alone cannot uniquely identify each customer because several `Customer_ID`s have different countries ascribed to them. Hence, the composite PK in `Customer`.
- Several products with same `StockCode` contain different descriptions and several products with the same 'StockCode' and `Description` contain different prices. Hence, the composite PK in `Product`.

## Dimesional Model

![Dimensional Model](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/figures/Dimensional-Model.png)

`PK` - Primary Key/Surrogate Key
`NK`- Natural Key

**Highlights**:

- The fact table stores relevant details for each invoice line
- Each invoice is owned by one customer and a table join can be performed to link an invoice line to the customer

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

I arranged the first four tasks the way I did to test if there could be any improvement in DAG runtime. The result: no significant improvement noticed. Regardless, I stuck with the solution.

`download_dataset` and `create_empty_bq_dataset` can successfuly run in parallel because they are independent of each other. `create_country_table` is downstream only to `create_empty_bq_dataset` while `preprocess_date_field` is downstream only to `download_dataset`. Once the first four tasks run successfully, the next tasks get triggered, with each task being upstream to the the next task.

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

#### create_empty_bq_dataset

This task creates an empty dataset in BigQuery that stores all tables to be created.

```
create_empty_bq_dataset = BigQueryCreateEmptyDatasetOperator(
	task_id="create_empty_bq_dataset",
	dataset_id="retail",
	gcp_conn_id="gcp"
)
```

#### preprocess_date_field

The [task for loading the CSV from GCS to BigQuery](#gcs_to_bigquery) was failing because it had trouble parsing the _InvoiceDate_ column.
For this reason, I created this `PythonOperator` task that casts the column into a string and preserves the `datetime` of the invoice line. This allow the loading into BigQuery successful.

Also 43 invoices contain invoice lines with different timestamps. This is probably due to the system processing the transaction line-by-line since the difference in the timestamps for all 43 invoices is one minute.

[__See snippet below__](#putscrenshot here)

Because of this, the `dim_invoices` (defined here) had non-unique `invoice_key` surrogate keys.

To ensure unique surrogate keys in the invoice dimension, we'll set the `InvoiceDate` for each line to be the maximum datetime within that specific invoice (Here's [an alternative](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/README.md#a---alternative-to-achieve-unique-invoice_key)).

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

This task is responsible for loading the CSV into BigQuery.

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

This task uses Soda to check that the loaded data meets [these criteria](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/soda/checks/sources/raw_invoices.yml).
The task is carried out in a Soda virtual environment ([configured here](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/Dockerfile)) to avoid dependency conflicts with Airflow.

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```

The function returns a called [check function](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/soda/check_function.py).

#### create_country_table

This task executes an [SQL script](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/table/country.sql) that contains DDL statements for creating the _country_ table and DML statements for inserting data into it and altering it.

```
    create_country_table = BigQueryExecuteQueryOperator(
        task_id='create_country_table',
        sql=country_sql,
        use_legacy_sql=False,
        gcp_conn_id="gcp",
    )
```

### transform

This task creates [dbt models](https://github.com/adedamola26/data-pipeline-4-online-retail/tree/main/include/dbt/models/transform) and loads them into BigQuery.

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

This task checks that the each model meets their [respective criteria](https://github.com/adedamola26/data-pipeline-4-online-retail/tree/main/include/soda/checks/transform).

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```

Like with [check_load](#check_load), it runs in the Soda venv and returns a [check function call](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/soda/check_function.py).

### report

This task creates tables ([defined here](https://github.com/adedamola26/data-pipeline-4-online-retail/tree/main/include/dbt/models/report)) that will be used to generate plots for the Metabase dashboard.

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

This checks that each report meets its [respective criteria](https://github.com/adedamola26/data-pipeline-4-online-retail/tree/main/include/soda/checks/report).

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```

Similar to the other check tasks, it returns a call on the [check function](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/soda/check_function.py) and runs in the Soda venv.

# Result

Here's a sped-up video recording of the dag run. The first X seconds shows there's no data in the destination _include/dataset/_ folder in the root folder, no dataset in the CGS bucket and no data in the data warehouse.

The next videos show the dashboard made with the "report\_..." tables made with dbt.

# Endnote

If you've got any feedback for me, please feel free to connect with me on [LinkedIn](https://www.linkedin.com/in/adedamolade/).

Thank you for reading!

## References

Lamberti, M. (2023, August 8). Data Engineer Project: An end-to-end airflow data pipeline with BigQuery, DBT soda, and more!. YouTube. https://www.youtube.com/watch?v=DzxtCxi4YaA&t=1554s&ab_channel=DatawithMarc

Data Warehouse Fundamentals for beginners | Udemy. Data Warehouse Fundamentals for Beginners. https://www.udemy.com/course/data-warehouse-fundamentals-for-beginners/

Kimball, R., & Ross, M. (2013). The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling. John Wiley & Sons.

# Appendix

### A - Alternative To Achieve Unique `invoice_key`

We could replace the content of [dim_invoices.sql](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/dbt/models/transform/dim_invoice.sql) with the following:

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

This way, `raw_invoices` is the same as the data from the source system.
