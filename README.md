# End-to-End Data Pipeline For An E-rerail Company's Customer Transaction Dataset
In this project, I develop a an automated data pipeline that extracts data from Kaggle and prepares the data, at the end of the pipeline, for building a dashboard. 
Along the pipeline, I automate several other tasks including data preprocessing/transformation and running data quality checks.

## Tools used
Here are the tools used in this project and their function in the project
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

## Dataset
According to UCI Machine Learning Repository, the dataset
> is a transactional dataset which contains all the transactions occurring between 01/12/2010 and 09/12/2011 for a UK-based and registered non-store online retail.
The company mainly sells unique all-occasion gifts. Many customers of the company are wholesalers.

It contains 541,909 rows and 8 features.

### Features and their descriptions
- _InvoiceNo_: a 6-digit integral number uniquely assigned to each transaction. If this code starts with letter 'C', it indicates a cancellation
- _StockCode_: a 5-digit integral number uniquely assigned to each distinct product
- _Description_: product name
- _Quantity_: the quantities of each product (item) per transaction
- _InvoiceDate_: the day and time when each transaction was generated
- _UnitPrice_: product price per unit (£)
- _CustomerID_: a 5-digit integral number uniquely assigned to each customer
- _Country_: the name of the country where each customer resides

## Scope
This project focuses on building an automated data pipeline that extracts a dataset from Kaggle and prepares data for building a dashboard.
Data quality checks were performed at mutiple points along the pipeline. No machine learning models were trained in this project.

## The Workflow
The picture below shows the sequence of tasks that make up the pipeline.
![picture of the pipeline](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/figures/pipeline.png)

## Methodology
The following Airflow `chain` function describes the workflow of the tasks that prepare the data for reporting.
```
chain(
        download_dataset,
        preprocess_date_format,
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_bigquery,
        check_load(),
        create_country_table,
        transform,
        check_transform(),
        report,
        check_report()
    )
```
### Chain breakdown
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
# load kaggle API credentials in 'kaggle_config' variable
def _download_dataset():
  with open('/usr/local/airflow/.kaggle/kaggle.json') as f:
    kaggle_config = json.load(f)

# authenticate the Kaggle API
  api = KaggleApi()
  api.set_config_value('username', kaggle_config['username'])
  api.set_config_value('key', kaggle_config['key'])
  api.authenticate()

# download the unzipped dataset from specified source to desired location in local machine
  api.dataset_download_files(
  dataset = 'tunguz/online-retail',
  path = '/usr/local/airflow/include/dataset',
  force = True,
  unzip = True
        )
```
**PS**: `/usr/local/airflow` is the root directory of the project

#### preprocess_date_format
The pipeline was failing to load the CSV file from GCS to BigQuery because it had trouble parsing the _InvoiceDate_ column.
For this reason, I created this task that transforms the column into a string format that preserves the actual timestamp of the transactions and makes for a successful transport. The task is performed before uploading the dataset to GCS.The dataset is transformed in place.

The task, a`PythonOperator`, is shown below
```
preprocess_date_format = PythonOperator(
        task_id='preprocess_date_format',
        python_callable= _preprocess_date_format,
    )
```
Here's the callable it implements
```
def _preprocess_date_format():
  df = pd.read_csv("/usr/local/airflow/include/dataset/Online_Retail.csv", encoding='iso-8859-1')
  
  df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
  
  df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%m/%d/%Y %I:%M %p')
  
  df.to_csv("/usr/local/airflow/include/dataset/Online_Retail.csv", index=False)
```

#### upload_csv_to_gcs
This `LocalFilesystemToGCSOperator` task uploads the CSV to GCS.
```
upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_csv_to_gcs',
        src = "/usr/local/airflow/include/dataset/Online_Retail.csv",
        dst = "raw/Online_Retail.csv",
        bucket = "ade_online_retail",
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv'
    )
```
_PS_: `gcp` is the ID of the connection (made in the Airflow Web UI) between Airflow and a Google Cloud service account.
The service account has admin priviledges for BigQuery and GCS.

#### create_retail_dataset
This task creates an empty dataset in BigQuery that stores all tables to be created.
```
create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
	task_id="create_retail_dataset",
	dataset_id="retail",
	gcp_conn_id="gcp"
)
```

#### gcs_to_bigquery
This task is responsible for loading the CSV into BigQuery.
```
gcs_to_bigquery= GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        bucket='ade_online_retail',
        source_objects=['raw/Online_Retail.csv'],
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
The task is carried out in a Soda virtual environment ([configured here](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/Dockerfile)) to avoid dependency conflicts.

```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```
The function returns a called [check function](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/soda/check_function.py).


#### create_country_table
This task executes an [SQL script](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/table/country.sql) that contains DDL and DML statements for creating the _country_ table and inserting data into it respectively.
```
with open('/usr/local/airflow/include/table/country.sql', 'r') as f:
        country_sql = f.read()

    create_country_table = BigQueryExecuteQueryOperator(
        task_id='create_country_table',
        sql=country_sql,
        use_legacy_sql=False,
        gcp_conn_id="gcp",
    )
```

### transform
This task creates dbt models and loads them into BigQuery.

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
# (Insert data models)[]

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
This task creates models ([defined here](https://github.com/adedamola26/data-pipeline-4-online-retail/tree/main/include/dbt/models/report)) that will be used to generate plots for the Metabase dashboard.
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
This checks that the reports each meet their [respective predefined criteria](https://github.com/adedamola26/data-pipeline-4-online-retail/tree/main/include/soda/checks/report).
```
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
```
Similar to the check tasks, it returns a call on the [check function](https://github.com/adedamola26/data-pipeline-4-online-retail/blob/main/include/soda/check_function.py) and runs in the Soda venv. 
# Result
Here's a sped-up video recording of the dag run. The first X seconds shows there's no data in the destination _include/dataset/_ folder in the  root folder, no dataset in the CGS bucket and no data in the data warehouse.

The next videos show the dashboard made with the "report_..." tables made with dbt.

# Acknowledgement

# References
