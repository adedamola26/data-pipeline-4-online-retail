from airflow.decorators import dag, task
from datetime import datetime
import json
from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from astro import sql as aql
from astro.files import File
from airflow.models.baseoperator import chain
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from astro.dataframes.load_options import PandasLoadOptions
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig


@dag(
    start_date = datetime(2024, 1, 1),
    schedule = None,
    catchup = False,
    tags = ['retail']
)
def retail():

    def _preprocess_date_format():
        df = pd.read_csv("/usr/local/airflow/include/dataset/Online_Retail.csv", encoding='iso-8859-1')

        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')

        df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%m/%d/%Y %I:%M %p')
        
        df.to_csv("/usr/local/airflow/include/dataset/Online_Retail.csv", index=False)

    def _download_dataset():
        with open('/usr/local/airflow/.kaggle/kaggle.json') as f:
            kaggle_config = json.load(f) 

        api = KaggleApi()
        api.set_config_value('username', kaggle_config['username'])
        api.set_config_value('key', kaggle_config['key'])
        api.authenticate()
        api.dataset_download_files(
        dataset = 'tunguz/online-retail',
        path = '/usr/local/airflow/include/dataset',
        force = True,
        unzip = True
        )
    
    download_dataset = PythonOperator(
    task_id='download_dataset',
    python_callable= _download_dataset,
    )

    preprocess_date_format = PythonOperator(
        task_id='preprocess_date_format',
        python_callable= _preprocess_date_format,
    )


    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_csv_to_gcs',
        src = "/usr/local/airflow/include/dataset/Online_Retail.csv",
        dst = "raw/Online_Retail.csv",
        bucket = "ade_online_retail",
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv'
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
	task_id="create_retail_dataset",
	dataset_id="retail",
	gcp_conn_id="gcp"
)

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

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    
    with open('/usr/local/airflow/include/table/country_ddl.sql', 'r') as f:
        ddl_sql = f.read()

    create_country_table = BigQueryExecuteQueryOperator(
        task_id='create_country_table',
        sql=ddl_sql,
        use_legacy_sql=False,
        gcp_conn_id="gcp",
    )

    transform = DbtTaskGroup(
        group_id = "transform",
        project_config = DBT_PROJECT_CONFIG,
        profile_config = DBT_CONFIG,
        render_config = RenderConfig(
            load_method = LoadMode.DBT_LS,
            select = ['path:models/transform']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    

    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)
    

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
    
retail()