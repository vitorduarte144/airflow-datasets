from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.datasets import Dataset
from astro.files import File
from astro.constants import FileType
from astro import sql as aql
from astro.sql.table import Table, Metadata


s3_folder_countries = Dataset("s3://vd-airflow-countries-aaa/countries/")
s3_folder_leagues   = Dataset("s3://vd-airflow-leagues-aaa/leagues/")
snowflake_countries = Dataset("snowflake://raw/public/countries")
snowflake_leagues   = Dataset("snowflake://raw/public/leagues")


@dag(
    dag_id="consumer_astro_python_sdk",
    schedule=(s3_folder_countries & s3_folder_leagues),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Vitor Duarte",
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    }
)
def init():

    start_task = EmptyOperator(
        task_id="starting"
    )
    
    load_countries = aql.load_file(
        task_id='load_countries_into_snowflake',
        input_file=File(path="s3://vd-airflow-countries-aaa/countries/", filetype=FileType.JSON, conn_id="aws_conn_vd"),
        output_table=Table(
            name="countries", 
            conn_id="snowflake_conn", 
            metadata=Metadata(
                database="raw",
                schema="public"
            )
        ),
        if_exists="replace",
        use_native_support=True,
        outlets=[snowflake_countries]
    )

    load_leagues = aql.load_file(
        task_id='load_leagues_into_snowflake',
        input_file=File(path="s3://vd-airflow-leagues-aaa/leagues/", filetype=FileType.JSON, conn_id="aws_conn_vd"),
        output_table=Table(
            name="leagues", 
            conn_id="snowflake_conn",
                metadata=Metadata(
                database="raw",
                schema="public"
            )
        ),
        if_exists="replace",
        use_native_support=True,
        outlets=[snowflake_leagues]
    )

    end_task = EmptyOperator(
        task_id="finishing"
    )
    
    start_task >> [load_countries, load_leagues] >> end_task


init()