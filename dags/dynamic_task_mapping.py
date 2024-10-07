from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.datasets import Dataset
from airflow.models import Variable
from datetime import datetime, timedelta


INGEST_INFO: dict = Variable.get(key='ingest_json_api', deserialize_json=True)
s3_folder_countries = Dataset("s3://vd-airflow-countries-aaa/countries/")
s3_folder_leagues = Dataset("s3://vd-airflow-leagues-aaa/leagues/")

@dag(
    dag_id="ingestion_s3_basic_dtm",
    schedule_interval=None,
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

    start_t0 = EmptyOperator(
        task_id="starting_ingestion"
    )

    @task_group(group_id='countries_endpoint_and_s3')
    def tg_countries():

        @task
        def get_files():

            import requests
            import json
            from time import sleep

            url: str = fr"https://{INGEST_INFO['host']}/countries"

            payload={}
            headers = {
            'x-rapidapi-key': f"{INGEST_INFO['api_key']}",
            'x-rapidapi-host': f"{INGEST_INFO['host']}"
            }

            response = requests.request("GET", url=url, headers=headers, data=payload).json()

            clean_list: list = [ 
                    { k:v for k,v in countries_dict.items() if k in ('name', 'code') } 
                    for countries_dict in response['response']
                    ]

            step: int = 10
            return_list: list = []

            for i in range(0, len(clean_list), step):
            

                if not len(clean_list) < i + step:

                    file_name = f'countries_{i}_{i+step-1}.json' 
                    file_content = json.dumps(clean_list[i:i+step-1]) 

                else:
                    file_name = f'countries_{i}_{len(clean_list)}.json' 
                    file_content = json.dumps(clean_list[i:len(clean_list)]) 

                    # forcing an error
                    # file_content = {"Error": 404}
                
                return_list.append([file_name, file_content])
            
            return return_list


        @task
        def load_files_into_s3(file_list):
            from time import sleep


            bucket_name: str = f"{INGEST_INFO['bucket_name']['countries']}"
            sleep(1)

            s3 = S3Hook(aws_conn_id='aws_conn_vd')

            s3.load_string(string_data=file_list[1],
                           bucket_name=bucket_name, 
                           key=fr"countries/{file_list[0]}",
                           replace=True) 
            
        countries_finished = EmptyOperator(
            task_id="end_countries",
            outlets=[s3_folder_countries]
        )

        get_files = get_files()
        load_files_into_s3 = load_files_into_s3.expand(file_list=get_files)

        get_files >> load_files_into_s3 >> countries_finished


    @task_group(group_id='leagues_endpoint_and_s3')
    def tg_leagues():
        
        @task
        def get_files():
            import requests
            import json

            url: str = fr"https://{INGEST_INFO['host']}/leagues"

            payload={}
            headers = {
            'x-rapidapi-key': f"{INGEST_INFO['api_key']}",
            'x-rapidapi-host': f"{INGEST_INFO['host']}"
            }

            response = requests.request("GET", url=url, headers=headers, data=payload).json()

            clean_list: list = []
            for i in response['response']:
                my_dict: dict = {}
                my_dict['id'] = i['league']['id']
                my_dict['name'] = i['league']['name']
                my_dict['country_name'] = i['country']['name']
                my_dict['country_code'] = i['country']['code']
                clean_list.append(my_dict)

            file_content: str = json.dumps(clean_list)

            return file_content

        
        @task(outlets=[s3_folder_leagues])
        def load_files_into_s3(file_content):

            bucket_name: str = f"{INGEST_INFO['bucket_name']['leagues']}"

            s3 = S3Hook(aws_conn_id='aws_conn_vd')

            s3.load_string(string_data=file_content,
                           bucket_name=bucket_name, 
                           key="leagues/leagues.json",
                           replace=True)
            

        get_files = get_files()
        load_files_into_s3 = load_files_into_s3(get_files)
        get_files >> load_files_into_s3



    end_process = EmptyOperator(
        task_id='end_process'
    )


    start_t0 >> [tg_countries(), tg_leagues()] >> end_process
    

init() 