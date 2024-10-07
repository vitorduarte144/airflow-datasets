""" 
An example DAG that uses Cosmos to render a dbt project. 
""" 

import os 
from datetime import datetime 
from pathlib import Path 
from airflow.decorators import dag
from airflow.datasets import Dataset
from cosmos import DbtDag, ProjectConfig, ProfileConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping 

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt" 
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH)) 

snowflake_countries = Dataset("snowflake://raw/public/countries")
snowflake_leagues   = Dataset("snowflake://raw/public/leagues")

profile_config = ProfileConfig( 
    profile_name="my_dbt_project", 
    target_name="dev", 
    profile_mapping=SnowflakeUserPasswordProfileMapping( 
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_example", "schema": "public"}, 
    ), 
) 

@dag(
    dag_id="cosmos_example",
    schedule=(snowflake_leagues & snowflake_countries),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def init(): 
    basic_cosmos_dag = DbtTaskGroup( 
        group_id="transform_dbt",
        project_config=ProjectConfig( 
            DBT_ROOT_PATH / "my_dbt_project", 
        ), 
        profile_config=profile_config, 
        operator_args={ 
            "install_deps": True 
        },
    ) 

    basic_cosmos_dag 

init()