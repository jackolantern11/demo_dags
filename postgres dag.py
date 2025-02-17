from datetime import datetime, timedelta
import pendulum
import logging
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define default arguments
default_args = {
    'owner': 'zfreeze',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition using @dag decorator
@dag(
    start_date=datetime(2025, 2, 14, 14, 0, 0, tzinfo=pendulum.timezone("US/Central")),
    end_date=None,
    schedule='1 * * * *',
    dag_id="postgres_dag",
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    description="postgres demo dag",
    max_active_runs=1,
    tags=['demo'],
    default_args=default_args
)
def hello_postgres():

    @task
    def hello():
        
        # Postgres Connection:
        conn = PostgresHook(postgres_conn_id='AIRFLOW_CONN_POSTGRES_DEFAULT')

        # SQL Query:
        sql: str = "select * from test_tbl.test_a;"

        # Query Response:
        return_data = conn.get_first(sql=sql)

        logging.info(f"{return_data=}")
    

    # Define task dependencies
    hello()

# Instantiate DAG
hello_postgres()
