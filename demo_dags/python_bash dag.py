from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

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
    dag_id="python_bash_dag",
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    description="python bash demo dag",
    max_active_runs=1,
    tags=['demo'],
    default_args=default_args
)
def hello_world():

    @task
    def hello_python():
        print("Hello from Python!")
    
    hello_bash = BashOperator(
        task_id="hello_bash",
        bash_command="echo 'Hello from Bash!'"
    )

    # Define task dependencies
    hello_python() >> hello_bash

# Instantiate DAG
hello_world()
