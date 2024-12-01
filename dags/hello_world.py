from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

current_date = datetime.now()
year = current_date.year
month = current_date.month
day = current_date.day

start_date = datetime(year, month, day)

default_args = {
    'owner': 'datamasterylab.com',
    'start_date': start_date,
    'catchup': False
}

dag = DAG(
    'hello_world',
    default_args = default_args,
    schedule=timedelta(days=1)
)

t1 = BashOperator(
    task_id = 'hello_world',
    bash_command='echo "Hello World"',
    dag = dag
)

t2 = BashOperator(
    task_id = 'goodbye_friend',
    bash_command='echo "Goodbye Friend"',
    dag = dag
)

t1 >> t2