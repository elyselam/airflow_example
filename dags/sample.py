from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import random

args = {
    'owner':'Elyse',
    'start_date':days_ago(1)
}

dag = DAG(dag_id='my_dag', default_args=args,schedule_interval=None)

def run_this_func(**context):
    print("hi elyse")

def random_fail(**context):
    if random.random() > .7:
        raise Exception('Exceptionnnnnn')
    print('not failed')

with dag:
    run_this_task = PythonOperator(
        task_id = 'run_this',
        python_callable=random_fail,
        provide_context=True
    )
    run_this_task2 = PythonOperator(
        task_id = 'run_this2',
        python_callable=random_fail,
        provide_context=True
        retries = 10,
        retry_delay=timedelta(seconds=1)
    )
    run_this_task >> run_this_task2