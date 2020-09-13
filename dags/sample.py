import os
from airflow.contrib.hooks.fs_hook import FSHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta

args = {
    'owner': 'Elyse',
    'start_date': days_ago(1)
}

dag = DAG(dag_id='my_dag', default_args=args, schedule_interval=None)


def print_file_content(**context):
    hook = FSHook('my_file_system2')
    base_path = hook.get_path()
    path = os.path.join(base_path, 'test.txt')
    with open(path, 'r') as fp:
        print(fp.read())
    os.remove(path)


with dag:
    sensing_task = FileSensor(
        task_id='sensing_task',
        filepath='test.txt',
        fs_conn_id='my_file_system2',
        poke_interval=10
    )
    read_file_content_task = PythonOperator(
        task_id='say_hi',
        python_callable=print_file_content,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

    sensing_task >> read_file_content_task
