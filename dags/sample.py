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


def say_hi(**context):
    print("hi elyse")


with dag:
    sensing_task = FileSensor(
        task_id='sensing_task',
        filepath='test.txt',
        fs_conn_id='my_file_system2',
        poke_interval=10
    )
    task1 = PythonOperator(
        task_id='say_hi',
        python_callable=say_hi,
        provide_context=True,
        retries=10,
        retry_delay=timedelta(seconds=1)
    )

    sensing_task >> task1
