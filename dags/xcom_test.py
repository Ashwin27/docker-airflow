import airflow

from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from datacleaner import data_cleaner

yesterdays_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'owner': 'Airflow',
    'retries' : 1,
}

def push_function(**kwargs):
    message = "Here is your data"
    ti = kwargs['ti']
    ti.xcom_push(key='message', value=message)

def pull_function(**kwargs):
    ti = kwargs['ti']
    message = ti.xcom_pull(key='message')
    print("Pulled Message: " + message)

with DAG('xcom_test', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    t1 = PythonOperator(task_id='push_event', python_callable=push_function, provide_context=True)
    t2 = PythonOperator(task_id='pull_event', python_callable=pull_function, provide_context=True)

    t1 >> t2