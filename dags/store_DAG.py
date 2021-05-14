from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator

from datacleaner import data_cleaner

yesterdays_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

default_args = {
    'start_date': datetime(2021, 5, 1),
    'owner': 'Airflow',
    'retries' : 1,
    'retry_delta' : timedelta(seconds=5)
}

with DAG('store_dag', default_args=default_args, schedule_interval='@daily', template_searchpath=['/usr/local/airflow/sql_files'], catchup=False) as dag:
    t1 = BashOperator(task_id='check_file_exists', bash_command='shasum /usr/local/airflow/store_files_airflow/raw_store_transactions.csv', retries=2, retry_delay=timedelta(seconds=15))
    t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)
    t3 = MySqlOperator(task_id='create_mysql_table', sql='create_table.sql', mysql_conn_id='mysql_conn')
    t4 = MySqlOperator(task_id='insert_into_table', sql='insert_into_table.sql', mysql_conn_id='mysql_conn')
    t5 = MySqlOperator(task_id='select_from_table', sql='select_from_table.sql', mysql_conn_id='mysql_conn')
    t6 = BashOperator(task_id='move_location_profits', bash_command='mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv' % yesterdays_date)
    t7 = BashOperator(task_id='move_store_profits', bash_command='mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv' % yesterdays_date)

    t1 >> t2 >> t3 >> t4 >> t5 >> [t6, t7]