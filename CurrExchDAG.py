import re
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

os.environ['AWS_ACCESS_KEY_ID'] = '<AWS_ACCESS_KEY_ID>'
os.environ['AWS_SECRET_ACCESS_KEY'] = '<AWS_SECRET_ACCESS_KEY>'
os.environ['AWS_DEFAULT_REGION'] = '<AWS_DEFAULT_REGION>'

os.environ['AIRFLOW_CONN_SNOWFLAKE_DEFAULT'] ='AIRFLOW_CONN_SNOWFLAKE_DEFAULT'


def _test_log():
    print('Inside function log')
    print(os.environ.get('AWS_ACCESS_KEY_ID'))

with DAG('CurrExchDAG',start_date=datetime(2024, 1, 1),schedule_interval='@daily',catchup=False) as dag:
    print('hello')
    # Define your tasks here using Airflow operators
    Start_Task = DummyOperator(
        task_id='Start_Task',
        dag=dag)

    check_API = HttpSensor(
        task_id = 'check_API',
        http_conn_id = 'currAPIconn',
        endpoint = 'v6/latest/USD')

    test_log = PythonOperator(
        task_id =  'test_log',
        python_callable=_test_log
    )

    invoke_lambda_task = AwsLambdaInvokeFunctionOperator(
        task_id='invoke_lambda_task',
        function_name='callCurrencyAPI',
        payload=json.dumps({"curr_from": "USD"})
    )

    sf_copy = '''COPY INTO SCD2_DB.ALL_TABLES.XX_RATES_SRC
                    FROM @SCD2_DB.external_stages.csv_input
                    pattern='currExch.*txt';'''
    
    sf_task_1 = SnowflakeOperator(
        task_id="sf_task_1",
        dag=dag,
        sql=sf_copy,
        autocommit = True
    )

    sf_task_2 = SnowflakeOperator(
        task_id="sf_task_2",
        dag=dag,
        sql="CALL SCD2_INSERT();",
        autocommit = True
    )

    sf_task_3 = SnowflakeOperator(
        task_id="sf_task_3",
        dag=dag,
        sql="CALL SCD2_UPDATE();",
        autocommit = True
    )

    End_Task = DummyOperator(
        task_id='End_Task',
        dag=dag)
    
    Start_Task >> check_API >> test_log >> invoke_lambda_task
    invoke_lambda_task >> sf_task_1 >> sf_task_2 >> sf_task_3 >> End_Task
    