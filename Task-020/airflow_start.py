#DAG object
from airflow import DAG
# BashOperator
from airflow.operators.bash_operator import BashOperator
# days ago function
from airflow.utils.dates import days_ago
from datetime import datetime as dt
from datetime import timedelta
import os

default_args = {
'owner' : 'airflow',
'depends_on_past' : False,
'start_date' : dt(2020, 12, 5),
'retries' : 1,
'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
'Start_hadoop',
description = 'Check if Hadoop running',
default_args = default_args,
schedule_interval = timedelta(days = 1)
)


t1 = BashOperator(
	task_id='Start_hadoop',
	bash_command='hadoop_start',
	dag=dag)
