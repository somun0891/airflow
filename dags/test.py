from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label

import pendulum
from datetime import datetime

def convert(date_time):
    format = '%Y-%m-%d'    
    dateval = datetime.strptime(date_time, format)
    return dateval


dag_start_date = '2024-10-01'
dag_start_date_obj = convert(dag_start_date)

year = int(dag_start_date_obj.year)
month = int(dag_start_date_obj.month)
day = int(dag_start_date_obj.day)
tz = 'America/New_York'

schedule = None
start_date=pendulum.datetime(year , month ,day , tz=tz)
tags = ['test','branch']
dagrun_timeout_mins = 120

# print(type(start_date))
print(start_date)

with DAG(
    dag_id='test_dag',
    description='A simple tutorial DAG',
    schedule = schedule,
    start_date=start_date,
    tags=tags,
    dagrun_timeout = pendulum.duration(minutes = dagrun_timeout_mins)
) as dag:
     def f():
          pass
     
     start = EmptyOperator(task_id="start")

     end = EmptyOperator(task_id="end")     

     start >> end


