
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator,PythonOperator,get_current_context
from airflow.operators.bash import BashOperator
from time import sleep
from airflow.models import Variable
from airflow.models.connection import Connection
from datetime import datetime,timedelta
from airflow.exceptions import AirflowSkipException

import os
import json
import pendulum
import snowflake.connector as sc

# private_key_file = '<path>'
# private_key_file_pwd = '<password>'



with DAG(
    dag_id = 'run_query',
    description='run_query',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 10,  5),
    catchup=False,
    tags=['run_query'],
) as dag:

  conn_params = {
  	    'user' : 'MEETNANU',
       'account': 'hhvqkhk-gbb36396',
      'role': 'ACCOUNTADMIN',
      'password' : 'QAWS123qaws',
     # 'private_key_file': private_key_file,
     # 'private_key_file_pwd':private_key_file_pwd,
     # 'private_key_content' : private_key_content
      'warehouse': 'COMPUTE_WH',
      'database': 'TESTDB',
      'schema': 'STAGING'
  }


    # def transform(**kwargs):
    #   ti = kwargs["ti"]

  #hardcoded connection values
  def fetch_lastrundate(**context):

      ctx = sc.connect(**conn_params)
      cs = ctx.cursor()
      complete_fl = cs.execute("select case when lastrundate >= current_date() - 1 then 1 else 0 end as Complete_Fl from TESTDB.staging.etl_execution").fetchone()
      print('The complete_fl is {0}'.format(complete_fl[0]))
      return complete_fl[0]
  
  
  check_EDB_Completion_status = PythonOperator(
              task_id="check_EDB_Completion_status",
              python_callable=fetch_lastrundate, 
          )
  


   #using params to fetch connection values using Jinja conn accessor
  def fetch_lastrundate_using_params(**context):
      
      #convert extra string to dict
      extra_dejson = json.loads( context['conn'].get('snow_conn_default').extra)
      
      user = context['conn'].get('snow_conn_default').login
      schema = context['conn'].get('snow_conn_default').schema        
      password = context['conn'].get('snow_conn_default').password 
      role = extra_dejson['role']
      account = extra_dejson['account']      
      warehouse = extra_dejson['warehouse'] 
      database = extra_dejson['database']

      conn_params = {
  	    'user' : user,
        'account': account,
        'role': role,
        'password' : password,
        'warehouse': warehouse,
        'database': database,
        'schema': schema
  }

      #print(conn_params)
      print( context['conn'].get('snow_conn_default')  )
      print( context['conn'].get('snow_conn_default').login  )   
      print( context['conn'].get('snow_conn_default').schema  )    
      print( context['conn'].get('snow_conn_default').password  )
      print( context['conn'].get('snow_conn_default').extra )    #it's a string not dict              
      print( context['dag'].dag_id  )
      print( context['dag'].task_ids  )      

      ctx = sc.connect(**conn_params)
      cs = ctx.cursor()
      complete_fl = cs.execute("select case when lastrundate >= current_date() - 1 then 1 else 0 end as Complete_Fl from TESTDB.staging.etl_execution").fetchone()
      print('Inside task check_EDB_Completion_status_with_context . The complete_fl is {0}'.format(complete_fl[0]))

      if complete_fl[0] == 0:
         raise AirflowSkipException("EDB Nightwork not complete yet. Skipping further processing!")
      
      return complete_fl[0]
      
  
  #NOT working!
  check_EDB_Completion_status_with_params = PythonOperator(
              task_id="check_EDB_Completion_status_with_params",
              python_callable=fetch_lastrundate_using_params, 
              params = {     #DOESN'T WORK , TREATS AS LITERAL STRING VALUES INSTEAD OF JINJA RESOLVED AT PARSE TIME
                       "user": '{{ conn.snow_conn_default.login }}',   
                       'password' : '{{ conn.snow_conn_default.password }}',                       
                        'schema': '{{ conn.snow_conn_default.schema }}' ,   
                        'role': '{{ conn.snow_conn_default.role }}',                                           
                        'account': '{{ conn.snow_conn_default.extra_dejson.account }}',
                        'warehouse': '{{ conn.snow_conn_default.extra_dejson.warehouse }}',
                       'database': '{{ conn.snow_conn_default.extra_dejson.database }}'
                        }
          )



    #use jinja conn in bash
  fetch_status = BashOperator(
      task_id="fetch_status",
      bash_command="echo 'Hi, the conn info is {{ conn.snow_conn_default.conn_type }} , {{ conn.snow_conn_default.schema }} , {{ conn.snow_conn_default.extra_dejson.database }}    and EDB completion status is : {{ ti.xcom_pull(task_ids = 'check_EDB_Completion_status_with_params' , key = 'return_value') }}. '"
  )


  #   #use jinja conn in bash
  # fetch_status = PythonOperator(
  #     task_id="fetch_status",
  #      ti.xcom_pull(task_ids = 'check_EDB_Completion_status_with_params' , key = 'return_value') }}. '"
  # )

  End = EmptyOperator(
     task_id = "End"
  )

  check_EDB_Completion_status  >> check_EDB_Completion_status_with_params  >> fetch_status >> End
   