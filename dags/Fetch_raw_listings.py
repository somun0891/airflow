from __future__ import annotations
import textwrap
import json
import os
import datetime
from datetime import datetime , timedelta 
from pendulum import datetime , duration

from airflow.models.dag import DAG 
from airflow.models import Variable 
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator,PythonOperator,get_current_context
from time import sleep

#Variables

#don't use top-level code 
# refencing variables, referring to external systems , running queries as it will be parsed every 30 seconds
var_env = Variable.get("AIRFLOW_VAR_ENV", "prod")  #bad practice
schedule = "@once"  #@daily  - timedelta(days=1)   ,@hourly - timedelta(minutes=30)

default_args = {
   "depends_on_past": False,
   "email_on_failure": False, 
   "email_on_success": False,
   "email_on_retry": False, 
   "retries": 0,
   "retry_delay": duration(minutes=1),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
}


sql_path = "/home/ubuntu/airflow/include/Scripts/sql"

itr = 3

#print("hi")

def fetch_num(itrnum):
  print(itrnum)

def sleeping():
  print("inside sleeping")
  sleep(60 * 2) #2 minutes

def get_listings():
  
  #print("inside get_listings")
  listings = SQLExecuteQueryOperator(
      task_id="fetch_raw_listings", 
      conn_id="snow_conn_default",
      sql="fetch_raw_listings.sql",
      # sql=""" USE ROLE TRANSFORM; 
      #         USE WAREHOUSE COMPUTE_WH;
      #         SELECT * FROM AIRBNB.RAW.RAW_LISTINGS LIMIT 10; """
    )
  return listings

with DAG(
    dag_id="Fetch_raw_listings",
    description = "Used to fetch and load raw listing data to snowflake " ,
    start_date=datetime(2024, 9, 27),
    schedule=schedule,
    default_args=default_args,
    catchup=False,
    template_searchpath= f"{sql_path}",
    tags = ["listings"],
) as dag:
  dag.doc_md = __doc__



  Start = EmptyOperator(task_id="Start")

  fetch_raw_listing = get_listings()
  #  SQLExecuteQueryOperator(
  #     task_id="fetch_raw_listings", 
  #     conn_id="snow_conn_default",
  #     sql="fetch_raw_listings.sql",
  #     # sql=""" USE ROLE TRANSFORM; 
  #     #         USE WAREHOUSE COMPUTE_WH;
  #     #         SELECT * FROM AIRBNB.RAW.RAW_LISTINGS LIMIT 10; """
  #   )

  fetch_raw_listing.doc_md = textwrap.dedent(
        """\
    #### fetch raw listing task
    A simple query to fetch hotel listings from snowflake db.
    """
    )   

  fetch_raw_listings_params = SQLExecuteQueryOperator(
    task_id = "fetch_raw_listings_params",
    conn_id = "snow_conn_default",
    sql="fetch_raw_listings_params.sql" ,
    params = {"id" : "3176"},
  )   

  
  fetch_raw_listings_params.doc_md = textwrap.dedent(
        """\
    #### fetch raw listing task
    ##### A simple query to ***fetch hotel listings*** from snowflake db with params.
    ---
    #### This task runs and tests these models as part of the group
    1. First model
    2. Second model
    3. Third model
    4. Fourth model
    ---
    ![Tux, the Linux mascot](../docs/assets/images/tux.jpg "dbt lineage graph")
    
    """
    )     

  select_date = SQLExecuteQueryOperator(
    task_id = "select_date",
    conn_id = "snow_conn_default",
    sql="select_date.sql",
    #params = {"env" : f"{var_env}" },    
    #params = {"env" : var_env},        
    params = {"env"  : Variable.get("env") }
  )
  

  query_status  = BashOperator(
      task_id="query_status",
      #bash_command='echo "{{ params.sql_path}}  executed successfully on {{ var.value.env }} "',
      bash_command='echo " {0} executed successfully on {1} "'.format( sql_path , Variable.get("env")  )    ,      
      #bash_command='echo "{{ macros.ds_add(ds, 7)}}"',
      params = {"sql_path" : f"{sql_path}/fetch_raw_listings.sql" },
      # env: Optional[Dict[str, str]] = None,  #DON'T USE THIS as it might reset the variables
      # output_encoding: str = 'utf-8',
      #skip_exit_code = 99,
  )

 # connection to snowflake defined as a URI
 # AIRFLOW_CONN_SNOWFLAKE_CONN='snowflake://MEETNANU:QAWS123qaws/?account=WHB16096&region=us-east-1'

  End = EmptyOperator(task_id="End", trigger_rule=TriggerRule.ALL_DONE)

  #loopy = BashOperator(task_id="Iteration_{0}".format(i) ,bash_command = 'echo "Iteration - {0}"'.format(i)   )

  for i in range(itr):
    Iteration = PythonOperator(
            task_id="Iteration_{0}".format(i),
            python_callable=fetch_num, #sleeping
            op_kwargs={"itrnum": i}, #used for passing parameters to the function
        )
 


    Start >> fetch_raw_listing >> fetch_raw_listings_params >> select_date >> query_status >> Iteration >> End

