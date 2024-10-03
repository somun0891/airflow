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
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import BranchPythonOperator,PythonOperator


#Variables

#don't use top-level code 
# refencing variables, referring to external systems , running queries as it will be parsed every 30 seconds
# var_env = Variable.get("AIRFLOW_VAR_ENV", "prod")  #bad practice
# dict_obj =  Variable.get("AIRFLOW_JSON_VARS", deserialize_json=True)
#Usage - dict_obj['my_var']

schedule = "@once"  #@daily  - timedelta(days=1)   ,@hourly - timedelta(minutes=30)

default_args = {
   "depends_on_past": False,
   "email_on_failure": False, 
   "email_on_success": False,
   "email": ['xyz@gmail.com','abc@gmail.com'],
   "email_on_retry": False, 
   "retries": 0,
   "retry_delay": duration(minutes=1),
        # 'depends_on_past': False,
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



def fetch_num(itrnum):
  print(itrnum)

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
    dag_id="dynamic_task_gen_final", #make sure to change this while copying pasting from other dags
    description = "My dag",
    start_date=datetime(2024, 9, 27),
    schedule=schedule,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,  #1 dag run is scheduled at a time    
    template_searchpath= f"{sql_path}",
    tags = ["listings"],
    dagrun_timeout=timedelta(minutes=60),
    params={"example_key": "example_value"},  
    is_paused_upon_creation = True,
) as dag:
  dag.doc_md = __doc__

  Start = EmptyOperator(task_id="Start")


#   with TaskGroup("fetch_raw_listing_data",tooltip = "Task Group for fetch_raw_listing_data") as grp:

#     fetch_raw_listing = get_listings()
#     #  SQLExecuteQueryOperator(
#     #     task_id="fetch_raw_listings", 
#     #     conn_id="snow_conn_default",
#     #     sql="fetch_raw_listings.sql",
#     #     # sql=""" USE ROLE TRANSFORM; 
#     #     #         USE WAREHOUSE COMPUTE_WH;
#     #     #         SELECT * FROM AIRBNB.RAW.RAW_LISTINGS LIMIT 10; """
#     #   )
  
#     fetch_raw_listing.doc_md = textwrap.dedent(
#           """\
#       #### fetch raw listing task
#       A simple query to fetch hotel listings from snowflake db.
#       """
#<ul>
#  <li>First item</li>
#  <li>Second item</li>
#  <li>Third item</li>
#  <li>Fourth item</li>
#</ul>
#       )   
  
#     fetch_raw_listings_params = SQLExecuteQueryOperator(
#       task_id = "fetch_raw_listings_params",
#       conn_id = "snow_conn_default",
#       sql="fetch_raw_listings_params.sql" ,
#       params = {"id" : "3176"},
#     ) 
    

#     fetch_raw_listing >> fetch_raw_listings_params    
  

  #Orchestrate Stage Models
  with TaskGroup("AUM_Staging_Models",tooltip = "Task Group for AUM Staging") as stg_grp:
    
   
    Start_AUM_Staging = EmptyOperator(task_id = "Start_AUM_Staging")  
 
    End_AUM_Staging = EmptyOperator(task_id = "End_AUM_Staging")      

    for stg in ["TF_Fund","Asset","Lkp_Static"]:
      dbt_aum_stg_gen = BashOperator(
          task_id="{0}".format(stg),
          bash_command='echo "dbt run --select {0}"'.format(stg)
      )
      Start_AUM_Staging >> dbt_aum_stg_gen >> End_AUM_Staging




  #Orchestrate Intermediate Models
  with TaskGroup("AUM_Int_Models",tooltip = "Task Group for AUM Intermediate models") as int_grp:
    
    Start_AUM_Int = EmptyOperator(task_id = "Start_AUM_Int")  
    End_AUM_Int = EmptyOperator(task_id = "End_AUM_Int")        
    
    int_models = ["Hist_Relations","Daily_MV","AUM_Pivot"]
    
    size = len(int_models)
    pairings = [] #define pairings to establish a relation
    
    #enumerate models to determine relation pairs
    for  idx,model in enumerate(int_models):
      if idx + 1 <  size:
        first = int_models[idx] 
        second = int_models[idx+1]

        pairings.append( tuple((first,second)) )
    
    #print(pairings) #prints relations as tuple
   
    tmp = pairings[1][1]

    for pair in pairings:
      if pair[0] != tmp:  #check to prevent adding the node if already in dag,else will throw parsing error

        first_node = BashOperator(
            task_id="{0}".format(pair[0]),
            bash_command='echo "dbt run --select {0}"'.format(pair[0])
        )

        Start_AUM_Int >> first_node #inside if , connect start with first node

      #indent - outside if
      second_node = BashOperator(
          task_id="{0}".format(pair[1]),
          bash_command='echo "dbt run --select {0}"'.format(pair[1])
      )

      tmp = pair[1] #store 2nd ele in the pair to compare in next iteration
      first_node >> second_node # connect the nodes in every iteration
    
      first_node = second_node #assign second to first for use as first node in the next iteration
    
    second_node >> End_AUM_Int # final node


#   select_date = SQLExecuteQueryOperator(
#     task_id = "select_date",
#     conn_id = "snow_conn_default",
#     sql="select_date.sql",
#     #params = {"env" : f"{var_env}" },    
#     #params = {"env" : var_env},        
#     params = {"env"  : Variable.get("env") }
#   )
  

#   query_status  = BashOperator(
#       task_id="query_status",
#       #bash_command='echo "{{ params.sql_path}}  executed successfully on {{ var.value.env }} "',
#       bash_command='echo " {0} executed successfully on {1} "'.format( sql_path , Variable.get("env")  )    ,      
#       #bash_command='echo "{{ macros.ds_add(ds, 7)}}"',
#       params = {"sql_path" : f"{sql_path}/fetch_raw_listings.sql" },
#       # env: Optional[Dict[str, str]] = None,  #DON'T USE THIS as it might reset the variables
#       # output_encoding: str = 'utf-8',
#       #skip_exit_code = 99,
#   )

 # connection to snowflake defined as a URI
 # AIRFLOW_CONN_SNOWFLAKE_CONN='snowflake://MEETNANU:QAWS123qaws/?account=WHB16096&region=us-east-1'

  AUM_Load = BashOperator(
          task_id="AUM_Load",
          bash_command='echo "dbt run --select AUM_Load"'
        )

  End = EmptyOperator(task_id="End", trigger_rule=TriggerRule.ALL_DONE)


  #loopy = BashOperator(task_id="Iteration_{0}".format(i) ,bash_command = 'echo "Iteration - {0}"'.format(i)   )

#   for i in range(itr):
#     Iteration = PythonOperator(
#             task_id="Iteration_{0}".format(i),
#             python_callable=fetch_num,
#             op_kwargs={"itrnum": i}, #used for passing parameters to the function
#         )



    #Start >> grp >> stg_grp >> select_date >> int_grp  >> query_status >> Iteration >> End

Start >> stg_grp  >> int_grp  >> AUM_Load >> End

