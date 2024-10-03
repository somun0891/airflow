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
from parallel_flows import parallel_flows
from sequential_flows import sequential_flows

from airflow.decorators import task 

with DAG(
    dag_id="dynamic_task_gen_v2",
    start_date = datetime(2022,10,1),
    schedule=None,
    catchup=False,
    tags = ["dynamic"]
):
  
  
  Start = EmptyOperator(task_id="Start")

  End = EmptyOperator(task_id="End") 

  AUM_Load = BashOperator(
      task_id="AUM_Load",
      bash_command='echo "dbt run --select AUM_Load"'
      # env: Optional[Dict[str, str]] = None,
      # output_encoding: str = 'utf-8',
      # skip_exit_code: int = 99,
  ) 


  stg_list = [ "TF_Fund" , "Lkp_Static" , "Historical_Holdings" ,"Asset" ]
  int_list = [ "Hist_Relationship" ,  "Daily_Mkt_Val" , "AUM_Pivot" ]
  int_models = []
  stg_container_name = "AUM_Stage"
  int_container_name = "AUM_Intermediate"  
  ext_list = [ 'Some_Parallel_Tsk_1','Some_Parallel_Tsk_2' ]
  ext_container_name = "Some_Parallel_Tasks"  

  

  with TaskGroup("AUM_Stage_Models",tooltip = "Task Group for AUM Stage") as Stg_grp:
    
    parallel_flows(stg_list ,stg_container_name )

      # for model in stg_list:   

      #   with TaskGroup("{0}_grp".format(model),tooltip = "Task Group for {0}".format(model)):  
         
      #    model_run = BashOperator(
      #     task_id="{0}_run".format(model ),
      #     bash_command= 'dbt run --select {0}'.format(model),
      #     #params = {"model":model_action[0] ,"model":model_action[1]    }
      #     # env: Optional[Dict[str, str]] = None,
      #     # output_encoding: str = 'utf-8',
      #     # skip_exit_code: int = 99,
      #   )
  
      #    model_test = BashOperator(
      #     task_id="{0}_test".format(model ),
      #     bash_command= 'dbt test --select {0}'.format(model),
      #     #params = {"model":model_action[0] ,"model":model_action[1]    }
      #     # env: Optional[Dict[str, str]] = None,
      #     # output_encoding: str = 'utf-8',
      #     # skip_exit_code: int = 99,
      #   )        
        
      #   model_run >> model_test
    

  #Sequential Workflows - Stage tasks in sequence one after the other
  with TaskGroup("AUM_Int_Models",tooltip = "Task Group for AUM Int") as int_grp:  
      
    with TaskGroup("AUM_Int_Container",tooltip = "Task Group for AUM Int") as AUM_Container_grp:  
      
      # Some_Parallel_Tsk_1 = EmptyOperator(task_id = "Some_Parallel_Tsk_1" )    
      # Some_Parallel_Tsk_2 = EmptyOperator(task_id = "Some_Parallel_Tsk_2" )

      parallel_flows(ext_list ,ext_container_name )

      sequential_flows(int_list ,int_container_name )

      # for model in int_list:    

      #   with TaskGroup("{0}_grp".format(model),tooltip = "Task Group for {0}".format(model)) as subtsk_grp:  
      
      #    model_run = BashOperator(
      #     task_id="{0}_run".format(model ),
      #     bash_command= 'dbt run --select {0}'.format(model),
      #     #params = {"model":model_action[0] ,"model":model_action[1]    }
      #     # env: Optional[Dict[str, str]] = None,
      #     # output_encoding: str = 'utf-8',
      #     # skip_exit_code: int = 99,
      #   )
  
      #    model_test = BashOperator(
      #     task_id="{0}_test".format(model ),
      #     bash_command= 'dbt test --select {0}'.format(model),
      #     #params = {"model":model_action[0] ,"model":model_action[1]    }
      #     # env: Optional[Dict[str, str]] = None,
      #     # output_encoding: str = 'utf-8',
      #     # skip_exit_code: int = 99,
      #   )        
        
      #   model_run >> model_test

      #   int_models.append(subtsk_grp)
      
      # size = len(int_models)
      # pairings = [] #define pairings to establish a relation
    
      # #enumerate models to determine relation pairs
      # for  idx,model in enumerate(int_models):
      #   if idx + 1 <  size:
      #     first = int_models[idx] 
      #     second = int_models[idx + 1]

      #     pairings.append( tuple((first,second)) )
  
      # tmp = pairings[0][1] #initialize tmp

      # for pair in pairings:
      #   if pair[0] != tmp:  #check to prevent adding the node if already in dag,else will throw parsing error
  
      #     first_grp_node = pair[0]

      #   #indent - outside if
      #   second_grp_node = pair[1]
  
      #   tmp = pair[1] #store 2nd ele in the pair to compare in next iteration
      #   first_grp_node >> second_grp_node # connect the nodes in every iteration
        
      #   first_grp_node = second_grp_node #assign second to first for use as first node in the next iteration


    
Start >> Stg_grp >> int_grp >> AUM_Load >> End

