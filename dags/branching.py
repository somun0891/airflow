from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label

import random
from pendulum import datetime

with DAG(
    dag_id='branching_dag',
    description='A simple tutorial DAG',
    schedule = None,
    start_date=datetime(2024 ,1,1),
    tags=['branching'],
) as dag:

        run_this_first = EmptyOperator(
        task_id='run_this_first',
    )
        
        flag = 1

        options = ["= 1","= 0"]

        def branch_out(flag):
                if flag == 1:
                      return ["taskA","taskB"] 
                
                return ["taskC"]
                
        branching = BranchPythonOperator(
           task_id='branching',
           python_callable=branch_out,
           op_args=[flag]
        )

   
        run_last = EmptyOperator(
            task_id='run_last',
            trigger_rule='none_failed_min_one_success',
        )
           
        task_A = EmptyOperator(
         task_id='taskA',
        )
   
        task_B = EmptyOperator(
         task_id='taskB',
        )        
   
        task_C = EmptyOperator(
         task_id='taskC',
        )

        run_this_first >> branching   



        branching >> Label('= 1') >> task_A  >> task_B  >> run_last
        branching >> Label('= 0') >> task_C  >> run_last



                


      
                