
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

def parallel_flows(model_list , container_name):
    
  with TaskGroup("{0}_Container".format(container_name),tooltip = "Task Group for {0}".format(container_name)):        
    
    for model in model_list:   
      with TaskGroup("{0}_grp".format(model),tooltip = "Task Group for {0}".format(model)):  
       
       model_run = BashOperator(
        task_id="{0}_run".format(model ),
        bash_command= 'echo "dbt run --select {0}"'.format(model),
        #params = {"model":model_action[0] ,"model":model_action[1]    }
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
      )

       model_test = BashOperator(
        task_id="{0}_test".format(model ),
        bash_command= 'echo "dbt test --select {0}"'.format(model),
        #params = {"model":model_action[0] ,"model":model_action[1]    }
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
      )        
      
      model_run >> model_test