
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

def sequential_flows(model_list , container_name):
      
  int_models = []

  with TaskGroup("{0}_Container".format(container_name),tooltip = "Task Group for {0}".format(container_name)):       
    
    for model in model_list:    

      with TaskGroup("{0}_grp".format(model),tooltip = "Task Group for {0}".format(model)) as subtsk_grp:  
    
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

      int_models.append(subtsk_grp)
    
    size = len(int_models)
    pairings = [] #define pairings to establish a relation
  
    #enumerate models to determine relation pairs
    for  idx,model in enumerate(int_models):
      if idx + 1 <  size:
        first = int_models[idx] 
        second = int_models[idx + 1]

        pairings.append( tuple((first,second)) )

    tmp = pairings[0][1] #initialize tmp

    for pair in pairings:
      if pair[0] != tmp:  #check to prevent adding the node if already in dag,else will throw parsing error

        first_grp_node = pair[0]

      #indent - outside if
      second_grp_node = pair[1]

      tmp = pair[1] #store 2nd ele in the pair to compare in next iteration
      first_grp_node >> second_grp_node # connect the nodes in every iteration
      
      first_grp_node = second_grp_node #assign second to first for use as first node in the next iteration

