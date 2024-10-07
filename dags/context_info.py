from pprint import pprint
import pendulum
from datetime import datetime,timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable



start_date = abs(-1)
tags=['context']
schedule = '0 3 * * *'  #'@daily'  #timedelta(minutes=30) #None


def dur(intv):
    duration = int(intv.split(" ")[0])
    intr = str(intv.split(" ")[1])
    factor = -1 if str(intv.split(" ")[2]) == 'ago' else 1 
    #print(duration,intr,factor)   

    if intr == 'hour' or intr == 'hours':
         return timedelta(hours = duration )
    elif intr == 'minute' or intr == 'minutes':
         return timedelta(minutes = duration )
    elif intr == 'second' or intr == 'seconds':
         return timedelta(seconds = duration )
    elif intr == 'millisecond' or intr == 'milliseconds':
         return timedelta(milliseconds = duration )    
    else:
         return timedelta(days = duration )
    
#interval("10 millisecond ago") 

dur1 = dur('2 hours ago')
print(dur1)

with DAG(
    dag_id='context_dag',
    description='A simple tutorial DAG',
    schedule = schedule,
    start_date= datetime.now() + dur('2 hours ago'),
    tags=tags,
    catchup = False
) as dag:
   
   #using context kwargs and push data as xcom object
   #return_value can be used as  key to xcom_pull in the consumer task
   def print_context(**context):
    # pprint(context['dag_id'])  #CONTEXT VALUES - Scheduling keys -  dag_run, conf , var , params 
     pprint(f"dag run info: { context['dag_run'] } " )
     pprint(f"dag name: { context['dag'].dag_id } " )
     pprint(f"dag taskids: { context['dag'].task_ids } " )        
     pprint(f"dag run object type: { type(context['dag_run']) } " )

     #pprint(f"airflow cfg info: {context['conf'].as_dict()} "  )
     pprint(f" dag folder location: { context['conf'].get(section='core',key='dags_folder') } " )  

     pprint(f" current env: {  context['var']['value'].get('env') }"   )  

     pprint(f"params used: {context['params']} ")

     pprint(f" task instance details: { context['ti'] } ")

     context['ti'].xcom_push(key = 'env' , value = 'prod')  #send data as xcom object
     return 'success' #return_value can be used as  key to xcom_pull in the consumer task


    #PASS params dict
   print_context_tsk = PythonOperator(
      task_id="print_context",
      python_callable=print_context,
      params = {"company" : "GI"}
   )  

    #using context values task instance object to reference xcom object
    #using context values params to access params dict
   def greet_user(**context):
      val1 = context['ti'].xcom_pull(task_ids = "print_context" , key = "env") #use xcom data
      val2 = context['ti'].xcom_pull(task_ids = "print_context" , key = "return_value")    
      greet = context['params']['greeting']
      user = context['params']['user']
      ts = context['ts']
      

      msg =  f"{greet}  {user} ! The logical date is {ts} . The print_context task returned  env = {val1}  and status = {val2}." 
      print(msg)

  
   greet_user = PythonOperator(
      task_id="greet_user",
      python_callable = greet_user,
     # provide_context = True, #deprecated option
      params = {  #can be used in function scope if used as context values
                    "greeting": "hello" ,  
                    "user" : "sachi" 
               },
   ) 

  #using  op_kwargs to pass parameters to callable function and template_dict to pass jinja template values
   def get_env(**kwargs):
      env = kwargs.get('env')
      execution_date = kwargs.get('execution_date')
      print(f"The runtimeis set to {env} and the logical date of the dag run is {execution_date} UTC")

   get_env = PythonOperator(
      task_id="get_env",
      python_callable = get_env,
      #provide_context = True, #deprecated option
      op_kwargs={'env': Variable.get('env' , 'prod')},  #get from variable else fallback to prod env
      templates_dict={'execution_date': '{{ ds }}'}      
   )


     #using params in a bash command
   welcome_user = BashOperator(
      task_id="welcome_user",
      bash_command = 'echo "{{ params.greeting }}  {{ params.user }}!. "   ', #only string , don't pass functions
      skip_on_exit_code = 99 ,
      params = {
                    "greeting": "Welcome" , 
                    "user" : "Sue" 
               },

   ) 

   #using default jinja template in a bash command
   print_logical_dt = BashOperator(
      task_id="print_logical_dt",
      bash_command= 'echo "{{ds}}"',
   ) 

    # bash_task = BashOperator(
    #     task_id="bash_task",
    #     bash_command="echo \"here is the message: '$message'\"",
    #     env={"message": '{{ dag_run.conf["message"] if dag_run else "" }}'},
    # )

   #using xcom_pull in a bash command to pull output of the bash command for a different task
   consume_xcom_data= BashOperator(
      task_id="consume_xcom_data",
      bash_command= "echo 'The upstream task sent the following logical date : '{{ ti.xcom_pull(task_ids='print_logical_dt') }}'.'",
   )  

   print_context_tsk >> greet_user >> get_env >> welcome_user >> print_logical_dt >> consume_xcom_data

   #dag_id=context_dag, task_id=print_context, run_id=manual__2024-10-01T18:03:20.750797+00:00, execution_date=20241001T180320, start_date=20241001T180326, end_date=20241001T180330