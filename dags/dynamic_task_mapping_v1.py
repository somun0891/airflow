from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Define a simple DAG
# with DAG(dag_id='example_dynamic_task_mapping', start_date=datetime(2022, 3, 4)) as dag:
with DAG(dag_id="example_dynamic_task_mapping_v1", start_date=datetime(2023, 5, 1), schedule=None, catchup=False) as dag:

    @task
    def apply_filter(mark):
        return mark if isinstance(mark, int) else None
    
    @task
    def calculate_percentage(marks):
        print(marks)
        print(sum(marks) / (len(marks)))
    
    filtered_values = apply_filter.expand(mark=[68, 96, 67, None, 'N/A', 88, 'N/A'])
    calculate_percentage(filtered_values)