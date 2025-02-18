from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from airflow import DAG

default_args = {
    "owner": "Aditya & Kanha",
    "start_date" : datetime(2025,2,17),
    "retries":1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id = "Stock_Market_Pipeline",
    default_args=default_args,
    schedule_interval= "0 19 * * 1-5",
    catchup=False
)

start_producer = BashOperator(
    task_id = "start_producer",
    bash_command= "python3 /home/sunbeam/Desktop/bigData_Project/api_producer",
    dag = dag
)

start_consumer = BashOperator(
    task_id = "start_consumer",
    bash_command="python3 /home/sunbeam/Desktop/bigData_Project/api_consumer",
    dag= dag
)

wait_until_close = TimeSensor(
    task_id="wait_until_close",
    target_time=datetime.now().replace(hour=23, minute=30, second=0),
    dag=dag,
)

stop_pipeline = BashOperator(
    task_id="stop_pipeline",
    bash_command="pkill -f producer.py && pkill -f consumer.py",
    dag=dag,
)

start_producer >> start_consumer >> wait_until_close >> stop_pipeline
