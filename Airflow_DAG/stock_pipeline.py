from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, timedelta
from pytz import timezone

# Convert IST to UTC
ist = timezone('Asia/Kolkata')
utc = timezone('UTC')

ist_time = ist.localize(datetime(2024, 8, 14, 13, 0, 0))
utc_time = ist_time.astimezone(utc)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stopdag',
    default_args=default_args,
    description='Start and stop Zookeeper, Kafka, and Spark services',
    schedule_interval=timedelta(days=1),
    catchup=False
)

wait_until_time = TimeSensor(
    task_id='wait_until_time',
    target_time=utc_time.time(),
    dag=dag
)

# Start Zookeeper
start_zookeeper = BashOperator(
    task_id='start_zookeeper',
    bash_command='gnome-terminal -- bash -c "cd /home/sunbeam/kafka_2.13-2.7.0 && zookeeper-server-start.sh ./config/zookeeper.properties"',
    dag=dag,
)

# Start Kafka
start_kafka = BashOperator(
    task_id='start_kafka',
    bash_command='sleep 5 && gnome-terminal -- bash -c "cd /home/sunbeam/kafka_2.13-2.7.0 && kafka-server-start.sh ./config/server.properties"',
    dag=dag,
)

# Run Kafka producer in the background
run_comments_producer = BashOperator(
    task_id='run_stock_producer',
    bash_command='gnome-terminal -- bash -c "cd /home/sunbeam/Desktop/CDAC/BigData_Project/ && python3 kafka_producer.py; exec bash"',
    dag=dag,
)

# Run Spark consumer in the background
run_stream_processor = BashOperator(
    task_id='run_stream_processor',
    bash_command='gnome-terminal -- bash -c "cd /home/sunbeam/Desktop/CDAC/BigData_Project/ && python3 kafka_consumer_spark.py; exec bash"',
    dag=dag,
)

# Define parallel execution
start_zookeeper >> start_kafka  
start_kafka >> [run_comments_producer, run_stream_processor]  # Run in parallel
