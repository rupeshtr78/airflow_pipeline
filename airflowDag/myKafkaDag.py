from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['rupesh@forsynet.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
# [END default_args]

# [START instantiate_dag]
with DAG(
        'Kafka_Producer_PipeLine',
        default_args=default_args,
        description='Kafka Cassandra Pipeline',
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['kafka','cassandra'],
) as dag:
    # [END instantiate_dag]
    t0 = DummyOperator(
        task_id='start_pipeline',
    )
    # t1, t2 are tasks created by instantiating bash operators
    # [START kafka producer]
    # space at the end of filepath. whichever operator you are using you should always follow the same rule.
    t1 = BashOperator(
        task_id='kafka_producer',
        bash_command='/home/hyper/scripts/airflow_bash/run_producer.sh ',
    )

    t2 = BashOperator(
        task_id='kafka_cassandra',
        depends_on_past=False,
        bash_command='/home/hyper/scripts/airflow_bash/run_cassandra.sh ',
    )
    # [END kafka consumer cassandra]

    t0 >> [t1,t2]
    # [END stocks pipeline]

