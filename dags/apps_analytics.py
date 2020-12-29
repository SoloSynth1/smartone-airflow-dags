from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'orix.auyeung',
    'depends_on_past': False,
    'email': ['orix.auyeung@hkmci.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
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

dag = DAG(
    'apps_analytics',
    default_args=default_args,
    description='Monthly triggered apps analytics pipeline',
    schedule_interval='@monthly',
    start_date=days_ago(2),
    tags=['analytics', 'apps'],
)

dag.doc_md = __doc__

app_store_analytics_worker = DummyOperator(task_id='app-store-analytics-worker', dag=dag)
app_store_analytics_reporter = DummyOperator(task_id='app-store-analytics-reporter', dag=dag)

play_store_analytics_worker = DummyOperator(task_id='play-store-analytics-worker', dag=dag)
play_store_analytics_reporter = DummyOperator(task_id='play-store-analytics-reporter', dag=dag)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

start.set_downstream([app_store_analytics_worker, play_store_analytics_worker])
app_store_analytics_worker.set_downstream(app_store_analytics_reporter)
play_store_analytics_worker.set_downstream(play_store_analytics_reporter)
end.set_upstream([app_store_analytics_reporter, play_store_analytics_reporter])
