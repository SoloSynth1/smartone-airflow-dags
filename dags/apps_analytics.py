from uuid import uuid4
from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def generate_run_id():
    return str(uuid4())


def print_run_id(run_id):
    received_id = "runId is {}".format(run_id)
    print(received_id)
    return received_id


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

appstore_analytics_worker = KubernetesPodOperator(namespace='airflow',
                                                  image="gcr.io/smartone-gcp-1/appstore-analytics-worker:latest",
                                                  name="appstore-analytics-worker",
                                                  task_id="appstore-analytics-worker",
                                                  get_logs=True,
                                                  dag=dag,
                                                  env_vars={
                                                      "RUN_ID": "{{ ti.xcom_pull(task_ids='run-id-generator') }}",
                                                  },
                                                  image_pull_policy='Always',
                                                  is_delete_operator_pod=True,)
appstore_analytics_reporter = DummyOperator(task_id='app-store-analytics-reporter', dag=dag)

playstore_analytics_worker = KubernetesPodOperator(namespace='airflow',
                                                   image="gcr.io/smartone-gcp-1/playstore-analytics-worker:latest",
                                                   name="playstore-analytics-worker",
                                                   task_id="playstore-analytics-worker",
                                                   get_logs=True,
                                                   dag=dag,
                                                   env_vars={
                                                       "RUN_ID": "{{ ti.xcom_pull(task_ids='run-id-generator') }}",
                                                   },
                                                   image_pull_policy='Always',
                                                   is_delete_operator_pod=True,)
playstore_analytics_reporter = DummyOperator(task_id='play-store-analytics-reporter', dag=dag)

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

run_id_generator = PythonOperator(task_id='run-id-generator',
                                  dag=dag,
                                  python_callable=generate_run_id,
                                  )


start >> run_id_generator >> [appstore_analytics_worker, playstore_analytics_worker]
appstore_analytics_worker >> appstore_analytics_reporter
playstore_analytics_worker >> playstore_analytics_reporter
[appstore_analytics_reporter, playstore_analytics_reporter] >> end
