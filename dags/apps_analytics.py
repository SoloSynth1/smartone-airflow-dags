from datetime import timedelta, datetime

import pytz
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# from kubernetes.client import models as k8s


def generate_run_id():
    hkt = pytz.timezone('Asia/Hong_Kong')
    datestring = datetime.now().astimezone(hkt).strftime("%Y%m%d%H%M")
    return "HKT_{}".format(datestring)


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

# crawler_affinity = k8s.V1Affinity(
#     node_affinity=k8s.V1NodeAffinity(
#         required_during_scheduling_ignored_during_execution=[
#             k8s.V1NodeSelector(
#                 node_selector_terms=[
#                     k8s.V1NodeSelectorTerm(
#                         match_expressions=[
#                             k8s.V1NodeSelectorRequirement(key='node-type', operator='In', values=['crawler-node'])
#                         ],
#                     )
#                 ]
#             )
#         ]
#     ),
#     pod_affinity=k8s.V1PodAntiAffinity(
#         required_during_scheduling_ignored_during_execution=[
#             k8s.V1PodAffinityTerm(
#                 label_selector=k8s.V1LabelSelector(
#                     match_expressions=[
#                         k8s.V1LabelSelectorRequirement(key="pod-type", operator="In", values="crawler-pod")
#                     ]
#                 ),
#                 topology_key="failure-domain.beta.kubernetes.io/zone",
#             )
#         ]
#     ),
# )
crawler_affinity = {
    'nodeAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': {
            'nodeSelectorTerms': [{
                'matchExpressions': [{
                    'key': 'cloud.google.com/gke-nodepool',
                    'operator': 'In',
                    'values': ['crawler-pool']
                }]
            }]
        }
    },
    'podAntiAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': [{
            'labelSelector': {
                'matchExpressions': [{
                    'key': 'pod-type',
                    'operator': 'In',
                    'values': ['crawler-pod']
                }]
            },
            'topologyKey': 'kubernetes.io/hostname'
        }]
    },
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

run_id_generator = PythonOperator(task_id='run-id-generator',
                                  dag=dag,
                                  python_callable=generate_run_id, )

appstore_analytics_worker_regex_range = ['[a-b]', '[c-e]', '[f]', '[g-h]', '[i-l]', '[m-n]', '[o-p]', '[q-r]', '[s-t]', '[u-z]']
appstore_analytics_workers = [KubernetesPodOperator(namespace='airflow',
                                                    image="gcr.io/smartone-gcp-1/appstore-scrapy:latest",
                                                    name="appstore-analytics-worker-{}".format(idx),
                                                    task_id="appstore-analytics-worker-{}".format(idx),
                                                    get_logs=True,
                                                    dag=dag,
                                                    env_vars={
                                                        "RUN_ID": "{{ ti.xcom_pull(task_ids='run-id-generator') }}",
                                                        "APPSTORE_RANGE_REGEX": regex_range,
                                                    },
                                                    resources={
                                                        "request_memory": "1024Mi",
                                                        "request_cpu": "300m",
                                                    },
                                                    affinity=crawler_affinity,
                                                    labels={'pod-type': 'crawler-pod'},
                                                    image_pull_policy='Always',
                                                    is_delete_operator_pod=True, ) for idx, regex_range in enumerate(appstore_analytics_worker_regex_range)]

appstore_analytics_reporter = KubernetesPodOperator(namespace='airflow',
                                                    image="gcr.io/smartone-gcp-1/reporter:latest",
                                                    name="appstore-analytics-reporter",
                                                    task_id="appstore-analytics-reporter",
                                                    get_logs=True,
                                                    dag=dag,
                                                    env_vars={
                                                        "RUN_ID": "{{ ti.xcom_pull(task_ids='run-id-generator') }}",
                                                        "RUN_FLOW": "appstore",
                                                        "OUTPUT_VOLUME_PATH": ".",
                                                    },
                                                    resources={
                                                        "request_memory": "1024Mi",
                                                        "request_cpu": "1200m",
                                                        "request_ephemeral_storage": "20Gi",
                                                    },
                                                    image_pull_policy='Always',
                                                    is_delete_operator_pod=True, )

playstore_analytics_worker = KubernetesPodOperator(namespace='airflow',
                                                   image="gcr.io/smartone-gcp-1/playstore-analytics-worker:latest",
                                                   name="playstore-analytics-worker",
                                                   task_id="playstore-analytics-worker",
                                                   get_logs=True,
                                                   dag=dag,
                                                   env_vars={
                                                       "RUN_ID": "{{ ti.xcom_pull(task_ids='run-id-generator') }}",
                                                   },
                                                   resources={
                                                       "request_memory": "1024Mi",
                                                       "request_cpu": "300m",
                                                   },
                                                   affinity=crawler_affinity,
                                                   labels={'pod-type': 'crawler-pod'},
                                                   image_pull_policy='Always',
                                                   is_delete_operator_pod=True, )
playstore_analytics_reporter = KubernetesPodOperator(namespace='airflow',
                                                     image="gcr.io/smartone-gcp-1/reporter:latest",
                                                     name="playstore-analytics-reporter",
                                                     task_id="playstore-analytics-reporter",
                                                     get_logs=True,
                                                     dag=dag,
                                                     env_vars={
                                                         "RUN_ID": "{{ ti.xcom_pull(task_ids='run-id-generator') }}",
                                                         "RUN_FLOW": "playstore",
                                                         "OUTPUT_VOLUME_PATH": ".",
                                                     },
                                                     resources={
                                                         "request_memory": "1024Mi",
                                                         "request_cpu": "1200m",
                                                         "request_ephemeral_storage": "20Gi",
                                                     },
                                                     image_pull_policy='Always',
                                                     is_delete_operator_pod=True, )

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
appstore_analytics = DummyOperator(task_id="appstore-analytics", dag=dag)
playstore_analytics = DummyOperator(task_id="playstore-analytics", dag=dag)

start >> run_id_generator >> [appstore_analytics, playstore_analytics]
appstore_analytics >> appstore_analytics_workers
playstore_analytics >> playstore_analytics_worker
appstore_analytics_workers >> appstore_analytics_reporter
playstore_analytics_worker >> playstore_analytics_reporter
[appstore_analytics_reporter, playstore_analytics_reporter] >> end
