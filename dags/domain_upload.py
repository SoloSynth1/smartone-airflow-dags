from datetime import timedelta

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
    'retry_delay': timedelta(minutes=5),
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

common_pod_args = {
    "is_delete_operator_pod": True,
    "image_pull_policy": "Always",
    "get_logs": False,
}

crawler_pod_args = {
    "affinity": {
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
        }
    },
    "labels": {
        'pod-type': 'crawler-pod',
        'redis-client': 'true'
    },
}

dag = DAG(
    'domain_upload_pipeline',
    default_args=default_args,
    description='Minio-triggered domain analytics pipeline',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['analytics', 'external trigger'],
)


domain_queuer = KubernetesPodOperator(
    namespace='airflow',
    image="gcr.io/smartone-gcp-1/domain-queuer:latest",
    env_vars={
        "RUN_ID": "{{ dag_run.conf['runId'] }}",
        "FILENAME": "{{ dag_run.conf['minioObject'] }}"
    },
    labels={"redis-client": "true"},
    name="domain-queuer",
    task_id="domain-queuer",
    dag=dag,
    **common_pod_args,
)


def landingpage_worker(worker_id):
    return KubernetesPodOperator(namespace='airflow',
                                 image="gcr.io/smartone-gcp-1/domain-landingpage-parser:latest",
                                 name="domain-landingpage-worker-{}".format(worker_id),
                                 task_id="domain-landingpage-worker-{}".format(worker_id),
                                 dag=dag,
                                 **common_pod_args,
                                 **crawler_pod_args,)


def webshrinker_worker(worker_id):
    return KubernetesPodOperator(namespace='airflow',
                                 image="gcr.io/smartone-gcp-1/domain-webshrinker-worker:latest",
                                 name="domain-webshrinker-worker-{}".format(worker_id),
                                 task_id="domain-webshrinker-worker-{}".format(worker_id),
                                 dag=dag,
                                 **common_pod_args,
                                 **crawler_pod_args,)


def googlesearch_worker(worker_id):
    return KubernetesPodOperator(namespace='airflow',
                                 image="gcr.io/smartone-gcp-1/domain-googlesearch-parser:latest",
                                 name="domain-googlesearch-worker-{}".format(worker_id),
                                 task_id="domain-googlesearch-worker-{}".format(worker_id),
                                 dag=dag,
                                 **common_pod_args,
                                 **crawler_pod_args,)


dag.doc_md = __doc__

domain_queuer.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

domain_googlesearch = DummyOperator(task_id="domain-googlesearch", dag=dag)
domain_landingpage = DummyOperator(task_id="domain-landingpage", dag=dag)
domain_webshrinker = DummyOperator(task_id="domain-webshrinker", dag=dag)

domain_googlesearch_workers = [googlesearch_worker(x) for x in range(3)]
domain_landingpage_workers = [landingpage_worker(x) for x in range(3)]
domain_webshrinker_workers = [webshrinker_worker(x) for x in range(3)]

domain_googlesearch_reporter = KubernetesPodOperator(namespace='airflow',
                                                     image="gcr.io/smartone-gcp-1/reporter:latest",
                                                     labels={"redis-client": "true"},
                                                     name="domain-googlesearch-reporter",
                                                     task_id="domain-googlesearch-reporter",
                                                     dag=dag,
                                                     env_vars={
                                                         "RUN_ID": "{{ dag_run.conf['runId'] }}",
                                                         "RUN_FLOW": "googlesearch",
                                                         "OUTPUT_VOLUME_PATH": ".",
                                                     },
                                                     resources={
                                                         "request_memory": "1024Mi",
                                                         "request_cpu": "1000m",
                                                         "request_ephemeral_storage": "20Gi",
                                                     },
                                                     **common_pod_args,)

domain_landingpage_reporter = KubernetesPodOperator(namespace='airflow',
                                                    image="gcr.io/smartone-gcp-1/reporter:latest",
                                                    labels={"redis-client": "true"},
                                                    name="domain-landingpage-reporter",
                                                    task_id="domain-landingpage-reporter",
                                                    dag=dag,
                                                    env_vars={
                                                        "RUN_ID": "{{ dag_run.conf['runId'] }}",
                                                        "RUN_FLOW": "landingpage",
                                                        "OUTPUT_VOLUME_PATH": ".",
                                                    },
                                                    resources={
                                                        "request_memory": "1024Mi",
                                                        "request_cpu": "1000m",
                                                        "request_ephemeral_storage": "20Gi",
                                                    },
                                                    **common_pod_args,)

domain_webshrinker_reporter = KubernetesPodOperator(namespace='airflow',
                                                    image="gcr.io/smartone-gcp-1/reporter:latest",
                                                    labels={"redis-client": "true"},
                                                    name="domain-webshrinker-reporter",
                                                    task_id="domain-webshrinker-reporter",
                                                    dag=dag,
                                                    env_vars={
                                                        "RUN_ID": "{{ dag_run.conf['runId'] }}",
                                                        "RUN_FLOW": "webshrinker",
                                                        "OUTPUT_VOLUME_PATH": ".",
                                                    },
                                                    resources={
                                                        "request_memory": "1024Mi",
                                                        "request_cpu": "1000m",
                                                        "request_ephemeral_storage": "20Gi",
                                                    },
                                                    **common_pod_args,)

start.set_downstream(domain_queuer)
domain_queuer.set_downstream([domain_googlesearch, domain_landingpage, domain_webshrinker])

domain_googlesearch.set_downstream(domain_googlesearch_workers)
domain_landingpage.set_downstream(domain_landingpage_workers)
domain_webshrinker.set_downstream(domain_webshrinker_workers)

domain_googlesearch_reporter.set_upstream(domain_googlesearch_workers)
domain_landingpage_reporter.set_upstream(domain_landingpage_workers)
domain_webshrinker_reporter.set_upstream(domain_webshrinker_workers)

end.set_upstream([domain_googlesearch_reporter, domain_landingpage_reporter, domain_webshrinker_reporter])
