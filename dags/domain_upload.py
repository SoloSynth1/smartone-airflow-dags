from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
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
dag = DAG(
    'domain_upload_pipeline',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
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
    get_logs=True,
    dag=dag,
    image_pull_policy='Always',
    is_delete_operator_pod=True,
)

dag.doc_md = __doc__

domain_queuer.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

start = DummyOperator(task_id="start")
end = DummyOperator(task_id="end")

domain_googlesearch = DummyOperator(task_id="domain-googlesearch")
domain_landingpage = DummyOperator(task_id="domain-landingpage")
domain_webshrinker = DummyOperator(task_id="domain-webshrinker")

domain_googlesearch_workers = [DummyOperator(task_id="domain-googlesearch-worker-{}".format(x) for x in range(3))]
domain_landingpage_workers = [DummyOperator(task_id="domain-landingpage-worker-{}".format(x) for x in range(3))]
domain_webshrinker_workers = [DummyOperator(task_id="domain-webshrinker-worker-{}".format(x) for x in range(3))]

domain_googlesearch_reporter = DummyOperator(task_id="domain-googlesearch-reporter")
domain_landingpage_reporter = DummyOperator(task_id="domain-landingpage-reporter")
domain_webshrinker_reporter = DummyOperator(task_id="domain-webshrinker-reporter")

start.set_downstream(domain_queuer)
domain_queuer.set_downstream([domain_googlesearch, domain_landingpage, domain_webshrinker])

domain_googlesearch.set_downstream(domain_googlesearch_workers)
domain_landingpage.set_downstream(domain_landingpage_workers)
domain_webshrinker.set_downstream(domain_webshrinker_workers)

domain_googlesearch_reporter.set_upstream(domain_googlesearch_workers)
domain_landingpage_reporter.set_upstream(domain_landingpage_workers)
domain_webshrinker_reporter.set_upstream(domain_webshrinker_workers)

end.set_upstream([domain_googlesearch_reporter, domain_landingpage_reporter, domain_webshrinker_reporter])
