from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator
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


domain_queuer = DockerOperator(
    image="gcr.io/smartone-gcp-1/",
    task_id="domain_queuer",
    command="{{ dag_run.conf['minioObject'] }} {{ dag_run.conf['runId'] }}",
    dag=dag,
    auto_remove=True
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

start = DummyOperator()
end = DummyOperator()

start >> domain_queuer >> end

# start >> domain_queuer >> [domain_webshrinker, domain_googlesearch, domain_landingpage] >> domain_reporter >> end
