# [START tutorial]
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG

# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'thiagodf',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['thiagodf@ciandt.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'test-mysql-db',
    default_args=default_args,
    description='A DAG test with mysql db',
    schedule_interval=timedelta(days=1),
    tags=['test-mysql-db'],
)
# [END instantiate_dag]

# [START basic_task]
# t1 - check if the database airflow_db in exists.
t1 = BashOperator(
    task_id='check_airflow_db',
    bash_command='date',
    dag=dag,
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag,
)
# [END basic_task]

# [START documentation]
dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""
# [END documentation]

# [START jinja_template]
templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)
# [END jinja_template]

# t1.set_downstream(t2)

# This means that t2 will depend on t1
# running successfully to run.
# It is equivalent to:
# t2.set_upstream(t1)

# The bit shift operator can also be
# used to chain operations:
# t1 >> t2

# And the upstream dependency with the
# bit shift operator:
# t2 << t1

# Chaining multiple dependencies becomes
# concise with the bit shift operator:
# t1 >> t2 >> t3

# A list of tasks can also be set as
# dependencies. These operations
# all have the same effect:
# t1.set_downstream([t2, t3])
t1 >> [t2, t3]
# [t2, t3] << t1
# [END tutorial]