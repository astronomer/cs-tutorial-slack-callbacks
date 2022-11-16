"""
### Callbacks with Slack

Example DAG to showcase the various callbacks in Airflow.

Follow Option #2 outlined here https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
in order to set up Slack HTTP webhook
"""

import datetime
from datetime import timedelta
from functools import partial

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from include import slack_callback_functions_with_partial

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    # Callbacks set in default_args will apply to all tasks unless overridden at the task-level
    # Using the partial module allows you to specify different channels for different alerts
    "on_success_callback": partial(
        slack_callback_functions_with_partial.success_callback,
        http_conn_id="slack_callbacks_partial",
    ),
    "on_failure_callback": partial(
        slack_callback_functions_with_partial.failure_callback,
        http_conn_id="slack_callbacks_partial",
    ),
    "on_retry_callback": partial(
        slack_callback_functions_with_partial.retry_callback,
        http_conn_id="slack_callbacks_partial",
    ),
    "sla": timedelta(seconds=10),
}
with DAG(
    dag_id="slack_callbacks_with_partial",
    default_args=default_args,
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=timedelta(minutes=2),
    # sla_miss only applies to scheduled DAG runs, it does not work for manually triggered runs
    # If a DAG is running for the first time and sla is missed, sla_miss will not fire on that first run
    sla_miss_callback=partial(
        slack_callback_functions_with_partial.sla_miss_callback,
        http_conn_id="slack_callbacks_partial",
    ),
    catchup=False,
    doc_md=__doc__
) as dag:
    # This task uses on_execute_callback to send a notification when the task begins
    dummy_trigger = DummyOperator(
        task_id="dummy_trigger",
        on_execute_callback=partial(
            slack_callback_functions_with_partial.dag_triggered_callback,
            http_conn_id="slack_callbacks_partial",
        ),
        on_success_callback=None,
    )

    # This task uses the default_args on_success_callback
    dummy_success_test = DummyOperator(task_id="dummy_success_test")

    # This task sends a Slack message via a python_callable
    slack_test_func = PythonOperator(
        task_id="slack_test_func",
        python_callable=partial(
            slack_callback_functions_with_partial.slack_test,
            http_conn_id="slack_callbacks_partial",
        ),
        on_success_callback=None,
    )

    # Task will sleep to showcase sla_miss callback
    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",
    )

    # Task will retry before failing to showcase on_retry_callback
    bash_fail = BashOperator(
        task_id="bash_fail",
        retries=1,
        bash_command="exit 123",
    )

    # Task will still succeed despite previous task failing
    dummy_dag_success = DummyOperator(
        task_id="dummy_dag_success",
        on_success_callback=partial(
            slack_callback_functions_with_partial.dag_success_callback,
            http_conn_id="slack_callbacks_partial",
        ),
        trigger_rule="all_done",
    )

    (
        dummy_trigger
        >> dummy_success_test
        >> slack_test_func
        >> bash_sleep
        >> bash_fail
        >> dummy_dag_success
    )
