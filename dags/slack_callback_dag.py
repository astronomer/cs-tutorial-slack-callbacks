"""
### Slack Callbacks

Example DAG to showcase the various callbacks in Airflow. Shows how to send success and failure notifications using custom callbacks.

Follow Option #2 outlined here https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
in order to set up Slack HTTP webhook
"""

import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from include import slack_callback_functions

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
    # Callbacks set in default_args will apply to all tasks unless overridden at the task-level
    "on_success_callback": slack_callback_functions.success_callback,
    "on_failure_callback": slack_callback_functions.failure_callback,
    "on_retry_callback": slack_callback_functions.retry_callback,
    "sla": timedelta(seconds=10),
}
with DAG(
        dag_id="slack_callbacks",
        default_args=default_args,
        start_date=datetime.datetime(2021, 1, 1),
        schedule_interval=timedelta(minutes=2),
        # sla_miss only applies to scheduled DAG runs, it does not work for manually triggered runs
        # If a DAG is running for the first time and sla is missed, sla_miss will not fire on that first run
        sla_miss_callback=slack_callback_functions.sla_miss_callback,
        doc_md=__doc__,
        catchup=False,
) as dag:
    # This task uses on_execute_callback to send a notification when the task begins
    dummy_trigger = DummyOperator(
        task_id="dummy_trigger",
        on_execute_callback=slack_callback_functions.dag_triggered_callback,
        on_success_callback=None,
    )

    # This task uses the default_args on_success_callback
    dummy_success_test = DummyOperator(
        task_id="dummy_success_test",
    )

    # This task sends a Slack message via a python_callable
    slack_test_func = PythonOperator(
        task_id="slack_test_func",
        python_callable=slack_callback_functions.slack_test,
        on_success_callback=None,
    )

    # Task will sleep beyond the 10 second SLA to showcase sla_miss callback
    bash_sleep = BashOperator(
        task_id="bash_sleep",
        bash_command="sleep 30",
    )

    # Task will retry once before failing to showcase on_retry_callback and on_failure_callback
    bash_fail = BashOperator(
        task_id="bash_fail",
        retries=1,
        bash_command="exit 123",
    )

    # Task will still succeed despite previous task failing, showcasing use of the
    # last task in a DAG to notify that the DAG has completed
    dummy_dag_success = DummyOperator(
        task_id="dummy_dag_success",
        on_success_callback=slack_callback_functions.dag_success_callback,
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
