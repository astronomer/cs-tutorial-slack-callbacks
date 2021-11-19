from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context
import traceback

"""
Follow Option #2 outlined here https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
in order to set up Slack HTTP webhook
"""


def dag_triggered_callback(context, **kwargs):
    slack_conn_id = kwargs["http_conn_id"]
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow-new: DAG has been triggered. 
            *Task*: {context.get('task_instance').task_id}  
            *DAG*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


def dag_success_callback(context, **kwargs):
    slack_conn_id = kwargs["http_conn_id"]
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow-new: DAG has succeeded. 
            *Task*: {context.get('task_instance').task_id}  
            *DAG*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


def success_callback(context, **kwargs):
    slack_conn_id = kwargs["http_conn_id"]
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :white_check_mark: Task has succeeded. 
            *Task*: {context.get('task_instance').task_id}  
            *DAG*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


def failure_callback(context, **kwargs):
    slack_conn_id = kwargs["http_conn_id"]
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    exception = context.get('exception')
    formatted_exception = ''.join(
        traceback.format_exception(etype=type(exception),
                                   value=exception,
                                   tb=exception.__traceback__)
    ).strip()
    slack_msg = f"""
            :x: Task has failed. 
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Exception*: {formatted_exception}
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


def retry_callback(context, **kwargs):
    slack_conn_id = kwargs["http_conn_id"]
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    exception = context.get('exception')
    formatted_exception = ''.join(
        traceback.format_exception(etype=type(exception),
                                   value=exception,
                                   tb=exception.__traceback__)
    ).strip()
    slack_msg = f"""
            :sos: Task is retrying.
            *Task*: {context.get('task_instance').task_id}
            *Try number:* {context.get('task_instance').try_number - 1} out of {context.get('task_instance').max_tries + 1}. 
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            *Exception*: {formatted_exception}
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


def slack_test(**kwargs):
    context = get_current_context()
    slack_conn_id = kwargs["http_conn_id"]
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow-spin-new: This is a test for sending a slack message via a PythonOperator.
            *Task*: {context.get('task_instance').task_id}
            *DAG*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis, *args, **kwargs):
    dag_id = slas[0].dag_id
    task_id = slas[0].task_id
    execution_date = slas[0].execution_date.isoformat()
    http_conn_id = kwargs["http_conn_id"]
    hook = SlackWebhookHook(
        http_conn_id=http_conn_id,
        webhook_token=BaseHook.get_connection(http_conn_id).password,
        message=f"""
            :sos: *SLA has been missed*
            *Task:* {task_id}
            *DAG:* {dag_id}
            *Execution Date:* {execution_date}
            """,
    )
    hook.execute()
