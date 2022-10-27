from __future__ import annotations

from abc import ABC
from datetime import timedelta
from typing import Callable

from airflow.configuration import conf
from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.models import DAG, TaskInstance
from airflow.models.baseoperator import BaseOperator


# def dag_policy(dag: DAG):
#     """Ensure that DAG has at least one tag"""
#     print("Executing dag")
#     if not dag.tags:
#         dag.tags.append("unravel")

# def task_instance_mutation_hook(task_instance: TaskInstance):
#     print("running task instance hook")
#     print("task_id info:", task_instance.task_id, task_instance.run_id, task_instance.duration, task_instance.pid)
#     if task_instance.try_number >= 1:
#         task_instance.queue = 'retry_queue'

def task_success_alert(context):
    print("Task has succeeded")
    print("Context details:",context)
    print(f"run_id: {context['run_id']}")

def task_failure_alert(context):
    print("Task has failed")
    print("context for failure case:",context)

def add_success_callback(task: BaseOperator):
    print("Adding success callback")
    if not task.on_success_callback:
        
        task.on_success_callback= task_success_alert
        task.on_failure_callback= task_failure_alert

def task_must_have_owners(task: BaseOperator):
    if task.owner and not isinstance(task.owner, str):
        raise AirflowClusterPolicyViolation(f'''owner should be a string. Current value: {task.owner!r}''')

    # if not task.owner or task.owner.lower() == conf.get('operators', 'default_owner'):
    #     raise AirflowClusterPolicyViolation(
    #         f'''Task must have non-None non-default owner. Current value: {task.owner}'''
    #     )

TASK_RULES: list[Callable[[BaseOperator], None]] = [
    task_must_have_owners,
    add_success_callback
]

def _check_task_rules(current_task: BaseOperator):
    """Check task rules for given task."""
    notices = []
    for rule in TASK_RULES:
        try:
            rule(current_task)
        except AirflowClusterPolicyViolation as ex:
            notices.append(str(ex))
    if notices:
        notices_list = " * " + "\n * ".join(notices)
        raise AirflowClusterPolicyViolation(
            f"DAG policy violation (DAG ID: {current_task.dag_id}, Path: {current_task.dag.fileloc}):\n"
            f"Notices:\n"
            f"{notices_list}"
        )

# def task_policy(task: BaseOperator):
#     """Ensure Tasks have non-default owners."""
#     print("When task policy gets executed??")
#     _check_task_rules(task)
