from airflow import DAG,Dataset
from airflow.operators import bash
from airflow.operators import empty

from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
import time
import pendulum

# Simulate sleep function
def sleep_mode(num):
    time.sleep(num)

# Datasets
trige2 = Dataset("file://trigger2/test2")
trige1 = Dataset("file://trigger1/test1")

# DAG definition
with DAG(
    dag_id="consumer_dag",
    start_date=pendulum.datetime(2025, 1, 17, tz="UTC"),
    schedule=[trige2, trige1],
    catchup=False,
) as dag1:

    # TaskGroup definition
    with TaskGroup(group_id="consumer_task") as ct:
        st = empty.EmptyOperator(task_id="start")  # Fix typo in task_id
        bo = bash.BashOperator(task_id="trigger_ds2", bash_command="sleep 5")
        et = empty.EmptyOperator(task_id="end")  # Fix typo in task_id

        # TaskGroup flow
        st >> bo >> et

    # Loop to dynamically create tasks and add them to the DAG
    for i in range(3):
        task = bash.BashOperator(
            task_id=f"bash_task_{i}",
            bash_command=f"echo 'Task {i}' && sleep {i}",
        )
        ct >> task  # Connect each task to the TaskGroup
