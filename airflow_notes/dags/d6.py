from airflow import DAG,Dataset
from airflow.models import Variable
from datetime import datetime
from airflow.operators import empty
from airflow.operators import bash
from airflow.utils.task_group import TaskGroup

default_args={
    "owner":"rjannu",
    "retries":0,
    "start_date":"2025-01-12",
    "depends_on_past":False,
    "email":["rjannu@pch.com"],
    'email_on_failure':True,
}
trige1= Dataset("file://trigger1/test1")

with DAG(
    dag_id="dataset1",
    default_args=default_args,
    max_active_runs=1,
    tags=["bigdata"],
    template_searchpath=["/home/airflow/gcs/dags/"],
    user_defined_macros={
        "dis":"test",
        "env":"dev"
    },
    schedule="15 * * * *",
    is_paused_upon_creation=True,
    catchup=False,
) as dag:
     with TaskGroup(group_id="task_1") as tg_config:
          st=empty.EmptyOperator(
               task_id="start",
          )
          baop=bash.BashOperator(outlets=[trige1],task_id="producer_1",
                                 bash_command="sleep 5"
          )

          et=empty.EmptyOperator(
               task_id="end",
          )

          (
               st >> baop >> et
          )
          
(
   tg_config  
)