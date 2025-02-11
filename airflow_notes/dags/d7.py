from airflow import DAG,Dataset
from airflow.utils.task_group import TaskGroup
import pendulum
from airflow.operators import bash
from airflow.operators import empty
from airflow.operators.python_operator import PythonOperator

default_args={
    "owner":"rjannu",
    "start_date":pendulum.datetime(2025, 1, 12, tz="UTC"),
    "retries":0,
    "depends_on_past":False,
    "email":["rjannu@pch.com"],
}
trige2= Dataset("file://trigger2/test2")
trige1= Dataset("file://trigger1/test1")

def my_callingpython(ds,*args,**kwargs):
  print(f"Hello from PythonOperator,{ds}")


with DAG(
  dag_id="dataset2",
  default_args=default_args,
  catchup=False,
  schedule="@daily",
  max_active_runs=1,
) as dag2:
  with TaskGroup(group_id="dataset2") as tg:
    st=empty.EmptyOperator(task_id="start")
    bo=bash.BashOperator(task_id="trigger_ds2",outlets=[trige2],bash_command="sleep 5")
    py=PythonOperator(task_id="callpy",python_callable=my_callingpython,provide_context=True)
    et=empty.EmptyOperator(task_id="end")
    (
      st >> bo >> py>>et
    )

(
  tg
)