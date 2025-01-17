from airflow import DAG, Dataset
from airflow.models import Variable
from airflow.operators import empty
from airflow.providers.google.cloud.operators import cloud_run
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "analytics",
    "retries": 0,
    "depends_on_past": False,
    "start_date": "2024-09-09",
    "email": ["nikumar@pch.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
        dag_id="analytics_cloud_run_jobs.cr_dsci_onb_quiz_scoring_job_dag",
        default_args=default_args,
        max_active_runs=1,
        schedule=None,
        tags=["analytics", "scoring", "onb", "quiz", "cloud_run"],
        user_defined_macros={
            "model_group": "onb",
            "model_name": "quiz",
            "ana_dsci_scoring_img_name": f"{Variable.get('s_analytics_dsci_genvar_registry_root')}/dsci_onb_quiz_scoring_repo:latest",
            "ana_dsci_scoring_eml_name": f"{Variable.get('s_analytics_dsci_genvar_hmem_pod_email')}",
            "cr_project_id": f"prod-cd-shared-infra",
        },
        is_paused_upon_creation=True,
        catchup=False,
        default_view="graph",
) as dag:
    with TaskGroup(group_id="cloud_run_batch_process") as cloud_run_batch:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # create a cloud run operator
        create_ana_dsci_scoring_onb_quiz = cloud_run.CloudRunCreateJobOperator(
            task_id="create_ana_dsci_scoring_onb_quiz",
            project_id=dag.user_defined_macros.get("cr_project_id"),
            region="us-east1",
            job_name="ana-dsci-onb-quiz-cld-run-batch",
            gcp_conn_id="ana_prod-analytics-datasci2",
            job={
                "template": {
                    "template": {
                        "containers": [
                            {
                                "image": dag.user_defined_macros.get(
                                    "ana_dsci_scoring_img_name"
                                ),
                                "resources": {"limits": {"cpu": "8", "memory": "32Gi"}},
                            }
                        ],
                        "service_account": dag.user_defined_macros.get(
                            "ana_dsci_scoring_eml_name"
                        ),
                    }
                }
            },
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        (
                start
                >> create_ana_dsci_scoring_onb_quiz
                >> Label("Analytics Model Scoring Pipeline Job Config Creation Complete")
                >> end
        )