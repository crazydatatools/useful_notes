from airflow import DAG
from airflow.operators import dummy
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod
from airflow.providers.google.cloud.transfers import gcs_to_bigquery
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "cnstmgmt",
    "retries": 0,
    "depends_on_past": False,
    "start_date": "2021-03-01",
    "email": "xx",
    "email_on_failure": True,
    "email_on_retry": True,
}


with DAG(
    dag_id="feeds.mssql_sync_dbo_campaign",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@once",
    catchup=False,
    default_view="graph",
) as dag:
    with TaskGroup(group_id="msql_gcs_gbq_load") as msql_gcs_gbq_load:
        # start
        start = dummy.DummyOperator(
            task_id="start",
        )
        # Task to load campaigns CSV data to a BigQuery table
        load_to_bq_dbo_campaigns = gcs_to_bigquery.GCSToBigQueryOperator(
            task_id="load_to_bq_dbo_campaigns",
            gcp_conn_id="cda_goldusersummary",
            location="US",
            bucket="us-east1-test-eval-438597e6-bucket",
            source_objects=["data/extracts/mssql/entryid/campaigns/Campaigns.csv"],
            source_format="CSV",
            destination_project_dataset_table="workspace.campaigns",
            allow_quoted_newlines=True,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=[
                {"name": "CampaignID", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "MailingID", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DeployDate", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DeadlineDate", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ESPMailingName", "type": "STRING", "mode": "NULLABLE"},
                {"name": "PackageID", "type": "STRING", "mode": "NULLABLE"},
                {"name": "MailingGroup", "type": "STRING", "mode": "NULLABLE"},
                {"name": "AutoClaimInd", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "CampaignStatusID", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "Created", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Updated", "type": "STRING", "mode": "NULLABLE"},
            ],
        )
        # campaigns table extract
        dbo_campaigns = kubernetes_pod.KubernetesPodOperator(
            task_id="dbo_campaigns",
            name="dbo_campaigns_k_pod_operator",
            namespace="composer-2-0-13-airflow-2-2-5-438597e6",
            service_account_name="default",
            image="us-east1-docker.pkg.dev/dev-cd-shared-infra/entryid-docker-repo/entryid_mssql_extract:latest",
            image_pull_policy="Always",
            cmds=["bash"],
            arguments=["campaigns.sh"],
        )
        # end
        end = dummy.DummyOperator(
            task_id="end",
        )
        (
            start
            >> Label("extract_prss")
            >> dbo_campaigns
            >> load_to_bq_dbo_campaigns
            >> end
        )

msql_gcs_gbq_load