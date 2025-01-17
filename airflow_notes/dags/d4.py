from airflow import DAG, Dataset
from airflow.operators import empty
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators import bigquery
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "cda",
    "retries": 0,
    "depends_on_past": False,
    "start_date": "2023-11-01",
    "email": "cda_dev_support@pch.com",
    "email_on_failure": True,
    "email_on_retry": True,
}


with DAG(
    dag_id="user_entry.gmt_entry_activity",
    default_args=default_args,
    max_active_runs=1,
    tags=["global_member_token", "user_entry", "gmt_entry_activity"],
    template_searchpath=["/home/airflow/gcs/data/gus/user_entry/gmt_entry_activity/"],
    schedule="@once",
    catchup=False,
    is_paused_upon_creation=True,
    default_view="graph",
) as dag:
    with TaskGroup(group_id="process") as process:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # pgus_cd_sa_online_variables_sa_online_billmeentries
        pgus_cd_sa_online_variables_sa_online_billmeentries = TriggerDagRunOperator(
            task_id="pgus_cd_sa_online_variables_sa_online_billmeentries",
            trigger_dag_id="it_sources.pgus_cd_sa_online_variables_sa_online_billmeentries",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # analytics_online_entries_contest_metadata
        analytics_online_entries_contest_metadata = TriggerDagRunOperator(
            task_id="analytics_online_entries_contest_metadata",
            trigger_dag_id="it_sources.analytics_online_entries_contest_metadata",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # analytics_online_entries_sweepstake_user_entry
        analytics_online_entries_sweepstake_user_entry = TriggerDagRunOperator(
            task_id="analytics_online_entries_sweepstake_user_entry",
            trigger_dag_id="it_sources.analytics_online_entries_sweepstake_user_entry",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # lookups_trackingtoken
        lookups_trackingtoken = TriggerDagRunOperator(
            task_id="lookups_trackingtoken",
            trigger_dag_id="it_sources.lookups_trackingtoken",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # gateway1
        gateway1 = empty.EmptyOperator(
            task_id="gateway1",
        )
        # gateway2
        gateway2 = empty.EmptyOperator(
            task_id="gateway2",
        )
        # prep_omni_billme_trackingtoken_gmt
        prep_omni_billme_trackingtoken_gmt = TriggerDagRunOperator(
            task_id="prep_omni_billme_trackingtoken_gmt",
            trigger_dag_id="it_common_prep.prep_omni_billme_trackingtoken_gmt",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
            conf={"is_backfill": "False"},
        )
        # prep_omni_vw_subscriberaddressmatchcodehistory
        prep_omni_vw_subscriberaddressmatchcodehistory = TriggerDagRunOperator(
            task_id="prep_omni_vw_subscriberaddressmatchcodehistory",
            trigger_dag_id="it_common_prep.prep_omni_vw_subscriberaddressmatchcodehistory",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
            conf={"is_backfill": "False"},
        )
        # mart_gmt_entry_activity
        mart_gmt_entry_activity_0 = TriggerDagRunOperator(
            task_id="mart_gmt_entry_activity_0",
            trigger_dag_id="it_common_mart.mart_gmt_entry_activity",
            execution_date="{{execution_date}}",
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # mart_gmt_entry_activity
        mart_gmt_entry_activity_1 = TriggerDagRunOperator(
            task_id="mart_gmt_entry_activity_1",
            trigger_dag_id="it_common_mart.mart_gmt_entry_activity",
            execution_date='{{ (execution_date + macros.timedelta(days=-1)).strftime("%Y-%m-%dT%H:%M:%S+00:00") }}',
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # mart_gmt_entry_activity
        mart_gmt_entry_activity_2 = TriggerDagRunOperator(
            task_id="mart_gmt_entry_activity_2",
            trigger_dag_id="it_common_mart.mart_gmt_entry_activity",
            execution_date='{{ (execution_date + macros.timedelta(days=-2)).strftime("%Y-%m-%dT%H:%M:%S+00:00") }}',
            wait_for_completion=True,
            reset_dag_run=True,
        )
        # Final view of snapshot table
        view_from_snapshot = bigquery.BigQueryInsertJobOperator(
            task_id="view_from_snapshot",
            configuration={
                "query": {
                    "query": "{% include 'process/view_create.sql' %}",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "user_digital_advertising",
                    "biz_execution": "daily",
                    "dev_search_terms": "gmt_entry_activity",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        (
            start
            >> [
                pgus_cd_sa_online_variables_sa_online_billmeentries,
                analytics_online_entries_contest_metadata,
                analytics_online_entries_sweepstake_user_entry,
                lookups_trackingtoken,
            ]
            >> gateway1
        )
        (
            gateway1
            >> [
                prep_omni_vw_subscriberaddressmatchcodehistory,
                prep_omni_billme_trackingtoken_gmt,
            ]
            >> gateway2
        )
        (
            gateway2
            >> mart_gmt_entry_activity_2
            >> mart_gmt_entry_activity_1
            >> mart_gmt_entry_activity_0
            >> Label("table refreshed ")
            >> view_from_snapshot
            >> end
        )
    with TaskGroup(group_id="post_process") as post_process:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        start >> end

process >> post_process
