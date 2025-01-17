from datetime import datetime
from airflow.models import Variable
import pendulum
from airflow import DAG, Dataset
from airflow.operators import bash
from airflow.providers.google.cloud.operators import dataform
from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import CreateBatchPredictionJobOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_mysql import BigQueryToMySqlOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod

A2_TAG_NAVI_SEARCH_CAT = Dataset(
    "file://home/airflow/gcs/data/shared/disservices/com_pch_search/navigated_search_categories"
    "/a2_tag_ds_navigated_search_categories.csv"
)

default_args = {
    "owner": "dis_cda",
    "retries": 0,
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 20, tzinfo=pendulum.timezone("America/New_York")),
    "email": ["lkolluru@pch.com","dlychev@pch.com"],
    "email_on_failure": True,
    "email_on_retry": True,
}

with DAG(
        dag_id="com_pch_search.schd_navigated_search_categories",
        default_args=default_args,
        max_active_runs=1,
        tags=["com_pch_search",
              "flw_cd_navigated_search_categories",
              "schd_cd_navigated_search_categories",
              "dataform",
              "ai",
              "schedule"
              ],
        template_searchpath=[
            "/home/airflow/gcs/dags/disservices/datasets/com_pch_search/pipelines/navigated_search_categories"
        ],
        user_defined_macros={
            "dis_ds_name": "navigated_search_categories",
            "airflow_dataset_root": "/home/airflow/gcs/data/shared/disservices/com_pch_search"
                                    "/navigated_search_categories",
            "env":  f"{Variable.get('s_dis_genvar_env')}",
            "data_sync_image_name": f"{Variable.get('s_diservices_genvar_registry_root')}/navigated_search_categories"
        },
        schedule="0 21 * * *",
        is_paused_upon_creation=True,
        catchup=False,
        default_view="graph",
) as dag:
    with TaskGroup(group_id="ai_cms_config") as ai_cms_config:
        # extract data from mysql to gcs for cms data extraction operations.
        mysql_to_gcs_navigated_search_categories = MySQLToGCSOperator(
            task_id='get_navigated_search_categories_mysql_to_gs',
            sql="{% include 'data/extract/categories.sql' %}",
            bucket='bkt_uat_transferservice_inbound',  # GCS bucket to store the file
            filename='mysql/ai/search_fp/navigated_search_categories/navigated_search_categories_mysql.csv',
            # Filename format for the output
            export_format='csv',  # Export format (can also be CSV)
            field_delimiter=',',  # Optional, used if exporting CSV
            mysql_conn_id='dis_searchv2db',  # Connection ID for MySQL
            gcp_conn_id='dis_uat_goldusersummary',  # Connection ID for Google Cloud
            gzip=False  # Optional: Set to True if you want to gzip the output file
        )

    with TaskGroup(group_id="ai_data_prepare") as ai_data_prepare:
        # create compilation result for the current run
        create_compilation_result = dataform.DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            project_id="uat-gold-usersummary",
            gcp_conn_id="cda_uat_goldusersummary",
            region="us-east1",
            repository_id="pipelines",
            compilation_result={
                "git_commitish": "main",
                "workspace": "projects/uat-gold-usersummary/locations/us-east1/repositories/pipelines/workspaces"
                             "/persona_mart",
                "code_compilation_config": {
                    "vars": {
                        "ds": "{{ data_interval_end  | ds }}",
                        "ds_nodash": "{{ data_interval_end | ds_nodash }}",
                        "ts": "{{ data_interval_end | ts }}",
                        "ts_nodash": "{{ data_interval_end | ts_nodash }}"
                    }
                },
            },
        )
        # get_compilation_result
        get_compilation_result = dataform.DataformGetCompilationResultOperator(
            task_id="get_compilation_result",
            project_id="uat-gold-usersummary",
            region="us-east1",
            repository_id="pipelines",
            compilation_result_id="{{ task_instance.xcom_pull('ai_data_prepare.create_compilation_result')["
                                  "'name'].split('/')[-1] }}",
            gcp_conn_id="cda_uat_goldusersummary",
        )
        # create_workflow_invocation
        create_workflow_invocation = dataform.DataformCreateWorkflowInvocationOperator(
            task_id="create_workflow_invocation",
            project_id="uat-gold-usersummary",
            region="us-east1",
            repository_id="pipelines",
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('ai_data_prepare.create_compilation_result')["
                                      "'name'] }}",
                "invocation_config": {
                    "included_tags": ["schd_cd_navi_srch_cat_data_prep"]
                },
            },
            gcp_conn_id="cda_uat_goldusersummary",
        )
        # get_workflow_invocation
        get_workflow_invocation = dataform.DataformGetWorkflowInvocationOperator(
            task_id="get_workflow_invocation",
            project_id="uat-gold-usersummary",
            region="us-east1",
            repository_id="pipelines",
            workflow_invocation_id="{{ task_instance.xcom_pull('ai_data_prepare.create_workflow_invocation')["
                                   "'name'].split('/')[-1] }}",
            gcp_conn_id="cda_uat_goldusersummary",
        )

        (
                create_compilation_result
                >> get_compilation_result
                >> Label("compilation_passed")
                >> create_workflow_invocation
                >> get_workflow_invocation
        )

    with TaskGroup(group_id="execute_batch_predictions") as execute_batch_predictions:
        # submit batch prediction job for ai model gemini-1.5-pro-001
        create_batch_prediction_job = CreateBatchPredictionJobOperator(
            task_id="create_batch_prediction_job",
            job_display_name="nav_search_categories_batch_prediction",
            model_name="publishers/google/models/gemini-1.5-pro-002",
            bigquery_source="bq://uat-gold-usersummary.it_ai_staging.tmp_ai_navigated_search_categories_request",
            bigquery_destination_prefix="bq://uat-gold-usersummary.it_ai_staging"
                                        ".tmp_ai_navigated_search_categories_response",
            region="us-east1",
            project_id="uat-gold-usersummary",
            gcp_conn_id="cda_uat_goldusersummary"
        )

    with TaskGroup(group_id="ai_data_post_process") as ai_data_post_process:
        # create compilation result for the current run
        create_compilation_result = dataform.DataformCreateCompilationResultOperator(
            task_id="create_compilation_result",
            project_id="uat-gold-usersummary",
            gcp_conn_id="cda_uat_goldusersummary",
            region="us-east1",
            repository_id="pipelines",
            compilation_result={
                "git_commitish": "main",
                "workspace": "projects/uat-gold-usersummary/locations/us-east1/repositories/pipelines/workspaces"
                             "/persona_mart",
                "code_compilation_config": {
                    "vars": {
                        "ds": "{{ data_interval_end  | ds }}",
                        "ds_nodash": "{{ data_interval_end | ds_nodash }}",
                        "ts": "{{ data_interval_end | ts }}",
                        "ts_nodash": "{{ data_interval_end | ts_nodash }}"
                    }
                },
            },
        )
        # get_compilation_result
        get_compilation_result = dataform.DataformGetCompilationResultOperator(
            task_id="get_compilation_result",
            project_id="uat-gold-usersummary",
            region="us-east1",
            repository_id="pipelines",
            compilation_result_id="{{ task_instance.xcom_pull('ai_data_post_process.create_compilation_result')["
                                  "'name'].split('/')[-1] }}",
            gcp_conn_id="cda_uat_goldusersummary",
        )
        # create_workflow_invocation
        create_workflow_invocation = dataform.DataformCreateWorkflowInvocationOperator(
            task_id="create_workflow_invocation",
            project_id="uat-gold-usersummary",
            region="us-east1",
            repository_id="pipelines",
            workflow_invocation={
                "compilation_result": "{{ task_instance.xcom_pull('ai_data_post_process.create_compilation_result')["
                                      "'name'] }}",
                "invocation_config": {
                    "included_tags": ["schd_cd_navi_srch_cat_data_prss"]
                },
            },
            gcp_conn_id="cda_uat_goldusersummary",
        )
        # get_workflow_invocation
        get_workflow_invocation = dataform.DataformGetWorkflowInvocationOperator(
            task_id="get_workflow_invocation",
            project_id="uat-gold-usersummary",
            region="us-east1",
            repository_id="pipelines",
            workflow_invocation_id="{{ task_instance.xcom_pull('ai_data_post_process.create_workflow_invocation')["
                                   "'name'].split('/')[-1] }}",
            gcp_conn_id="cda_uat_goldusersummary",
        )
        (
                create_compilation_result
                >> get_compilation_result
                >> Label("compilation_passed")
                >> create_workflow_invocation
                >> get_workflow_invocation

        )

    with TaskGroup(group_id="ai_data_export") as ai_data_export:
        # prepare data for export to mysql
        navigated_search_execute_bq_stored_procedure = BigQueryInsertJobOperator(
            task_id='execute_bq_stored_procedure',
            configuration={
                "query": {
                    "query": "{% include 'data/export/gcp/navigated_search_terms.sql' %}",
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='dis_ds_outbound'
        )
        # push data to cloud storage post validations
        navigated_search_push_2_gcs = BigQueryInsertJobOperator(
            task_id='push_data_2_gcs',
            configuration={
                "query": {
                    "query": "{% include 'data/export/gcp/exprt_gcs_navigated_search_categories.sql' %}",
                    "useLegacySql": False,
                }
            },
            gcp_conn_id='dis_ds_outbound'
        )
        # cleanup mysql table mapped to web UI
        navigated_search_truncate_mysql_table = MySqlOperator(
            task_id='truncate_table',
            mysql_conn_id='dis_searchv2db',
            sql="{% include 'data/export/mysql/truncate_mysql_table.sql' %}"
        )
        # run data transfer from bq to mysql using load data local infile
        navigated_search_bq_to_mysql_transfer = kubernetes_pod.KubernetesPodOperator(
            task_id="push_data_to_mysql",
            name="k_pod_push_data_to_mysql",
            namespace="composer-user-workloads",
            config_file="/home/airflow/composer_kube_config",
            image_pull_policy="Always",
            image=dag.user_defined_macros.get("data_sync_image_name")
        )

        # prepare_airflow_datasets for downstream connectivity
        prepare_airflow_datasets = bash.BashOperator(
            task_id="prepare_airflow_datasets",
            bash_command="mkdir -p {{ airflow_dataset_root }} \u0026\u0026 touch {{ airflow_dataset_root "
                         "}}/a2_tag_ds_{{ dis_ds_name }}.csv",
            outlets=[A2_TAG_NAVI_SEARCH_CAT],
        )
        (
                navigated_search_execute_bq_stored_procedure
                >> Label("Search_data_validated")
                >> navigated_search_push_2_gcs
                >> Label("pushed_to_gcs")
                >> navigated_search_truncate_mysql_table
                >> Label("mysql_table_truncated")
                >> navigated_search_bq_to_mysql_transfer
                >> Label("mysql_table_populated")
                >> prepare_airflow_datasets
        )

(ai_cms_config >>
 Label("categories_from_mysql_complete") >>
 ai_data_prepare >>
 Label("data_prepare_complete") >>
 execute_batch_predictions >>
 Label("vertex_ai_batch_process_complete") >>
 ai_data_post_process >>
 Label("navigated_search_post_process_complete") >>
 ai_data_export >>
 Label("navigated_search_transfer_to_mysql_complete"))