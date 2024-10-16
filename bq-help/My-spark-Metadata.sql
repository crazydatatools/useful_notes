select * from mapr_bigdata_uat_metahub.gcp_project where project_id=8 and status_flag=1
SELECT * FROM mapr_bigdata_uat_metahub.gcp_credentials where  project_id=8 #related to ingestion only
SELECT * FROM mapr_bigdata_uat_metahub.gcp_subjectarea where subjectarea_name in ('device_details') and status_flag=1 and  project_id in (2,8);

SELECT * FROM mapr_bigdata_uat_metahub.gcp_data_classification #to classify core or reference
SELECT * FROM mapr_bigdata_uat_metahub.gcp_exec_classification #to classify for ingesttion-2 or compute
SELECT * FROM mapr_bigdata_uat_metahub.gcp_subjectarea where subjectarea_name='email_activity' and project_id=2 ##'device_details' and  status_flag=1 --2,8 are active

SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where subjectarea_id in (29,107) #main table for ingesttion
SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_controller where sourceid in (SELECT sourceid FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where subjectarea_id in (26,59))
select * from mapr_bigdata_uat_metahub.gcp_scheduler_ingestion_mapping where sourceid in (SELECT sourceid FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where subjectarea_id in (26,59))

select * from mapr_bigdata_uat_metahub.gcp_scheduler_scheduler_trigger where group_binding_id in (select group_binding_id from mapr_bigdata_uat_metahub.gcp_scheduler_ingestion_mapping where sourceid in (SELECT sourceid FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where subjectarea_id in (26,59))
)
# controller for ingestion 
#only for fullload--noofmappers
#appendmode-- if id column then make it as 'Y' for data column mention as 'NA'
# for days cdcbacktracedays --applicable for datecolumns
# whereclause for s3 ingestion latency=9
#gcp_scheduler=--ingestion,compute--trigger script--clasify for ingestion or compute
SELECT * FROM mapr_bigdata_uat_metahub.gcp_scheduler where group_binding_id in (1,2)

SELECT * FROM mapr_bigdata_uat_metahub.gcp_airflow_scheduler where group_binding_id=2

#--generic_spark_ingestion_trigger-script.py
SELECT * FROM mapr_bigdata_uat_metahub.gcp_scheduler_ingestion_mapping
#gcp_ingestion_lastrun
SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_lastrun
# vw_gcp_ingestion_details
SELECT * FROM mapr_bigdata_uat_metahub.vw_gcp_ingestion_details where sourceid=43

SELECT * FROM mapr_bigdata_uat_metahub.vw_gcp_jobs_etl_tracking where check_flag != 'CanBeIgnored'

SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where sourceid =28 status_flag=0 to skip
SELECT * FROM mapr_bigdata_uat_metahub.gcp_scheduler_ingestion_mapping where sourceid =28 --skip-flag=1 to diable

SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where sourceid =45 status_flag=0 to skip--120 kt demo

Fro workflow
 SELECT * FROM mapr_bigdata_uat_metahub.gcp_config_objects where object_id in (217)
 select * from mapr_bigdata_uat_metahub.gcp_airflow_configobjs_mapping where object_id in (222)
SELECT * FROM mapr_bigdata_uat_metahub.gcp_scheduler_ingestion_mapping where sourceid =28





subjectarea_id in (SELECT subjectarea_id FROM mapr_bigdata_uat_metahub.gcp_subjectarea where subjectarea_name in ('email_hubs') and status_flag=1)