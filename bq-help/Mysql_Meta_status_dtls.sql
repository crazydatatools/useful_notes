SELECT * FROM mapr_bigdata_uat_metahub.vw_gcp_jobs_etl_tracking where check_flag != 'CanBeIgnored'

SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where sourceid =44 and status_flag=1 # 0 to skip
SELECT * FROM mapr_bigdata_uat_metahub.gcp_scheduler_ingestion_mapping where sourceid =44 --skip-flag=1 to disable
SELECT * FROM mapr_bigdata_uat_metahub.gcp_scheduler where group_binding_id=6
SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_lastrun where mapping_id =44 and lastrundate=current_date-3

SELECT * FROM mapr_bigdata_uat_metahub.gcp_ingestion_sources where sourceid =45 status_flag=0 to skip--120 kt demo

Fro workflow
 SELECT * FROM mapr_bigdata_uat_metahub.gcp_config_objects where object_id in (217)
 select * from mapr_bigdata_uat_metahub.gcp_airflow_configobjs_mapping where object_id in (222)
 #`prod-gold-bi-reporting.online_cohort_kpi.rpt_acq_new_mailables_engagement_revenue_md_weekly`
 # vw_gcp_ingestion_details
SELECT * FROM mapr_bigdata_uat_metahub.vw_gcp_ingestion_details where sourceid=44
# vw_gcp_compute_details
select * from vw_gcp_compute_basic_details where subjectarea_name='email_activity'
 
 hdl_loadtype	hdl_subtype	frequency
FULL				TABLE	daily
							hourly
INCREMENTAL			RELOAD	 
					LASTMODIFIED	 
#Adhoc--
#Entry to credentails--only for ingestion
SELECT *, cast(aes_decrypt(`password`,'bigdataetl_ingestion') as char charset utf8) AS `decrypted_password`  FROM mapr_bigdata_uat_metahub.gcp_credentials;
insert into mapr_bigdata_uat_metahub.gcp_credentials
values (9,3,'s3','service account details connecting to s3 connectivity','p_bd_ms_etl_svc',AES_ENCRYPT('Had00p123','bigdataetl_ingestion'),'NA');

mysql -h maprsqldb.prod.pch.com -u p_bd_ms_mysql_svc -p   S3ntr@L-st@shnU           
mapr_bigdata_uat_metahub       