# useful_notes

# Hive Metadata extract from DB-Query----

SELECT D.NAME DatabaseName,t.TBL_NAME tbl_name, t.TBL_TYPE,D.OWNER_NAME,c.column_name, c.comment, c.type_name, c.integer_idx,
 tbl_id, create_time, owner, retention, t.sd_id, input_format, is_compressed, location,
 num_buckets, output_format, serde_id, s.cd_id
FROM TBLS t, SDS s, COLUMNS_V2 c, DBS D  
-- WHERE tbl_name = 'my_table'
WHERE t.SD_ID = s.SD_ID
AND s.cd_id = c.cd_id
AND t.DB_ID=D.DB_ID  and upper(c.column_name) like '%BIRTH_DT_SRC_CD%' and t.TBL_NAME not like '%dmsm10_%'

# =================Hive Metadata Report Query===================

select
 'Edge02--BigData' as 'Metastore',T.OWNER, D.NAME DatabaseName,T.TBL_NAME, T.TBL_TYPE,D.OWNER_NAME,FROM_UNIXTIME(T.CREATE_TIME) OBJ_CREATIONDATE,
 TIMESTAMPDIFF(month, FROM_UNIXTIME(T.CREATE_TIME), FROM_UNIXTIME(unix_timestamp(now()))) AgeTillDateInMonths,
  TIMESTAMPDIFF(day, FROM_UNIXTIME(T.CREATE_TIME), FROM_UNIXTIME(unix_timestamp(now()))) AgeTillDateInDays,
 S.LOCATION,
TP.numFiles, TP.numRows, TP.totalSize, TP.rawDataSize, TP.STATS,
SUBSTR(S.OUTPUT_FORMAT,30,20) OFORMAT,D.DB_LOCATION_URI,T.TBL_ID,S.SD_ID,S.CD_ID,D.DB_ID
from
  TBLS as T
  join DBS D on T.DB_ID=D.DB_ID
  join SDS S on T.SD_ID=S.SD_ID
  join -- pivot table_params from rows to columns
  (select TBL_ID,
   max(case when PARAM_KEY='numFiles' then PARAM_VALUE else null end) numFiles,
   max(case when PARAM_KEY='numRows' then PARAM_VALUE else null end) numRows,
   max(case when PARAM_KEY='totalSize' then PARAM_VALUE else null end) totalSize,
   max(case when PARAM_KEY='rawDataSize' then PARAM_VALUE else null end) rawDataSize,
   max(case when PARAM_KEY='COLUMN_STATS_ACCURATE' then PARAM_VALUE else null end) STATS
   from TABLE_PARAMS
   group by TBL_ID) as TP
   on T.TBL_ID=TP.TBL_ID
   where lower(T.TBL_NAME)  like '%projection%'
   order by (T.CREATE_TIME) desc;
Ctrl+Shift-V
 (Ctrl+K V) as a keyboard shortcut to create
 trl+Space, and VS Code will provide you with a context sensitive list of Markdown commands
# ====Hive Std=====================

SELECT c.column_name, tbl_name, c.comment, c.type_name, c.integer_idx,
 tbl_id, create_time, owner, retention, t.sd_id, tbl_type, input_format, is_compressed, location,
 num_buckets, output_format, serde_id, s.cd_id
FROM TBLS t, SDS s, COLUMNS_V2 c
-- WHERE tbl_name = 'my_table'
WHERE t.SD_ID = s.SD_ID
AND s.cd_id = c.cd_id
AND lower(tbl_name)  like '%tnacusd%'

# ==========oozie cmds==============

select app_name, concat(id,'@',status) COLUMN_NAME,concat(start_time,'@',end_time) COLUMN_VALUE,TIMESTAMPDIFF(minute,start_time,end_time) from WF_JOBS
where app_name='Prod_DMS_CUSTOMER_w_full_TDMACIN_016'
and start_time = (select max(start_time) from WF_JOBS where app_name='Prod_DMS_CUSTOMER_w_full_TDMACIN_016');

select app_name,app_path,parent_id,run,created_time,user_name,wf_instance, concat(id,'@',status) OBJNAME_STATUS,concat(start_time,'@',end_time) START_END,start_time,end_time,TIMESTAMPDIFF(minute,start_time,end_time) timetakeninmin from WF_JOBS
where start_time >= CURDATE()
order by start_time asc

# === Dynamic create table ==============

   select concat( 'show create table ' , T.NAME , '.', T.TBL_NAME,';') from (select DBS.NAME, TBLS.TBL_NAME from TBLS left join DBS on TBLS.DB_ID = DBS.DB_ID where DBS.NAME='prod_im_email_activity')  T

# =================Hive===========================

select df.OWNER ,count(1) cnt from (select
 'Edge02--BigData' as 'Metastore',T.OWNER, D.NAME DatabaseName,T.TBL_NAME, T.TBL_TYPE,D.OWNER_NAME,FROM_UNIXTIME(T.CREATE_TIME) OBJ_CREATIONDATE,
 TIMESTAMPDIFF(month, FROM_UNIXTIME(T.CREATE_TIME), FROM_UNIXTIME(unix_timestamp(now()))) AgeTillDateInMonths,
  TIMESTAMPDIFF(day, FROM_UNIXTIME(T.CREATE_TIME), FROM_UNIXTIME(unix_timestamp(now()))) AgeTillDateInDays,
 S.LOCATION,
TP.numFiles, TP.numRows, TP.totalSize, TP.rawDataSize, TP.STATS,
SUBSTR(S.OUTPUT_FORMAT,30,20) OFORMAT,D.DB_LOCATION_URI,T.TBL_ID,S.SD_ID,S.CD_ID,D.DB_ID
from
  TBLS as T
  join DBS D on T.DB_ID=D.DB_ID
  join SDS S on T.SD_ID=S.SD_ID
  join -- pivot table_params from rows to columns
  (select TBL_ID,
   max(case when PARAM_KEY='numFiles' then PARAM_VALUE else null end) numFiles,
   max(case when PARAM_KEY='numRows' then PARAM_VALUE else null end) numRows,
   max(case when PARAM_KEY='totalSize' then PARAM_VALUE else null end) totalSize,
   max(case when PARAM_KEY='rawDataSize' then PARAM_VALUE else null end) rawDataSize,
   max(case when PARAM_KEY='COLUMN_STATS_ACCURATE' then PARAM_VALUE else null end) STATS
   from TABLE_PARAMS
   group by TBL_ID) as TP
   on T.TBL_ID=TP.TBL_ID
  ) df group by df.OWNER
  order by 2 desc

  =====================Complex====
  ELECT db_name, tbl_name table_name, cluster_uri,
        cast(
        case when substring_index(hdfs_path, '/', -4) regexp '[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{2}'
             then substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -4))-1)
             when substring_index(hdfs_path, '/', -3) regexp '[0-9]{4}/[0-9]{2}/[0-9]{2}'
             then substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -3))-1)
             when substring_index(hdfs_path, '/', -1) regexp '20[0-9]{2}([\\._-]?[0-9][0-9]){2,5}'
               or substring_index(hdfs_path, '/', -1) regexp '1[3-6][0-9]{11}(-(PT|UTC|GMT|SCN)-[0-9]+)?'
               or substring_index(hdfs_path, '/', -1) regexp '[0-9]+([\\._-][0-9]+)?'
               or substring_index(hdfs_path, '/', -1) regexp '[vV][0-9]+([\\._-][0-9]+)?'
               or substring_index(hdfs_path, '/', -1) regexp '(prod|ei|qa|dev)_[0-9]+\\.[0-9]+\\.[0-9]+_20[01][0-9]([_-]?[0-9][0-9]){2,5}'
             then substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -1))-1)
             when hdfs_path regexp '/datepartition=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
               or hdfs_path regexp '/datepartition=[[:alnum:]]+'
             then substring(hdfs_path, 1, locate('/datepartition=', hdfs_path)-1)
             when hdfs_path regexp '/date_sk=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
             then substring(hdfs_path, 1, locate('/date_sk=', hdfs_path)-1)
             when hdfs_path regexp '/ds=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
             then substring(hdfs_path, 1, locate('/ds=', hdfs_path)-1)
             when hdfs_path regexp '/dt=20[01][0-9]([\\._-]?[0-9][0-9]){2,3}'
             then substring(hdfs_path, 1, locate('/dt=', hdfs_path)-1)
             when hdfs_path regexp '^/[[:alnum:]]+/[[:alnum:]]+/[[:alnum:]]+/20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring_index(hdfs_path, '/', 4), '/_',
                         substring(hdfs_path, length(substring_index(hdfs_path, '/', 5))+1))
             when hdfs_path regexp '^/[[:alnum:]]+/[[:alnum:]]+/20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring_index(hdfs_path, '/', 3), '/_',
                         substring(hdfs_path, length(substring_index(hdfs_path, '/', 4))+1))
             when substring_index(hdfs_path, '/', -3) regexp '^(prod|ei|qa|dev)_[0-9]+\\.[0-9]+\\.[0-9]+_20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -3))-1), '/*/',
                         substring_index(hdfs_path, '/', -2))
             when substring_index(hdfs_path, '/', -2) regexp '^(prod|ei|qa|dev)_[0-9]+\\.[0-9]+\\.[0-9]+_20[01][0-9]([\\._-]?[0-9][0-9]){2,5}/'
             then concat(substring(hdfs_path, 1, hdfs_path_len - length(substring_index(hdfs_path, '/', -2))-1), '/*/',
                         substring_index(hdfs_path, '/', -1))
             else hdfs_path
        end as char(300)) abstract_hdfs_path
      FROM (
select d.NAME DB_NAME, t.TBL_NAME,
            substring_index(s.LOCATION, '/', 3) cluster_uri,
            substring(s.LOCATION, length(substring_index(s.LOCATION, '/', 3))+1) hdfs_path,
            length(s.LOCATION) - length(substring_index(s.LOCATION, '/', 3)) as hdfs_path_len
            ,p.PART_NAME
          from SDS s
               join TBLS t
            on s.SD_ID = t.SD_ID
               join DBS d
            on t.DB_ID = d.DB_ID
               left join PARTITIONS p
            on t.TBL_ID = p.TBL_ID
          where p.PART_ID is null
            and LOCATION is not null
          union all
          select d.NAME DB_NAME, t.TBL_NAME,
            substring_index(s.LOCATION, '/', 3) cluster_uri,
            substring(s.LOCATION, length(substring_index(s.LOCATION, '/', 3))+1) hdfs_path,
            length(s.LOCATION) - length(substring_index(s.LOCATION, '/', 3)) as hdfs_path_len
          from SDS s
               join (select TBL_ID, MAX(SD_ID) SD_ID from PARTITIONS group by 1) p
            on s.SD_ID = p.SD_ID
               join TBLS t
            on p.TBL_ID = t.TBL_ID
               join DBS d
            on t.DB_ID = d.DB_ID
            where not LOCATION like 'hdfs:%__HIVE_DEFAULT_PARTITION__%'
      ) x where hdfs_path not like '/tmp/%'
      order by 1,2

## ===========Drop columns===

   SELECT D.NAME DatabaseName,t.TBL_NAME tbl_name, t.TBL_TYPE,D.OWNER_NAME,c.column_name, c.comment, c.type_name, c.integer_idx,
 tbl_id, create_time, owner, retention, t.sd_id, input_format, is_compressed, location,
 num_buckets, output_format, serde_id, s.cd_id
FROM TBLS t, SDS s, COLUMNS_V2 c, DBS D  
-- WHERE tbl_name = 'my_table'
WHERE t.SD_ID = s.SD_ID
AND s.cd_id = c.cd_id
AND t.DB_ID=D.DB_ID
 and c.column_name in ('er')
and t.TBL_NAME like 'dmsm10_%' and D.NAME='im'

# ===QA metaa Queries

SELECT
    inv.item_name,
    invty.inventory_item__type_code,
    invty.inventory_item_type_code_desc,
    itmassign.local_name,
    itmassign.folder_name,
    itmloc.location_area_code,
    itmloc.location_area_code_desc,
    datastruct.column_position_number,
    datastruct.column_name,
    datastruct.data_type_dtls,
    datastruct.allow_nulls_ind,
    datastruct.business_definition
FROM
    bdinventory inv
        JOIN
    bdinventoryitemtype invty ON inv.inventory_item_type_id = invty.inventory_item_type_id
        JOIN
    itemlocationassignment itmassign ON inv.item_id = itmassign.item_id
        JOIN
    inventoryitemlocationarea itmloc ON itmloc.inventory_item_location_area_id = itmassign.inventory_item_location_area_id
        JOIN
    datastructure datastruct ON datastruct.item_location_assignment_id = itmassign.item_location_assignment_id
WHERE
    inv.item_name = 'tdmefcu'
        --AND itmloc.location_area_code = ''

SELECT BDI.ITEM_NAME,DBP.project_name,
    invty.inventory_item__type_code,
    IIL.LOCATION_AREA_CODE,
    IIL.SCHEMA_LOC,ILA.LOCAL_NAME ,ILA.folder_name
FROM bdinventory BDI INNER JOIN dbinventorystatushistory DBISH ON (BDI.ITEM_ID = DBISH.ITEM_ID)
 JOIN
    bdinventoryitemtype invty ON BDI.inventory_item_type_id = invty.inventory_item_type_id
INNER JOIN dbproject DBP
ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
INNER JOIN projectitemrelease PIR
ON (PIR.BD_PROJECT_ID = DBP.BD_PROJECT_ID AND PIR.ITEM_ID = BDI.ITEM_ID)
INNER JOIN itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
WHERE DBISH.STATUS_CODE = 'A' ;

SELECT BDI.ITEM_NAME, IIL.LOCATION_AREA_CODE,IIL.SCHEMA_LOC FROM bdinventory BDI INNER JOIN dbinventorystatushistory DBISH ON (BDI.ITEM_ID = DBISH.ITEM_ID)
INNER JOIN dbproject DBP
ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
INNER JOIN projectitemrelease PIR
ON (PIR.BD_PROJECT_ID = DBP.BD_PROJECT_ID AND PIR.ITEM_ID = BDI.ITEM_ID)
INNER JOIN itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
WHERE DBISH.STATUS_CODE = 'A' AND
(BDI.ITEM_NAME = ILA.LOCAL_NAME OR ILA.LOCAL_NAME = 'club' OR ILA.LOCAL_NAME like 'lookup%')
and DBP.PROJECT_NAME = 'Angels'   ;

SELECT ILA.LOCAL_NAME, IIL.LOCATION_AREA_CODE,IIL.SCHEMA_LOC FROM bdinventory BDI INNER JOIN dbinventorystatushistory DBISH ON (BDI.ITEM_ID = DBISH.ITEM_ID)
INNER JOIN dbproject DBP
ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
INNER JOIN projectitemrelease PIR
ON (PIR.BD_PROJECT_ID = DBP.BD_PROJECT_ID AND PIR.ITEM_ID = BDI.ITEM_ID)
INNER JOIN itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
WHERE DBISH.STATUS_CODE = 'A'  ;

SELECT BDI.ITEM_NAME, IIL.LOCATION_AREA_CODE,IIL.SCHEMA_LOC
FROM
bdinventory BDI
INNER JOIN dbinventorystatushistory DBISH
ON (BDI.ITEM_ID = DBISH.ITEM_ID)
INNER JOIN dbproject DBP
ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
INNER JOIN projectitemrelease PIR
ON (PIR.BD_PROJECT_ID = DBP.BD_PROJECT_ID AND PIR.ITEM_ID = BDI.ITEM_ID)
INNER JOIN itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
WHERE DBISH.STATUS_CODE = 'A' AND IIL.inventory_item_location_area_id  IN (SELECT IIL.inventory_item_location_area_id FROM
bdinventory BDI
INNER JOIN itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
INNER JOIN dbproject DBP ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
where DBP.PROJECT_NAME = 'Angels'
 and IIL.location_area_code_desc='Interpretive Model')
 ;

 select bdi.item_name,bdiit.inventory_item__type_code from NexusQA.bdinventory bdi
inner join NexusQA.bdinventoryitemtype bdiit
on (bdi.inventory_item_type_id = bdiit.inventory_item_type_id)
inner join NexusQA.dbproject dbp
on (dbp.bd_project_id = bdi.bd_project_id)
where dbp.project_name ='Braves'

select inv.item_name,invty.inventory_item__type_code,invty.inventory_item_type_code_desc,itmassign.local_name,itmassign.folder_name,
itmloc.location_area_code,itmloc.location_area_code_desc,
datastruct.column_position_number,datastruct.data_type_dtls,datastruct.column_name,
datastruct.allow_nulls_ind,datastruct.business_definition
 from  bdinventory inv
  join  bdinventoryitemtype invty  on inv.inventory_item_type_id=invty.inventory_item_type_id
  join itemlocationassignment itmassign on inv.item_id=itmassign.item_id
    join inventoryitemlocationarea itmloc on itmloc.inventory_item_location_area_id=itmassign.inventory_item_location_area_id
     join datastructure datastruct on datastruct.item_location_assignment_id=itmassign.item_location_assignment_id
     where inv.item_name= 'tnbefhh';

     select inv.item_name from bdinventory inv join bdinventoryitemtype invty on inv.inventory_item_type_id=invty.inventory_item_type_id 
join dbproject proj on proj.bd_project_id=inv.bd_project_id join dbinventorystatushistory dbstatus
on dbstatus.item_id=inv.item_id and
dbstatus.status_code='A' and dbstatus.appended_status_flag='A' and
inventory_item__type_code IN (  'CORE','HDLP','REFERENCE')
and proj.project_name =  'Braves';

select test_log.pass_ind from (select * FROM test_executionlog  where test_execution_log_id=(SELECT test_execution_log_id FROM test_executionlog
where id =(select max(id) from test_executionlog))) test_log join test tm on tm.test_id=test_log.test_id JOIN itemlocationassignment ila ON tm.to_item_location_assignment_id = ila.item_location_assignment_id
JOIN inventoryitemlocationarea itmloc ON itmloc.inventory_item_location_area_id = ila.inventory_item_location_area_id JOIN bdinventory inv ON inv.item_id = ila.item_id JOIN bdinventoryitemtype invty ON inv.inventory_item_type_id = invty.inventory_item_type_id
JOIN testscenario ts ON ts.test_scenario_id = tm.test_scenario_id JOIN testclass tc ON ts.test_class_id = tc.test_class_id JOIN testscript testscrpt ON ts.test_scenario_id = testscrpt.test_scenario_id
where item_name='tdmpraf' order by test_log.test_id asc ;

select distinct item_code, item_name
from ( select   inv.item_name itname,inv.item_id id,invty.inventory_item__type_code item_code
from bdinventory inv join bdinventoryitemtype invty on inv.inventory_item_type_id=invty.inventory_item_type_id  join AdminEtlCustData_Dev.itemassociation ia
 on  ia.item_id=inv.item_id  where inv.item_name='tdmefcu' ) base  join AdminEtlCustData_Dev.itemassociation ia   on  base.id=ia.item_id    join bdinventory inv on inv.item_id=ia.associatied_to_item_id
join bdinventoryitemtype invty on inv.inventory_item_type_id=invty.inventory_item_type_id

select item_id, item_name from bdinventory where item_id in (select it.associatied_to_item_id
from bdinventory bd join itemassociation it on  (bd.item_id = it.item_id )  where item_name ='tdmefcu')

# --Not null

select inv.item_name,invty.inventory_item__type_code,invty.inventory_item_type_code_desc,itmassign.local_name,itmassign.folder_name,
itmloc.location_area_code,itmloc.location_area_code_desc,
datastruct.column_position_number,datastruct.data_type_dtls,datastruct.column_name,
datastruct.allow_nulls_ind,datastruct.business_definition
 from  bdinventory inv
  join  bdinventoryitemtype invty  on inv.inventory_item_type_id=invty.inventory_item_type_id
  join itemlocationassignment itmassign on inv.item_id=itmassign.item_id
    join inventoryitemlocationarea itmloc on itmloc.inventory_item_location_area_id=itmassign.inventory_item_location_area_id
     join datastructure datastruct on datastruct.item_location_assignment_id=itmassign.item_location_assignment_id
     where inv.item_name='tdmefcu' and allow_nulls_ind = 1 and itmloc.location_area_code= 'CM' ;

SELECT BDI.ITEM_NAME, IIL.LOCATION_AREA_CODE,IIL.SCHEMA_LOC FROM AdminEtlCustData_Dev.bdinventory BDI INNER JOIN AdminEtlCustData_Dev.dbinventorystatushistory DBISH ON (BDI.ITEM_ID = DBISH.ITEM_ID)
INNER JOIN AdminEtlCustData_Dev.dbproject DBP
ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
INNER JOIN AdminEtlCustData_Dev.projectitemrelease PIR
ON (PIR.BD_PROJECT_ID = DBP.BD_PROJECT_ID AND PIR.ITEM_ID = BDI.ITEM_ID)
INNER JOIN AdminEtlCustData_Dev.itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN AdminEtlCustData_Dev.inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
WHERE DBISH.STATUS_CODE = 'A'

SELECT distinct ITEM_NAME,DBP.project_name,
    invty.inventory_item__type_code,
       MAX(case when IIL.LOCATION_AREA_CODE='SOURCE' then schema_loc else '' end) as 'SOURCE',
     MAX(case when IIL.LOCATION_AREA_CODE='CM' then schema_loc  else '' end) as 'CM',
      MAX(case when IIL.LOCATION_AREA_CODE='IM' then schema_loc else '' end) as 'IM',

          MAX(case when IIL.LOCATION_AREA_CODE='SOURCE' then ILA.LOCAL_NAME end) as 'SA_NAME',
      MAX(case when IIL.LOCATION_AREA_CODE='CM' then ILA.LOCAL_NAME end) as 'CM_NAME',
       MAX(case when IIL.LOCATION_AREA_CODE='IM' then ILA.LOCAL_NAME end) as 'IM_NAME',
              MAX(case when IIL.LOCATION_AREA_CODE='SOURCE' then ILA.folder_name end) as 'SA_URL',
      MAX(case when IIL.LOCATION_AREA_CODE='CM' then ILA.folder_name end) as 'CM_URL',
       MAX(case when IIL.LOCATION_AREA_CODE='IM' then ILA.folder_name end) as 'IM_URL'
FROM bdinventory BDI INNER JOIN dbinventorystatushistory DBISH ON (BDI.ITEM_ID = DBISH.ITEM_ID)
 JOIN
    bdinventoryitemtype invty ON BDI.inventory_item_type_id = invty.inventory_item_type_id
INNER JOIN dbproject DBP
ON (DBP.BD_PROJECT_ID = BDI.BD_PROJECT_ID)
INNER JOIN projectitemrelease PIR
ON (PIR.BD_PROJECT_ID = DBP.BD_PROJECT_ID AND PIR.ITEM_ID = BDI.ITEM_ID)
INNER JOIN itemlocationassignment ILA
ON (BDI.ITEM_ID = ILA.ITEM_ID)
INNER JOIN inventoryitemlocationarea IIL
ON (IIL.INVENTORY_ITEM_LOCATION_AREA_ID = ILA.INVENTORY_ITEM_LOCATION_AREA_ID)
where ITEM_NAME='tdmcudq'
group by  ITEM_NAME,DBP.project_name,
    invty.inventory_item__type_code

 select  TBL_NAME ,count(*) COLUMN_VALUE  from (select tbls.TBL_NAME, c.COLUMN_NAME, db.name, TYPE_NAME , db.owner_name, location
                 FROM SDS s, COLUMNS_V2 c , DBS db, TBLS tbls
                 where tbls.SD_ID = s.SD_ID AND s.cd_id = c.cd_id  and db.db_id = tbls.db_id  
                 and location IS NOT  NULL)   tt  
