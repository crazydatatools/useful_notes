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


##call mysql

mysql -h  maprsqldb.prod.com -u u_bd_ms_mysql_svc -p

#2.DENSE_RANK( ): Seamless Ranking without Gaps
It is similar to RANK(), but it eliminates gaps in the ranking sequence. The DENSE_RANK() assigns a unique, dense rank to each row within a window, ensuring a continuous ranking without skipping numbers.

#3.ROW_NUMBER(): Sequential Identification
It assigns a unique sequential integer to each row within a specified partition of a result set. The ROW_NUMBER() function in SQL is commonly used in scenarios where you need to assign a unique ranking or sequence to rows within a specified group or partition.

#4.PERCENT_RANK( ): Quantifying Relative Position
It calculates the percentile rank of each row within a window, providing a normalized measure ranging from 0 to 1. The PERCENT_RANK()is particularly useful when assessing the relative position of data in a distribution.

#5.CUME_DIST( ): Cumulative Distribution Unveiled

It computes the cumulative distribution of each row within a window, revealing its relative position in the entire dataset. The CUME_DIST()delivers a value between 0 and 1, indicating the cumulative percentage of data less than or equal to the current row.

#6.NTILE( ): Dividing Data into Quantiles

It breaks down the dataset into quantile buckets, allowing for the classification of rows based on their relative position. This function assigns a bucket number (1-based) to each row, facilitating the segmentation of data into equal parts.


## Slow Changin Dimensions

##SCD Type 1
- A Slowly Changing Dimension Type 1 refers to an instance where the latest snapshot of a record is maintained in the data warehouse, without any historical records.
SCD Type 1 are commonly used to correct errors in a dimension updating values that were wrong or irrelevant. No history is retained,A SCD Type 1 would update the record to reflect my name accurately, correcting the error in the name spelling. The incorrect record would be updated without adding any new rows or columns to the data warehouse.

##SCD Type 2
- So what is used when we want to maintain the history? very time there is a change in the source system, a new row will be added to the data warehouse table. In the resulting table, there will be more records, but the prior history is retained and queryable.In SCD Type 2, there would be two methods to distinguish between current and historical records:
A column signifying the current record and to_date and from_date columns.

##SCD Type 3
we would have one row, but an additional column to denote the previous and current salary, along with a date the record was updated:this approach is that it offers the ability to compare a record before and after a change occurred, however limited history is preserved.

##SCD Type 4
Historical data will be maintained as in SCD Type 2 but the distinction here is that the history will be maintained on a separate table within the data warehouse. The current record will be included in the primary table.

# To find the median of the records--Middle record
  ```
  With ranks AS (
      SELECT LAT_N, RANK() OVER(ORDER BY LAT_N DESC) as ranking
      FROM STATION
  )
  SELECT ROUND(LAT_N,4)
  FROM ranks
  WHERE ranking IN (
      SELECT ROUND((COUNT(*)/2)) 
      FROM STATION
      WHERE LAT_N IS NOT NULL);
#To select Vowels
      select DISTINCT CITY from station where CITY REGEXP  '^[^AEIOUaeiou]'

-- Identify the employee who received at least  3 year over year increase in salaries!
INSERT INTO employee_salary (employee_id, name, year, salary, department) VALUES
(125, 'John Doe', 2021, 50000, 'Sales'),
(125, 'John Doe', 2022, 52000, 'Sales'),
with pre_sal as(
select *,lag(salary,1) over (partition by employee_id order by year ) prev_sal from employee_salary
) select employee_id,
    name,count(1) from pre_sal where salary>prev_sal
GROUP BY employee_id, name
HAVING COUNT(*) >= 3

--Calculate each store running total Growth ratio compare to previous month return store name, sales amount, running total, growth ratio 
INSERT INTO sales (store_name, sale_date, sales_amount) 
VALUES
('A', '2024-01-01', 1000.00),
('A', '2024-02-01', 1500.00),
WITH cte_monthly_sale
AS
(
SELECT *,
    SUM(sales_amount) OVER(PARTITION BY store_name ORDER  BY sale_date) as running_total,
    LAG(sales_amount, 1) OVER(PARTITION BY store_name ORDER  BY sale_date) as last_month_sale
FROM sales
)
SELECT
    store_name,
    sale_date,
    sales_amount,
    running_total,
    last_month_sale,
    (sales_amount - last_month_sale)/last_month_sale * 100
FROM cte_monthly_sale

-- Write a SQL query to find the top 2 restaurants in each city with the highest average rating.
 If two restaurants have the same average rating, they should have the same rank.
 
 select * from(
 select r.restaurant_id,r.restaurant_name,c.city_name,avg(rating) as avg_rating, dense_rank() over (partition by c.city_name order by avg(rating) desc) kk  
 from restaurants r
 join cities c on r.city_id=c.city_id
 join orders o on r.restaurant_id=o.restaurant_id
 group by r.restaurant_id,r.restaurant_name,c.city_name)l
 where kk<=2


  Write a query to find the top 3 products with the 
highest sales volume (total quantity sold) for each quarter of the year 2023.

 Products (product_id, product_name, category, price)
 Sales (sale_id, product_id, customer_id, sale_date, quantity, amount)
 
 select * from (
select EXTRACT(QUARTER FROM sale_date) qtr ,p.product_id, product_name,
sum(quantity) sum_sal_vol,
dense_rank() over(partition by EXTRACT(QUARTER FROM sale_date) order by sum(quantity) desc)  k
from Products p
join Sales s on p.product_id=s.product_id
where extract(year from s.sale_date)=2023
group by EXTRACT(QUARTER FROM sale_date),p.product_id, product_name
) k where k<=3

/*
Question:
Analyze Spotify's user listening data to find out 
which genre of music has the highest average listening time per user.
*/
-- users (user_id, user_name, country)
-- plays (user_id, song_id, genre, listening_time) 

SELECT genre, 
       AVG(total_listening_time) AS avg_listening_time_per_user
FROM (
    SELECT genre, user_id, SUM(listening_time) AS total_listening_time
    FROM plays
    GROUP BY genre, user_id
) AS user_genre_listening
GROUP BY genre
ORDER BY avg_listening_time_per_user DESC
LIMIT 1;

-- Write a query to calculate the average monthly sales for each category!
-- return category that has highest average sale in each month!
	-- Products (product_id, product_name, category, price) 
	-- Customers (customer_id, customer_name, customer_city, customer_state)
	-- Sales (sale_id, product_id, customer_id, sale_date, quantity, amount)

select * from (
select extract(month from sale_date) sales_monthly,
p.category,
avg(amount) avg_sal_amt ,
rank() over(partition by extract(month from sale_date) order by avg(amount) desc) rnk
from   Sales s 
join Products p on s.product_id=p.product_id
join Customers c on c.customer_id=c.customer_id
group by  extract(month from sale_date) ,
p.category
) k where rnk=1
#https://github.com/najirh/100_days_challenge_community/blob/main/day_12_q2.sql
-- Write a query to identify the customers who spent the most 
-- money during the Big Billion Days Sale (November 24-27) in 2023. return customer name, id and total spent

select c.customer_name,c.customer_id,sum(amount) sum_sal_amt ,
rank() over (partition by customer_id order by sum(amount) desc) rnk
from   Sales s 
join Customers c on c.customer_id=c.customer_id
where date(sale_date) between '2023-11-24' and  '2023-11-27' 
group by c.customer_name,c.customer_id
order by sum_sal_amt desc
limit 1

-- Write a SQL query to find the customer IDs who have made purchases consecutively in every month up to the current month (July 2024) of this year.
-- amazon_sales (customer_id, purchase_date, amount)

SELECT customer_id
FROM amazon_sales
WHERE purchase_date >= '2024-01-01' AND purchase_date < '2024-08-01'  -- restrict to Jan to Jul 2024
GROUP BY customer_id
HAVING COUNT(DISTINCT MONTH(purchase_date)) = 7; 

/*
You are given two tables: Restaurants and Orders. After receiving an order, 
each restaurant has 15 minutes to dispatch it. Dispatch times are categorized as follows:

on_time_dispatch: Dispatched within 15 minutes of order received.
late_dispatch: Dispatched between 15 and 20 minutes after order received.
super_late_dispatch: Dispatched after 20 minutes.
Task: Write an SQL query to count the number of dispatched orders in each category for each restaurant.
*/
-- Restaurants (name, location)
-- Orders (restaurant_id, order_time, dispatch_time) 
select name,
count(case when dispatch_category='on_time_dispatch' then 1 end) cnt_on_time_dispatch,
count(case when dispatch_category='late_dispatch' then 1 end) cnt_late_dispatch,
count(case when dispatch_category='super_late_dispatch' then 1 end) cnt_super_late_dispatch from (
select r.name,
case 
	when o.dispatch_time< DATE_ADD(o.order_time, INTERVAL 15 MINUTE)   THEN 'on_time_dispatch'
    when o.dispatch_time> DATE_ADD(o.order_time, INTERVAL 15 MINUTE)  and o.dispatch_time< DATE_ADD(o.order_time, INTERVAL 20 MINUTE)  then 'late_dispatch'
	else 'super_late_dispatch' end as dispatch_category
    
from Restaurants r
join Orders o on r.id=o.restaurant_id
) k group by name


-- Find out each users and productivity time in hour!
-- productivity time = login - logout time
-- user_activities (user_id, activity, activity_time)
(2, 'Logout', '2024-01-01 18:00:00'),
(3, 'Login', '2024-01-01 08:30:00'),
(3, 'Logout', '2024-01-01 12:30:00');
WITH login_logout_table
AS
(
SELECT 
    *,
    LAG(activity_time) OVER(PARTITION BY user_id ORDER BY activity_time) as prev_activity_time,
    LAG(activity) OVER(PARTITION BY user_id ORDER BY activity_time) as prev_activity
FROM user_activities
),
session_table
AS
(
SELECT
    user_id,
    prev_activity as login,
    prev_activity_time as login_time,
    activity as logout,
    activity_time as logout_time,
TIMESTAMPDIFF(HOUR,prev_activity_time,activity_time) as productivity_hour
FROM login_logout_table
WHERE 
    prev_activity = 'Login'
    AND
    activity = 'Logout'
)
SELECT 
     user_id,
     SUM(productivity_hour)
FROM session_table
GROUP BY user_id;
-- Write SQL Query to find users whose email addresses contain only lowercase letters before the @ symbol


SELECT * FROM users
WHERE mail REGEXP '^[a-z.0-9]+@[a-z]+\.[a-z]+$'; 


-- Given a user_activity table, write a SQL query to find all users who have logged in on at least 3 consecutive days.
INSERT INTO user_activity (user_id, login_date) VALUES
(1, '2024-08-01'),
(1, '2024-08-02'),
WITH steak_table
AS    
(SELECT 
    user_id,
    login_date,
    CASE
        WHEN login_date = LAG(login_date) OVER(PARTITION BY user_id ORDER BY login_date) + INTERVAL '1 day' THEN 1
        ELSE 0
    END as steaks
FROM user_activity),
steak2
AS
(SELECT 
    user_id,
    login_date,
    SUM(steaks) OVER(PARTITION BY user_id ORDER BY login_date) as consecutive_login
FROM steak_table    
)
SELECT 
    distinct user_id
FROM steak2
WHERE consecutive_login >=2




 -- spotify (user_id, song_id, play_date) 
/*
Question:
Identifying Trending Songs:
Spotify wants to identify songs that have suddenly gained popularity within a week.

Write a SQL query to find the song_id and week_start 
date of all songs that had a play count increase of 
at least 300% from one week to the next. 
Consider weeks starting on Mondays.
*/
INSERT INTO spotify (user_id, song_id, play_date) VALUES
(333, 101, '2023-01-17'), -- Week 1
(233, 101, '2023-01-17'), -- Week 1

WITH weekly_plays
AS    
(SELECT 
     song_id,
     week(play_date) as week_start_day,
     COUNT(*)::numeric as play_cnt   
FROM spotify
GROUP BY 1, 2
),
prev_plays
AS    
(SELECT 
     song_id,
     week_start_day,
     play_cnt,   
    LAG(play_cnt) OVER(PARTITION BY song_id ORDER BY week_start_day) as prev_w_play_cnt
FROM weekly_plays
),
growth_ratio
AS    
(SELECT 
     song_id,
     week_start_day,
     play_cnt,   
     prev_w_play_cnt,
    ROUND((play_cnt-prev_w_play_cnt)/prev_w_play_cnt * 100, 2) as growth_ratio
FROM prev_plays
WHERE play_cnt > prev_w_play_cnt
)
SELECT 
     song_id,
     week_start_day 
FROM growth_ratio    
WHERE growth_ratio > 300;
    
-- Retrieve all Ids of a person whose rating is greater than friend's id
-- If person does not have any friend, retrieve only their id only if rating greater than 85
Friends (id, friend_id)
Ratings (id, rating)

SELECT 
    -- f.id,
    -- f.friend_id,
    -- r.rating as rating,
    DISTINCT(f.id)
    
FROM Friends as f
LEFT JOIN Ratings as r
ON r.id = f.id
LEFT JOIN Ratings as r2
ON f.friend_id = r2.id
WHERE 
    (f.friend_id IS NOT NULL AND r.rating > r2.rating)    
    OR
    (f.friend_id IS NULL AND r.rating > 85) 

-- Write SQL query to find average processing time by each machine!
INSERT INTO Activity (machine_id, process_id, activity_type, timestamp)
VALUES
(1, 1, 'start', 10.5),
(1, 1, 'end', 15.0),
(1, 2, 'start', 20.0),
(1, 2, 'end', 25.5),
SELECT machine_id,
       AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_processing_time_seconds
FROM (
    SELECT machine_id, 
           process_id,
           MAX(CASE WHEN activity_type = 'start' THEN timestamp END) AS start_time,
           MAX(CASE WHEN activity_type = 'end' THEN timestamp END) AS end_time
    FROM Activity
    GROUP BY machine_id, process_id
) AS process_durations
WHERE start_time IS NOT NULL AND end_time IS NOT NULL
GROUP BY machine_id;
--To find each customer's latest and second latest order amounts, we can use window functions such as 
ROW_NUMBER or RANK to assign a rank to each order by date for each customer. Here’s a query to achieve this:orders (customer_id, order_date, amount)

WITH ranked_orders AS (
    SELECT customer_id,
           amount,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS order_rank
    FROM orders
)
SELECT customer_id,
       MAX(CASE WHEN order_rank = 1 THEN amount END) AS latest_order_amount,
       MAX(CASE WHEN order_rank = 2 THEN amount END) AS second_latest_order_amount
FROM ranked_orders
WHERE order_rank IN (1, 2)
GROUP BY customer_id;
--To determine each employee's level in the hierarchy, we can use a recursive common table expression (CTE).
 Employees without a manager are at level 1, and for each subsequent level, we increment the level by 1.
WITH RECURSIVE EmployeeHierarchy AS (
    -- Base case: Employees with no manager are at level 1
    SELECT id, name, manager_id, 1 AS level
    FROM Employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: Find direct reports and increment their level by 1
    SELECT e.id, e.name, e.manager_id, eh.level + 1 AS level
    FROM Employees e
    JOIN EmployeeHierarchy eh ON e.manager_id = eh.id
)

SELECT id, name, level
FROM EmployeeHierarchy
ORDER BY id;

SELECT e1.id,
       e1.name,
       CASE 
           WHEN e1.manager_id IS NULL THEN 1
           WHEN e2.manager_id IS NULL THEN 2
           WHEN e3.manager_id IS NULL THEN 3
           ELSE 4
       END AS level
FROM Employees e1
LEFT JOIN Employees e2 ON e1.manager_id = e2.id
LEFT JOIN Employees e3 ON e2.manager_id = e3.id
LEFT JOIN Employees e4 ON e3.manager_id = e4.id
ORDER BY e1.id;


  ```

  # New Emloyee based quries
  ```
  /*
Enter your query here.
*/
with pg as(
    select task_id,start_date,end_date,
     DATE_ADD(Start_Date, interval (-ROW_NUMBER() OVER (ORDER BY Start_Date)) day) AS Project_Group_date
    from Projects
),
Projectstart_endDate AS (
    SELECT 
        MIN(start_date) AS Project_StartDate,
        MAX(end_date) AS Project_EndDate,
 DATEDIFF(  MAX(End_Date),MIN(Start_Date)) + 1 AS Duration
    FROM pg
    GROUP BY Project_Group_date
)
SELECT 
    Project_StartDate, 
    Project_EndDate
FROM Projectstart_endDate
ORDER BY 
    Duration ASC,
    Project_StartDate ASC;



select * from Students s
join Packages p
on s.id=p.id

SELECT s.hacker_id, h.name, SUM(s.max_score) AS total_score
FROM (
    SELECT hacker_id, challenge_id, MAX(score) AS max_score
    FROM Submissions
    GROUP BY hacker_id, challenge_id
) s
JOIN Hackers h ON s.hacker_id = h.hacker_id
GROUP BY s.hacker_id, h.name
HAVING SUM(s.max_score) > 0
ORDER BY total_score DESC, s.hacker_id;

SELECT 
    CASE WHEN a + b > c AND b + c > a AND c + a > b
        THEN  
            CASE WHEN a = b AND b = c
                THEN 'Equilateral'
            ELSE
                CASE WHEN a = b OR b = c OR c = a
                    THEN 'Isosceles'
                ELSE 'Scalene'
                END
            END
    ELSE 'Not A Triangle'
    END
    FROM triangles;

    SELECT 
    MAX(CASE WHEN Occupation = 'Doctor' THEN Name END) AS Doctor,
    MAX(CASE WHEN Occupation = 'Professor' THEN Name END) AS Professor,
    MAX(CASE WHEN Occupation = 'Singer' THEN Name END) AS Singer,
    MAX(CASE WHEN Occupation = 'Actor' THEN Name END) AS Actor
FROM (
    SELECT Name, Occupation,
           ROW_NUMBER() OVER (PARTITION BY Occupation ORDER BY Name) AS RowNum
    FROM OCCUPATIONS
) AS NumberedOccupations
GROUP BY RowNum
ORDER BY RowNum;



select 
max(case when OCCUPATION ='Doctor' then name end )as Doctor,
max(case when OCCUPATION ='Professor' then name end )as Professor ,
max(case when OCCUPATION ='Singer' then name end )as Singer  ,
max(case when OCCUPATION ='Actor' then name end )as Actor   
from OCCUPATIONS 
group OCCUPATION


select (case when g.grade >=8 then s.name else null end), g.grade, s.marks from students s
join grades g on s.marks between g.min_mark and g.max_mark order by g.grade desc, s.name asc, s.marks asc;

select ID, case when  grade > 8 then Name else 'NULL' end name, Marks,grade ,case when grade > 8 then 1 else 0 end flag from (
select ID, Name, Marks,g.grade from Students s join Grades g on s.marks between g.min_mark and g.max_mark
    )k order by 
        flag DESC, 
    CASE WHEN flag = 1 THEN grade END DESC,
    CASE WHEN flag = 1 THEN Name END,
    CASE WHEN flag = 0 THEN Name END,
    CASE WHEN flag = 0 THEN Marks END ASC;


WITH RECURSIVE Numbers AS ( SELECT 2 AS num UNION ALL SELECT num + 1 FROM Numbers WHERE num < 1000 ), 
Primes AS ( SELECT n.num FROM Numbers n WHERE NOT EXISTS ( SELECT 1 FROM Numbers n2 WHERE n2.num < n.num AND n.num % n2.num = 0 ) ) SELECT GROUP_CONCAT(num SEPARATOR '&') AS PrimeNumber FROM Primes;


select studentid ,count(distinct courseid) cnt
 where cnt=(
select
  count(distinct courseid) course_cnt
 from sudent_batch where extract(year from enrollmentdate)=2019)
 where extract(year from enrollmentdate)=2019
 group by studentid


 -- i. List out the department wise maximum salary, 
-- minimum salary, average salary of the employees.

-- join d and e on departid
-- departmentname, ef. salary
-- GROUP BY departmentname, MIN, MAX, AVG

SELECT 
    d.department,
    MIN(ef.salary) as min_salary,
    MAX(ef.salary) as max_salry,
    AVG(ef.salary) as avg_salary
FROM department as d
JOIN 
emp_fact ef
ON ef.department_id = d.department_id
GROUP BY d.department




-- ii. List out employee having the third highest salary.

SELECT *
FROM 
 (   SELECT 
        *,
        RANK() OVER(ORDER BY salary DESC) as ranks,
        DENSE_RANK() OVER(ORDER BY salary DESC) as dr
    FROM emp_fact
    ) as subqeury
WHERE dr = 3


WITH cte_salary_table
AS
 (   SELECT 
        *,
        RANK() OVER(ORDER BY salary DESC) as ranks,
        DENSE_RANK() OVER(ORDER BY salary DESC) as dr
    FROM emp_fact
    ) 
SELECT * FROM 
cte_salary_table
WHERE dr = 3

-- List out the department having at least four employees.


SELECT 
    d.department
    -- COUNT(ef.employee_id)
FROM department as d
JOIN 
emp_fact ef
ON ef.department_id = d.department_id
GROUP BY d.department
HAVING COUNT(ef.employee_id) >= 4


-- iv. Find out the employees who earn greater than the average salary for their department.

SELECT 
    e1.employee_id,
    e1.emp_name,
    e1.salary,
    e1.department_id
FROM emp_fact as e1 
WHERE e1.salary > (SELECT
                    AVG(e2.salary)
                FROM emp_fact as e2
                WHERE e2.department_id = e1.department_id);

SELECT
    AVG(salary)
FROM emp_fact
WHERE department_id = 30


# list out employee whose drwing more salary than there managers

with emp_slary as(
select e.employee_id,e.salary from 
employee s
),
manager_sal as (
select e.employe_id,m.manager_id,e.salary from 
employee e join emp-manager m on e=employeeid=m.employeeid
)
select distinct e.employee_id
from  empl_salary e join manager_sal m 
on e.employeeid=m.manager_id
where e.empsal>m.manger_sal

select a.employee_id,m.managerid,a.salary_empsalary,sm salary manager_sal
from employee a
join manager_employee m
on a.employee_id=m.employee_id
join employee sm
on m.manager_id=sm.employee.id
where a.salary>sm.salary

  ```

