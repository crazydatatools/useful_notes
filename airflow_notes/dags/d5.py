from airflow import DAG, Dataset
from airflow.operators import empty
from airflow.providers.google.cloud.operators import bigquery
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "cda",
    "retries": 0,
    "depends_on_past": False,
    "start_date": "2021-03-01",
    "email": "premieraudiencesupport@pch.com",
    "email_on_failure": True,
    "email_on_retry": True,
}


with DAG(
    dag_id="user_persona.agg_fact_user_intrests",
    default_args=default_args,
    max_active_runs=1,
    tags=["fact", "retool", "personas", "agg_fact_user_intrests"],
    schedule="@once",
    is_paused_upon_creation=True,
    catchup=False,
    default_view="graph",
) as dag:
    with TaskGroup(group_id="pre_process") as pre_process:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # _tmp_ds_create
        _tmp_ds_create = bigquery.BigQueryCreateEmptyDatasetOperator(
            task_id="_tmp_ds_create",
            dataset_id="tmp_ds_user_persona_agg_fact_user_intrests",
            gcp_conn_id="cda_goldusersummary",
        )
        # check_dependencies_it_user_persona_repo_personas_repo
        check_dependencies_it_user_persona_repo_personas_repo = bigquery.BigQueryCheckOperator(
            task_id="check_dependencies_it_user_persona_repo_personas_repo",
            sql="select count(1) from `it_user_persona_repo.personas_repo_current`\n where last_access_date = \u0027{{ macros.ds_add(ds,-1) }}\u0027\n",
            use_legacy_sql=False,
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        (
            start
            >> check_dependencies_it_user_persona_repo_personas_repo
            >> Label("sys_chk_passed")
            >> _tmp_ds_create
            >> end
        )
    with TaskGroup(group_id="process") as process:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # tmp_get_01_agg_fact_user_intrests
        tmp_get_01_agg_fact_user_intrests = bigquery.BigQueryInsertJobOperator(
            task_id="tmp_get_01_agg_fact_user_intrests",
            configuration={
                "query": {
                    "query": "create or replace table tmp_ds_user_persona_agg_fact_user_intrests.tmp_get_01_agg_fact_user_intrests as \n with tmp_get_01_personas_repo_hema_id as\n                         (select \n                             uid, \n                             cust_id, \n                             _hema_id as hema_id, \n                             age, \n                             gender, \n                             state,  \n                             dma_cd, \n                             dma_cd_desc, \n                             last_access_date, \n                             personas, \n                             tpd_personas,\n                             segment,\n                             emailable.user_clicks.indicators as emailable_click_ids,\n                         from it_user_persona_repo.personas_repo_current as _record_source  \n                         left join unnest(hema_ids) as _hema_id)\n             ,tmp_get_02_personas_repo_hema_id as\n                       (select \n                         uid, \n                         cust_id, \n                         hema_id,\n                         (\n                         select \n                           profile_repo \n                         from it_user_persona_repo_snapshot.hema_ids_{{ ds_nodash }}  _dim_online_only_users\n                         where _record_source.hema_id = _dim_online_only_users.hema_id\n                         ) as gmt_profiles,\n                         age, \n                         gender, \n                         state, \n                         dma_cd, \n                         dma_cd_desc, \n                         last_access_date, \n                         personas,\n                         tpd_personas,\n                         segment,\n                         emailable_click_ids\n                       from tmp_get_01_personas_repo_hema_id as _record_source)\n             ,tmp_get_03_personas_repo_hema_id_gmt as\n                         (select \n                             uid, \n                             cust_id, \n                             hema_id,\n                             _gmt_profile.global_member_token as global_member_token,\n                             _gmt_profile.is_email_purged_ind as is_email_purged_ind,\n                             _gmt_profile.last_access_date as online_last_access_date,\n                             _record_source.age  as age,\n                             _record_source.gender  as gender,\n                             _record_source.state  as state,\n                             _record_source.dma_cd  as dma_cd,\n                             _record_source.dma_cd_desc  as dma_cd_desc,\n                             _record_source.last_access_date  as overall_last_access_date,\n                             _record_source.personas as personas,\n                             _record_source.tpd_personas as tpd_personas ,\n                             _record_source.segment,\n                             _record_source.emailable_click_ids\n                         from tmp_get_02_personas_repo_hema_id as _record_source \n                         left join unnest(gmt_profiles) as _gmt_profile)\n             ,tmp_get_04_personas_repo_hema_id_gmt as \n                       (select \n                         uid,\n                         cust_id, \n                         hema_id, \n                         global_member_token,\n                         current_date() as etl_load_date,\n                         sha512(to_json_string(\n                                   struct(\n                                       uid,\n                                       cust_id, \n                                       hema_id, \n                                       global_member_token\n                                   )\n                               )\n                         ) as etl_user_ids_hash,\n                         sha512(to_json_string(\n                                   struct(\n                                     age, \n                                     gender, \n                                     state, \n                                     dma_cd, \n                                     dma_cd_desc\n                                   )\n                               )\n                         ) as etl_user_personas_profiles_hash,\n                         sha512(to_json_string(\n                                   struct(\n                                     overall_last_access_date\n                                   )\n                               )\n                         ) as etl_user_personas_last_access_dates_hash,\n                         sha512(to_json_string(\n                                   struct(\n                                     personas\n                                   )\n                               )\n                         ) as etl_user_personas_hash,\n                         sha512(to_json_string(\n                                   struct(\n                                     tpd_personas\n                                   )\n                               )\n                         ) as etl_user_tpd_personas_hash,\n                         is_email_purged_ind,\n                         overall_last_access_date, \n                         online_last_access_date,\n                         age, \n                         gender, \n                         state, \n                         dma_cd, \n                         dma_cd_desc, \n                         personas as personas, \n                         tpd_personas as tpd_personas,\n                         segment,\n                         emailable_click_ids\n                       from tmp_get_03_personas_repo_hema_id_gmt)\t \n             ,tmp_get_05_personas_repo_dim_ids as\n                         (select \n                           etl_load_date,\n                           unix_date(overall_last_access_date) etl_load_partitoin_id,\n                           extract(year FROM overall_last_access_date) AS etl_load_partition_year,\n                           etl_user_ids_hash,\n                           abs(farm_fingerprint(to_json_string(etl_user_ids_hash))) _fact_user_id,\n                           uid,\n                           abs(farm_fingerprint(to_json_string(uid))) _fact_uid, \n                           cust_id,\n                           abs(farm_fingerprint(to_json_string(cust_id))) _fact_cust_id,\n                           hema_id,\n                           abs(farm_fingerprint(to_json_string(hema_id))) _fact_hema_id,\n                           global_member_token,\n                           abs(farm_fingerprint(to_json_string(global_member_token))) _fact_global_member_token,\n                           is_email_purged_ind,\n                           etl_user_personas_profiles_hash,\n                           age,\n                           abs(farm_fingerprint(to_json_string(age))) _dim_age_id,\n                           gender,\n                           abs(farm_fingerprint(to_json_string(gender))) _dim_gender_id, \n                           state,\n                           abs(farm_fingerprint(to_json_string(state))) _dim_state_id, \n                           dma_cd, \n                           dma_cd_desc,\n                           abs(farm_fingerprint(to_json_string(struct(dma_cd,dma_cd_desc)))) _dim_dma_cd_id,\n                           etl_user_personas_last_access_dates_hash, \n                           overall_last_access_date,online_last_access_date,\n                           personas,\n                           array(  select \n                                     abs(farm_fingerprint(sha512(to_json_string(struct(_p as _fpd_raw_field_name))))) as _dim_fpd_persona_id\n                                   from \n                                     unnest(personas),\n                                     unnest(persona) _p\n                                   group by \n                                     _p\n                                 ) as _dim_persona_ids,\n                           abs(farm_fingerprint(to_json_string(personas))) _dim_overall_persona_id,\n                           tpd_personas,\n                           abs(farm_fingerprint(to_json_string(tpd_personas))) _dim_overall_tpd_persona_id, \n                           array(  select \n                                     abs(farm_fingerprint(sha512(to_json_string(struct(tpd_raw_field_name as _tpd_raw_field_name,tier))))) as _dim_tpd_persona_id\n                                   from \n                                   unnest(tpd_personas.personas),\n                                   unnest(tiers) tier\n                               ) as _dim_tpd_persona_ids,\n                           segment,\n                           array(  select  abs(farm_fingerprint(to_json_string(_s)))  as _dim_segment_id\n                                   from \n                                     unnest(segment) _s\n                                   group by \n                                     _s\n                                 ) as _dim_segment_ids,\n                           emailable_click_ids,\n                           array(  select  abs(farm_fingerprint(to_json_string(_s)))  as _dim_emailable_click_ids\n                                   from \n                                     unnest(emailable_click_ids) _s\n                                   group by \n                                     _s\n                                 ) as _dim_emailable_click_ids,\n\n                         from tmp_get_04_personas_repo_hema_id_gmt)\n           ,tmp_get_06_personas_repo_stg_agg_segments as\n                             (select \n                             _fact_user_id,\n                               _info.segment_id as segment_id,\n                             from \n                               tmp_get_05_personas_repo_dim_ids _source \n                             join \n                               unnest(_source._dim_segment_ids)  as _source_dim_segment\n                             join it_user_persona_snapshot.dim_segment_{{ ds_nodash }} _info on   _info._dim_segment_id = _source_dim_segment\n                             group by \n                               1,\n                               2)\n           ,tpd_rolled_up as \n                         (\n                           select _dim_tpd_persona_id,persona_id from it_user_persona_snapshot.dim_tpd_persona_{{ ds_nodash }} _info  \n                           union distinct\n                           select c._dim_tpd_persona_id,p.persona_id  from it_user_persona_snapshot.dim_tpd_persona_{{ ds_nodash }} c\n                           join it_user_persona_snapshot.dim_tpd_persona_{{ ds_nodash }} p on c.parent=p.persona_name and c.tier=p.tier and p.approved_entry=2  and c.approved_entry=1\n                         )\n             \n           ,tmp_get_06_personas_repo_stg_agg_tpd_personas as\n                         (select \n                         _fact_user_id,_info.persona_id as tpd_persona_id \n                         from \n                         tmp_get_05_personas_repo_dim_ids _source \n                         join \n                         unnest(_source._dim_tpd_persona_ids)  as _source_dim_tpd_persona_ids\n                         join  tpd_rolled_up _info on _info._dim_tpd_persona_id =_source_dim_tpd_persona_ids\n                         group by \n                         1,\n                         2)\t\t\t\t\t\n           ,tmp_get_06_personas_repo_stg_agg_dim as\n                           ( select \n                           _fact_user_id,\n                           _fact_cust_id,\n                           _fact_hema_id,\n                           _fact_global_member_token,\n                           is_email_purged_ind,\n                           (select age_id from it_user_persona_snapshot.dim_age_{{ ds_nodash }} as _info where _source._dim_age_id = _info._dim_age_id ) as age_id,\n                           (select gender_id from it_user_persona_snapshot.dim_gender_{{ ds_nodash }} as _info where _source._dim_gender_id = _info._dim_gender_id ) as gender_id,\n                           (select state_id from it_user_persona_snapshot.dim_state_{{ ds_nodash }} as _info where _source._dim_state_id = _info._dim_state_id ) as state_id,\n                           (select dma_cd_id from it_user_persona_snapshot.dim_dma_cd_{{ ds_nodash }} as _info where _source._dim_dma_cd_id = _info._dim_dma_cd_id ) as dma_cd_id,\n                           _dim_persona_ids,\n                           _dim_tpd_persona_ids,\n                           online_last_access_date,\n                           overall_last_access_date\n                           from tmp_get_05_personas_repo_dim_ids  _source )\n                     ,fpd_rolled_up as \n               (\n                             select _dim_fpd_persona_id,persona_id from it_user_persona_snapshot.dim_fpd_persona_{{ ds_nodash }} _info  \n                             union distinct\n                             select c._dim_fpd_persona_id,p.persona_id  from it_user_persona_snapshot.dim_fpd_persona_{{ ds_nodash }} c\n                             join it_user_persona_snapshot.dim_fpd_persona_{{ ds_nodash }} p on c.parent=p.persona_name and p.approved_entry=2 and c.approved_entry=1\n                             )\n           ,tmp_get_06_personas_repo_stg_agg_fpd_personas as\n                             (select \n                             _fact_user_id,\n                               _info.persona_id as persona_id,\n                             from \n                               tmp_get_05_personas_repo_dim_ids _source \n                             join \n                               unnest(_source._dim_persona_ids)  as _source_dim_persona_ids\n                             join fpd_rolled_up _info on   _info._dim_fpd_persona_id = _source_dim_persona_ids\n                             group by \n                               1,\n                               2)\n         ,tmp_get_06_personas_repo_stg_agg_merge AS\n                         ( SELECT\n                             overall_last_access_date,\n                             online_last_access_date,\n                             age_id,\n                             gender_id,\n                             state_id,\n                             dma_cd_id,\n                             _fact_cust_id,\n                             case when _fact_cust_id=1659971858173592857 then if(_fact_global_member_token=1659971858173592857,null,_fact_global_member_token) else _fact_cust_id end explode_in_gmt,\n                             ARRAY_AGG(DISTINCT\n                             IF\n                             (_fact_hema_id=1659971858173592857,NULL,_fact_hema_id) IGNORE NULLS) _fact_hema_ids,\n                             ARRAY_AGG(DISTINCT\n                             IF\n                             (is_email_purged_ind=0\n                               AND _fact_global_member_token!=1659971858173592857,_fact_global_member_token,NULL) IGNORE NULLS) _fact_unpurged_gmts,\n                             ARRAY_AGG(DISTINCT CAST(persona_id AS string)  IGNORE NULLS) _activity_persona_ids,\n                             ARRAY_AGG(DISTINCT CAST(tpd_persona_id AS string) IGNORE NULLS) _activity_tpd_persona_ids\n                           FROM\n                             tmp_get_06_personas_repo_stg_agg_dim src\n                           LEFT JOIN\n                             tmp_get_06_personas_repo_stg_agg_fpd_personas fpd\n                           ON\n                             src._fact_user_id=fpd._fact_user_id\n                           LEFT JOIN\n                             tmp_get_06_personas_repo_stg_agg_tpd_personas tpd\n                           ON\n                             src._fact_user_id=tpd._fact_user_id\n                           GROUP BY\n                             overall_last_access_date,\n                             online_last_access_date,\n                             age_id,\n                             gender_id,\n                             state_id,\n                             dma_cd_id,\n                             _fact_cust_id,\n                             explode_in_gmt )\n         , tmp_get_06_personas_repo_stg_agg_emailable_ind as \n                           ( select \n                             _fact_user_id,\n                               _info.last_access_date_id as last_emailable_ind,\n                             from tmp_get_05_personas_repo_dim_ids _source \n                             join \n                               unnest(_source._dim_emailable_click_ids)  as _source_dim_emailable_click_id\n                             join it_user_persona_snapshot.dim_last_access_date_{{ ds_nodash }} _info on   _info._dim_last_access_date_id = _source_dim_emailable_click_id\n                             group by \n                               1,\n                               2)\t\t\t\t\t\t\t   \n                     ,emailable_extract AS \n               (\n                           SELECT\n                             overall_last_access_date,\n                             online_last_access_date,\n                             age_id,\n                             gender_id,\n                             state_id,\n                             dma_cd_id,\n                             _fact_cust_id,\n                             case when _fact_cust_id=1659971858173592857 then if(_fact_global_member_token=1659971858173592857,null,_fact_global_member_token) else _fact_cust_id end explode_in_gmt,\n                             ARRAY_AGG(DISTINCT\n                             IF\n                             (_fact_hema_id=1659971858173592857,NULL,_fact_hema_id) IGNORE NULLS) _fact_hema_ids,\n                             ARRAY_AGG(DISTINCT\n                             IF\n                             (is_email_purged_ind=0\n                               AND _fact_global_member_token!=1659971858173592857,_fact_global_member_token,NULL) IGNORE NULLS) _fact_unpurged_gmts,\n                             ARRAY_AGG(DISTINCT CAST(last_emailable_ind AS string) IGNORE NULLS) _last_emailable_inds\n                           FROM\n                             tmp_get_06_personas_repo_stg_agg_dim src\n                           JOIN\n                             tmp_get_06_personas_repo_stg_agg_emailable_ind seg\n                           ON\n                             src._fact_user_id=seg._fact_user_id\n                           GROUP BY\n                             overall_last_access_date,\n                             online_last_access_date,\n                             age_id,\n                             gender_id,\n                             state_id,\n                             dma_cd_id,\n                             _fact_cust_id,\n                             explode_in_gmt\n                             )\n         ,tmp_get_06_personas_repo_stg_agg_emailable_merge AS\n                             (SELECT\n                               SHA512(TO_JSON_STRING(STRUCT( \n                                     _fact_cust_id,\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_hema_ids) as x order by 1) ))),\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_unpurged_gmts) as x order by 1)))),\n                                     overall_last_access_date,\n                                     online_last_access_date,\n                                     age_id,\n                                     gender_id,\n                                     state_id,\n                                     dma_cd_id ))) uniq_id,_last_emailable_inds\n                               from emailable_extract\n                             )\n                             ,persona_extract AS (\n                             SELECT\n                               SHA512(TO_JSON_STRING(STRUCT( \n                                     _fact_cust_id,\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_hema_ids) as x order by 1) ))),\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_unpurged_gmts) as x order by 1)))),\n                                     overall_last_access_date,\n                                     online_last_access_date,\n                                     age_id,\n                                     gender_id,\n                                     state_id,\n                                     dma_cd_id ))) uniq_id,\n                               ARRAY(\n                               SELECT\n                                 _info.persona_id\n                               FROM\n                                 it_user_persona_snapshot.dim_persona_{{ ds_nodash }} _info\n                               JOIN\n                                 UNNEST(_activity_tpd_persona_ids) AS _tpd_persona_id\n                               ON\n                                 CAST(_info.tpd_persona_id AS string) = _tpd_persona_id ) AS _activity_unified_tpd_persona_ids,\n                               ARRAY(\n                               SELECT\n                                 _info.persona_id\n                               FROM\n                                 it_user_persona_snapshot.dim_persona_{{ ds_nodash }} _info\n                               JOIN\n                                 UNNEST(_activity_persona_ids) AS _fpd_persona_id\n                               ON\n                                 CAST(_info.fpd_persona_id AS string) = _fpd_persona_id ) AS _activity_unified_fpd_persona_ids\n                             FROM\n                               tmp_get_06_personas_repo_stg_agg_merge agg )\n       ,tmp_get_06_personas_repo_stg_agg_unified_personas AS \n                           (SELECT\n                             uniq_id,\n                             ARRAY(\n                             SELECT\n                               DISTINCT CAST(_pid AS string)\n                             FROM\n                               UNNEST(ARRAY_CONCAT(_activity_unified_tpd_persona_ids,_activity_unified_fpd_persona_ids)) AS _pid) AS _activity_unified_persona_ids\n                           FROM\n                             persona_extract)\n       ,segment_extract AS (\n                           SELECT\n                             overall_last_access_date,\n                             online_last_access_date,\n                             age_id,\n                             gender_id,\n                             state_id,\n                             dma_cd_id,\n                             _fact_cust_id,\n                             case when _fact_cust_id=1659971858173592857 then if(_fact_global_member_token=1659971858173592857,null,_fact_global_member_token) else _fact_cust_id end explode_in_gmt,\n                             ARRAY_AGG(DISTINCT\n                             IF\n                             (_fact_hema_id=1659971858173592857,NULL,_fact_hema_id) IGNORE NULLS) _fact_hema_ids,\n                             ARRAY_AGG(DISTINCT\n                             IF\n                             (is_email_purged_ind=0\n                               AND _fact_global_member_token!=1659971858173592857,_fact_global_member_token,NULL) IGNORE NULLS) _fact_unpurged_gmts,\n                             ARRAY_AGG(DISTINCT CAST(segment_id AS string) IGNORE NULLS) _activity_segment_ids\n                           FROM\n                             tmp_get_06_personas_repo_stg_agg_dim src\n                           LEFT JOIN\n                             tmp_get_06_personas_repo_stg_agg_segments seg\n                           ON\n                             src._fact_user_id=seg._fact_user_id\n                           GROUP BY\n                             overall_last_access_date,\n                             online_last_access_date,\n                             age_id,\n                             gender_id,\n                             state_id,\n                             dma_cd_id,\n                             _fact_cust_id,\n                             explode_in_gmt\n                             )\n       ,tmp_get_06_personas_repo_stg_agg_segment_merge AS\n                           ( SELECT\n                               SHA512(TO_JSON_STRING(STRUCT( \n                                     _fact_cust_id,\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_hema_ids) as x order by 1) ))),\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_unpurged_gmts) as x order by 1)))),\n                                     overall_last_access_date,\n                                     online_last_access_date,\n                                     age_id,\n                                     gender_id,\n                                     state_id,\n                                     dma_cd_id ))) uniq_id,_activity_segment_ids\n                               from segment_extract\n                               --where _activity_segment_ids is not null\n                               )\n       , tmp_get_06_personas_repo_stg_agg AS\n                           ( SELECT\n                             agg.*,\n                             u_persona._activity_unified_persona_ids,\n                             seg._activity_segment_ids,\n                             email._last_emailable_inds\n                           FROM\n                             tmp_get_06_personas_repo_stg_agg_merge agg\n                           LEFT JOIN\n                             tmp_get_06_personas_repo_stg_agg_unified_personas u_persona\n                           ON\n                             SHA512(TO_JSON_STRING(STRUCT( \n                                     _fact_cust_id,\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_hema_ids) as x order by 1) ))),\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_unpurged_gmts) as x order by 1)))),\n                                     overall_last_access_date,\n                                     online_last_access_date,\n                                     age_id,\n                                     gender_id,\n                                     state_id,\n                                     dma_cd_id ))) =u_persona.uniq_id\n                           LEFT JOIN\n                             tmp_get_06_personas_repo_stg_agg_segment_merge seg\n                           ON\n                             SHA512(TO_JSON_STRING(STRUCT( \n                                     _fact_cust_id,\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_hema_ids) as x order by 1) ))),\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_unpurged_gmts) as x order by 1)))),\n                                     overall_last_access_date,\n                                     online_last_access_date,\n                                     age_id,\n                                     gender_id,\n                                     state_id,\n                                     dma_cd_id ))) =seg.uniq_id \n                           LEFT JOIN\n                             tmp_get_06_personas_repo_stg_agg_emailable_merge email\n                           ON\n                             SHA512(TO_JSON_STRING(STRUCT( \n                                     _fact_cust_id,\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_hema_ids) as x order by 1) ))),\n                                     abs(farm_fingerprint(to_json_string(array(select x from unnest(_fact_unpurged_gmts) as x order by 1)))),\n                                     overall_last_access_date,\n                                     online_last_access_date,\n                                     age_id,\n                                     gender_id,\n                                     state_id,\n                                     dma_cd_id ))) =email.uniq_id\n                   )\n                   select * from tmp_get_06_personas_repo_stg_agg;                          \n",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "tmp_ds_user_persona_agg_fact_user_intrests",
                    "biz_execution": "_daily",
                    "dev_search_terms": "cust_profile_data",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        start >> tmp_get_01_agg_fact_user_intrests >> end
    with TaskGroup(group_id="update_process") as update_process:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # agg_fact_user_intrests_delete
        agg_fact_user_intrests_delete = bigquery.BigQueryInsertJobOperator(
            task_id="agg_fact_user_intrests_delete",
            configuration={
                "query": {
                    "query": "truncate table `user_persona.agg_fact_user_intrests` ;\n",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "tmp_ds_user_persona_agg_fact_user_intrests",
                    "biz_execution": "_daily",
                    "dev_search_terms": "cust_profile_data",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # agg_fact_user_intrests_insert
        agg_fact_user_intrests_insert = bigquery.BigQueryInsertJobOperator(
            task_id="agg_fact_user_intrests_insert",
            configuration={
                "query": {
                    "query": "insert into `user_persona.agg_fact_user_intrests` \nSELECT \n  _fact_cust_id,\n  abs(farm_fingerprint(to_json_string(_fact_hema_ids))) overall_fact_hema_id,\n  abs(farm_fingerprint(to_json_string(_fact_unpurged_gmts))) overall_fact_unpurged_gmts_id,\n  cast('{{ ds }}' as date) as etl_load_date,\n  overall_last_access_date,\n  age_id, \n  gender_id, \n  state_id, \n  dma_cd_id,\n  _fact_hema_ids, \n  _fact_unpurged_gmts,\n  _activity_persona_ids,\n  _activity_tpd_persona_ids,\n  online_last_access_date,\n  _activity_unified_persona_ids,\n  _activity_segment_ids,\n  _last_emailable_inds\nfrom `tmp_ds_user_persona_agg_fact_user_intrests.tmp_get_01_agg_fact_user_intrests` ;\n",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "tmp_ds_user_persona_agg_fact_user_intrests",
                    "biz_execution": "_daily",
                    "dev_search_terms": "cust_profile_data",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        start >> agg_fact_user_intrests_delete >> agg_fact_user_intrests_insert >> end
    with TaskGroup(group_id="post_process") as post_process:
        # start
        start = empty.EmptyOperator(
            task_id="start",
        )
        # Drop snapshot for already existing snapshot table
        _drop_snapshot = bigquery.BigQueryInsertJobOperator(
            task_id="_drop_snapshot",
            configuration={
                "query": {
                    "query": "DROP SNAPSHOT TABLE IF EXISTS  it_user_persona_snapshot.agg_fact_user_intrests_{{ ds_nodash }}\n",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "it_personalization_repo",
                    "biz_execution": "reference_object",
                    "dev_search_terms": "ids_repo",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # Snapshot for target table
        _create_snapshot = bigquery.BigQueryInsertJobOperator(
            task_id="_create_snapshot",
            configuration={
                "query": {
                    "query": "CREATE SNAPSHOT TABLE it_user_persona_snapshot.agg_fact_user_intrests_{{ ds_nodash }}\n CLONE `user_persona.agg_fact_user_intrests`\n   OPTIONS (\n     expiration_timestamp = TIMESTAMP_ADD( '{{ds}}', INTERVAL 7 DAY)\n     );\n",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "it_personalization_repo",
                    "biz_execution": "reference_object",
                    "dev_search_terms": "ids_repo",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # Hub view from Snapshot table
        _app_view_from_snapshot = bigquery.BigQueryInsertJobOperator(
            task_id="_app_view_from_snapshot",
            configuration={
                "query": {
                    "query": "CREATE OR REPLACE VIEW\n   app_user_persona_hub.agg_fact_user_intrests AS\n SELECT\n   *\n FROM\n   it_user_persona_snapshot.agg_fact_user_intrests_{{ ds_nodash }}\n",
                    "useLegacySql": False,
                },
                "labels": {
                    "biz_resource": "it_personalization_repo",
                    "biz_execution": "reference_object",
                    "dev_search_terms": "ids_repo",
                },
            },
            gcp_conn_id="cda_goldusersummary",
            location="US",
        )
        # _tmp_ds_delete
        _tmp_ds_delete = bigquery.BigQueryDeleteDatasetOperator(
            task_id="_tmp_ds_delete",
            dataset_id="tmp_ds_user_persona_agg_fact_user_intrests",
            gcp_conn_id="cda_goldusersummary",
            delete_contents=True,
        )
        # end
        end = empty.EmptyOperator(
            task_id="end",
        )
        (
            start
            >> _drop_snapshot
            >> _create_snapshot
            >> _app_view_from_snapshot
            >> Label("clnup_tasks")
            >> _tmp_ds_delete
            >> end
        )

pre_process >> process >> update_process >> post_process