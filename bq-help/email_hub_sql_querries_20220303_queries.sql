-- full_load
--------------> load the data to <object_name>_intrim_stg cluster by global_member_token,mailable_event_ts

SELECT
  base_vw.globalmembertoken AS global_member_token,
  base_vw.subscriptioncode AS sub_cd,
  base_vw.eligsubscriptioncode AS eligibility_sub_cd,
  base_vw.startdate AS mailable_event_ts,
  base_vw.eligibilityscenarioname AS eligibility_scenario_name,
  base_vw.eligibilityscenariogroup AS eligibility_scenario_group_name,
  base_vw.scenariorule AS scenario_rule_name,
  CASE
    WHEN base_vw.scenariorule = 'newnopreviousactivity' THEN 'true new'
    WHEN base_vw.scenariorule IN ('newnorecentactivity',
    'existingnorecentactivity') THEN 'preconditioned 24 months'
    WHEN base_vw.scenariorule IN ('newunprotected', 'existingunprotected') THEN 'unprotected'
END
  reporting_scenario_rule_name,
  DATE(base_vw.startdate) AS mailable_event_dt
FROM
  `it_sa_onl_ing_base_ext.vw_subscribersubscriptioneligibility` base_vw
UNOIN ALL
SELECT
  cdc_vw.globalmembertoken AS global_member_token,
  cdc_vw.subscriptioncode AS sub_cd,
  cdc_vw.eligsubscriptioncode AS eligibility_sub_cd,
  cdc_vw.startdate AS mailable_event_ts,
  cdc_vw.eligibilityscenarioname AS eligibility_scenario_name,
  cdc_vw.eligibilityscenariogroup AS eligibility_scenario_group_name,
  cdc_vw.scenariorule AS scenario_rule_name,
  CASE
    WHEN cdc_vw.scenariorule = 'newnopreviousactivity' THEN 'true new'
    WHEN cdc_vw.scenariorule IN ('newnorecentactivity',
    'existingnorecentactivity') THEN 'preconditioned 24 months'
    WHEN cdc_vw.scenariorule IN ('newunprotected', 'existingunprotected') THEN 'unprotected'
END
  reporting_scenario_rule_name,
  DATE(cdc_vw.startdate) AS mailable_event_dt
FROM
  `it_sa_onl_ing_cdc_ext.vw_subscribersubscriptioneligibility_cdc` cdc_vw
;

-- Incremental_RECON --> load data to hubs_drv_intr_stg cluster by global_member_token,mailable_event_ts
--------------------
			WITH
  			cdc_driver AS (
  			SELECT
    			cdc_vw.globalmembertoken AS global_member_token,
    			cdc_vw.subscriptioncode AS sub_cd,
    			cdc_vw.eligsubscriptioncode AS eligibility_sub_cd,
    			cdc_vw.startdate AS mailable_event_ts,
    			cdc_vw.eligibilityscenarioname AS eligibility_scenario_name,
    			cdc_vw.eligibilityscenariogroup AS eligibility_scenario_group_name,
    			cdc_vw.scenariorule AS scenario_rule_name,
    			CASE
      			WHEN cdc_vw.scenariorule = 'newnopreviousactivity' THEN 'true new'
      			WHEN cdc_vw.scenariorule IN ('newnorecentactivity',
      			'existingnorecentactivity') THEN 'preconditioned 24 months'
      			WHEN cdc_vw.scenariorule IN ('newunprotected', 'existingunprotected') THEN 'unprotected'
  			END
    			reporting_scenario_rule_name,
    			DATE(cdc_vw.startdate) AS mailable_event_dt
  			FROM
    			`it_sa_onl_ing_cdc_ext.vw_subscribersubscriptioneligibility_cdc` cdc_vw
  			WHERE
    			src_ing_dt >= "{{ get_macro_value('114') }}")
			SELECT
  			drv.*
			FROM
  			(select *,1 as 'sort_ind' from cdc_driver
				UNION ALL
			select *, 2 as 'sort_ind' from `dev-gold-core.it_onl_elt_com_stg_vmk.hubs_drv_mssng_scrs`
			)
  			drv
			LEFT JOIN
			(
  			SELECT
    			global_member_token,
    			mailable_event_ts
  			FROM
    			`prod_gold_bi_reporting.drivers.gmt_edp_test_driver_profile`
  			WHERE
    			mailable_event_dt IN (
    			SELECT
      			mailable_event_dt
    			FROM
      			cdc_driver
    			GROUP BY
      			mailable_event_dt)) ref
			ON
  			(drv.global_member_token = ref.global_member_token
    			AND drv.mailable_event_ts = ref.mailable_event_ts)
			WHERE
  			(( drv.sort_ind = 1 and ref.global_member_token IS NULL and ref.mailable_event_ts IS NULL ) OR (drv.sort_ind = 2)
  			)


-- INCREMENTAL_HOURLY --> load data to hubs_drv_intr_stg cluster by global_member_token,mailable_event_ts
----------------------
WITH
  cdc_driver AS (
  SELECT
    cdc_vw.globalmembertoken AS global_member_token,
    cdc_vw.subscriptioncode AS sub_cd,
    cdc_vw.eligsubscriptioncode AS eligibility_sub_cd,
    cdc_vw.startdate AS mailable_event_ts,
    cdc_vw.eligibilityscenarioname AS eligibility_scenario_name,
    cdc_vw.eligibilityscenariogroup AS eligibility_scenario_group_name,
    cdc_vw.scenariorule AS scenario_rule_name,
    CASE
      WHEN cdc_vw.scenariorule = 'newnopreviousactivity' THEN 'true new'
      WHEN cdc_vw.scenariorule IN ('newnorecentactivity',
      'existingnorecentactivity') THEN 'preconditioned 24 months'
      WHEN cdc_vw.scenariorule IN ('newunprotected', 'existingunprotected') THEN 'unprotected'
  END
    reporting_scenario_rule_name,
    DATE(cdc_vw.startdate) AS mailable_event_dt
  FROM
    `it_sa_onl_ing_intraday_ext.vw_subscribersubscriptioneligibility_intraday` cdc_vw
    )
SELECT
  drv.*
FROM
(select *, 1 as 'sort_ind' from cdc_driver
	UNION ALL
select *, 2 as 'sort_ind' from `dev-gold-core.it_onl_elt_com_stg_vmk.hubs_drv_mssng_scrs`
) drv
LEFT JOIN (
  SELECT
    global_member_token,
    mailable_event_ts
  FROM
    `prod_gold_bi_reporting.drivers.gmt_edp_test_driver_profile`
  WHERE
    mailable_event_dt IN (
    SELECT
      mailable_event_dt
    FROM
      cdc_driver
    GROUP BY
      mailable_event_dt)) ref
ON
  (drv.global_member_token = ref.global_member_token
    AND drv.mailable_event_ts = ref.mailable_event_ts)
WHERE
  (( drv.sort_ind = 1 and ref.global_member_token IS NULL
  AND ref.mailable_event_ts IS NULL ) or (drv.sort_ind = 2)
  )


-- driver object for computing score
-- emailhubs_pre_final_driver1 are the records with score (scenario 1)
create or replace table `hubs_drv_active_scrs` partition by score_status cluster by global_member_token, mailable_event_ts
  AS
SELECT
  subdrv2.global_member_token,
  subdrv2.sub_cd,
  subdrv2.eligibility_sub_cd,
  subdrv2.mailable_event_ts,
  subdrv2.eligibility_scenario_name,
  subdrv2.eligibility_scenario_group_name,
  subdrv2.scenario_rule_name,
  subdrv2.reporting_scenario_rule_name,
  scrgrpcrt.score_type_nm AS score_type_nm,
  scrgrpcrt.scrgrp_cd AS score_group_cd,
  scrgrpcrt.scrgrp_cd_desc AS score_group_cd_desc,
  subdrv2.mailable_event_dt,
  case when subdrv2.effective_dt is null then 0 -- 'no_score'
   else 1 -- 'score'
   end as score_status
FROM (
  SELECT
    *
  FROM (
    SELECT
      drv.global_member_token,
      drv.sub_cd,
      drv.eligibility_sub_cd,
      drv.mailable_event_ts,
      drv.eligibility_scenario_name,
      drv.eligibility_scenario_group_name,
      drv.scenario_rule_name,
      drv.reporting_scenario_rule_name,
      drv.mailable_event_dt
      ecomm_score.ecomm_score_id,
      ecomm_score.score_type_priority_no,
      ecomm_score.ecomm_score_type_id,
      ecomm_score.score_no,
      ecomm_score.effective_ts,
      ecomm_score.effective_dt,
      -- DATE(ecomm_score.effective_dt) --> needs to be replaced by effective_dt
      case when
      coalesce(DATE(ecomm_score.effective_dt),drv.mailable_event_dt) <= drv.mailable_event_dt then 1 else 0 end as it_flag,
      RANK() OVER(PARTITION BY drv.global_member_token, drv.mailable_event_dt ORDER BY ecomm_score.score_type_priority_no DESC, ecomm_score.effective_ts DESC, ecomm_score.ecomm_score_id DESC) AS rnk
    FROM
      `hubs_drv_intr_stg` drv
    LEFT JOIN
      `it_online_auxiliary.online_event_scores` ecomm_score
    ON
      (drv.global_member_token = ecomm_score.global_member_token)
    ) subdrv1
  WHERE
    rnk = 1 and it_flag = 1
    ) subdrv2
LEFT JOIN
  (select
    score_type_nm,
    scrgrp_cd,
    scrgrp_cd_desc,
    ecomm_score_type_id,
    scrgrpcrtstartdate,
    COALESCE(scrgrpcrtenddate,DATETIME(CURRENT_TIMESTAMP,
      'America/New_York')) as scrgrpcrtenddate,
    scrgrpcrtscoremin,
    scrgrpcrtscoremax
    from `it_online_auxiliary.vw_ptp_scrgrpmapping` ) scrgrpcrt
ON
  (subdrv2.ecomm_score_type_id = scrgrpcrt.ecomm_score_type_id)
WHERE
  subdrv2.startdate >= scrgrpcrt.scrgrpcrtstartdate
  AND subdrv2.startdate < scrgrpcrt.scrgrpcrtenddate
  AND subdrv2.score_no >= scrgrpcrt.scrgrpcrtscoremin
  AND subdrv2.score_no <= scrgrpcrt.scrgrpcrtscoremax
),

-- hubs_drv_inactive_scrs are  the records where actual score is yet to be captured (scenario 2 & 3)
----
create or replace table `hubs_drv_inactive_scrs` partition by score_status cluster by global_member_token, mailable_event_ts
  as
SELECT
  subdrv2.global_member_token,
  subdrv2.sub_cd,
  subdrv2.eligibility_sub_cd,
  subdrv2.mailable_event_ts,
  subdrv2.eligibility_scenario_name,
  subdrv2.eligibility_scenario_group_name,
  subdrv2.scenario_rule_name,
  subdrv2.reporting_scenario_rule_name,
  NULL AS score_type_nm,
  NULL AS score_group_cd,
  NULL AS score_group_cd_desc,
  subdrv2.mailable_event_dt,
  case
  when subdrv2.effective_dt is null then 0 -- 'no_score'
  else 1 -- 'score' 
  end as score_status
FROM (
  SELECT
    *
  FROM (
    SELECT
      drv.global_member_token,
      drv.sub_cd,
      drv.eligibility_sub_cd,
      drv.mailable_event_ts,
      drv.eligibility_scenario_name,
      drv.eligibility_scenario_group_name,
      drv.scenario_rule_name,
      drv.reporting_scenario_rule_name,
      drv.mailable_event_dt
      ecomm_score.ecomm_score_id,
      ecomm_score.score_type_priority_no,
      ecomm_score.ecomm_score_type_id,
      ecomm_score.score_no,
      ecomm_score.effective_ts,
      ecomm_score.effective_dt,
      coalesce(DATE(ecomm_score.effective_dt),drv.mailable_event_dt) as derived_effective_or_mailable_dt,
      RANK() OVER(PARTITION BY drv.global_member_token, drv.mailable_event_dt ORDER BY ecomm_score.score_type_priority_no DESC, ecomm_score.effective_ts DESC, ecomm_score.ecomm_score_id DESC) AS rnk
    FROM
      (select intr_stg.* from 
      `hubs_drv_intr_stg` intr_stg 
      left join 
      `hubs_drv_active_scrs` drv_act_scrs 
      on intr_stg.global_member_token = drv_act_scrs.global_member_token 
      and intr_stg.mailable_event_ts and drv_act_scrs.mailable_event_ts 
      where drv_act_scrs.global_member_token is null and drv_act_scrs.mailable_event_ts is null ) drv -- b3 left join D3 where D3 key is null
    LEFT JOIN
      `it_online_auxiliary.online_event_scores` ecomm_score
    ON
      (drv.global_member_token = ecomm_score.global_member_token)
    ) subdrv1
  WHERE
    rnk = 1 and (derived_effective_or_mailable_dt <= CASE WHEN derived_effective_or_mailable_dt > mailable_event_dt THEN derived_effective_or_mailable_dt ELSE mailable_event_dt END)
    ) subdrv2


-- Here are union'ing the emailhubs_pre_final_driver1(records with score) and emailhubs_pre_final_driver2(Records with scores yet to be captured)
-- and load the data to below object.

CREATE OR REPLACE TABLE `dev-gold-core.it_onl_elt_com_stg_vmk.hubs_drv_intr_final` partition by score_status cluster by global_member_token,mailable_event_ts
select
t1.global_member_token as global_member_token,
t1.sub_cd as sub_cd,
t1.eligibility_sub_cd as eligibility_sub_cd,
t1.mailable_event_ts as mailable_event_ts,
t1.eligibility_scenario_name as eligibility_scenario_name,
t1.eligibility_scenario_group_name as eligibility_scenario_group_name,
t1.scenario_rule_name as scenario_rule_name,
t1.reporting_scenario_rule_name as reporting_scenario_rule_name,
t1.score_type_nm as score_type_nm,
t1.score_group_cd as score_group_cd,
t1.score_group_cd_desc as score_group_cd_desc,
t1.mailable_event_dt as mailable_event_dt,
t1.score_status as score_status
from `hubs_drv_active_scrs` t1
UNION ALL
SELECT
  drv.global_member_token,
  drv.sub_cd,
  drv.eligibility_sub_cd,
  drv.mailable_event_ts,
  drv.eligibility_scenario_name,
  drv.eligibility_scenario_group_name,
  drv.scenario_rule_name,
  drv.reporting_scenario_rule_name,
  drv.score_type_nm,
  drv.score_group_cd,
  drv.score_group_cd_desc,
  drv.mailable_event_dt,
  drv.score_status
from `hubs_drv_inactive_scrs` drv


-- Records with no-score are stored in `missing_scores_recon_intrim_obj` cluster by gmt and mailable_event_ts
-- record is inserted and deleted based on the effective_dt column.

MERGE `dev-gold-core.it_onl_elt_com_stg_vmk.hubs_drv_mssng_scrs` intrim
-- considering records where there is no score in the stg_driver_obj (Cell # I8 in excel)
USING (select * from `dev-gold-core.it_onl_elt_com_stg_vmk.hubs_drv_intr_final` ) driver2
-- Joining bbased on Key columns
ON (intrim.global_member_token = driver2.global_member_token and intrim.mailable_event_ts = driver2.mailable_event_ts)
-- If there is match and effective_dt is not null then delete, meaning in prev execution there was no score but in the current execution the score is captured for (gmt,mailable_event_dt) combination hence we delete the records from `missing_scores_recon_intrim_obj` object.
WHEN matched and driver2.effective_dt is not null
THEN DELETE
-- If there no matching (gmt,mailable_event_dt) meaning, we have record for which the score is yet to be captured (scenario 2 & 3 ) and those records will be inserted in `missing_scores_recon_intrim_obj` object and the same will be used in incrementa_Cdc and incremental_hourly reconcilation.
WHEN NOT MATCHED and driver2.effective_dt is null
THEN INSERT values
(driver2.global_member_token,driver2.sub_cd,driver2.eligibility_sub_cd,driver2.mailable_event_ts,driver2.eligibility_scenario_name,driver2.eligibility_scenario_group_name,driver2.scenario_rule_name,driver2.reporting_scenario_rule_name,driver2.score_type_nm,driver2.score_group_cd,driver2.score_group_cd_desc,driver2.mailable_event_dt,driver2.score_status)


-- In case of incremental (hourly and cdc) load we are merging the data to final object so that latest score values will be populated.

MERGE `prod_gold_bi_reporting.drivers.gmt_edp_test_driver_profile` final
USING (select * from `dev-gold-core.it_onl_elt_com_stg_vmk.hubs_drv_intr_final` ) driver2
ON (final.global_member_token = driver2.global_member_token and final.mailable_event_ts = driver2.mailable_event_ts)
WHEN MATCHED THEN
UPDATE SET
final.global_member_token = driver2.global_member_token,final.sub_cd = driver2.sub_cd,final.eligibility_sub_cd = driver2.eligibility_sub_cd,final.mailable_event_ts = driver2.mailable_event_ts,final.eligibility_scenario_name = driver2.eligibility_scenario_name,final.eligibility_scenario_group_name = driver2.eligibility_scenario_group_name,final.scenario_rule_name = driver2.scenario_rule_name,final.reporting_scenario_rule_name = driver2.reporting_scenario_rule_name,final.score_type_nm = driver2.score_type_nm,final.score_group_cd = driver2.score_group_cd,final.score_group_cd_desc = driver2.score_group_cd_desc,final.mailable_event_dt = driver2.mailable_event_dt
WHEN NOT MATCHED THEN
INSERT VALUES
(driver2.global_member_token,driver2.sub_cd,driver2.eligibility_sub_cd,driver2.mailable_event_ts,driver2.eligibility_scenario_name,driver2.eligibility_scenario_group_name,driver2.scenario_rule_name,driver2.reporting_scenario_rule_name,driver2.score_type_nm,driver2.score_group_cd,driver2.score_group_cd_desc,driver2.mailable_event_dt,driver2.score_status)