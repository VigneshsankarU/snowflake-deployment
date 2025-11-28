-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_UNDRWRT_REQMT_STS_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- PIPELINE START FOR 1
  -- Component SQ_pc_uwissuehistory, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_uwissuehistory AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS uw_aplctn_id,
                $2  AS uw_reqmt_type_cd,
                $3  AS uw_sts_cd,
                $4  AS uw_aplctn_sts_strt_dttm,
                $5  AS uw_issu_key_id,
                $6  AS uw_indiv_prty_id,
                $7  AS trans_strt_dttm,
                $8  AS rnk,
                $9  AS ins_flag,
                $10 AS edw_strt_dttm,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH appctnuwsts AS
                                  (
                                                  SELECT DISTINCT rank() over (PARTITION BY jobnumber,typecode,uwissuetypecode,issuekey ORDER BY updatetime DESC, busnstartdate DESC, id DESC) rnk,
                                                                  updatetime,
                                                                  jobnumber,
                                                                  typecode,
                                                                  uwissuetypecode,
                                                                  uwrefrsnststypecode,
                                                                  addressbookuid,
                                                                  src_sys ,
                                                                  busnstartdate,
                                                                  issuekey,
                                                                  id
                                                                  /*,case when rnk=1 then cast(''9999-12-31 23:59:59.999999'' as timestamp(6)) else max(BusnStartDate) OVER (PARTITION BY JobNumber,TypeCode,UWIssueTypeCode,lower(issuekey)
ORDER BY BusnStartDate DESC ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING) end TransendDate*/
                                                  FROM            (
                                                                                  SELECT DISTINCT updatetime,
                                                                                                  jobnumber,
                                                                                                  typecode,
                                                                                                  uwissuetypecode,
                                                                                                  uwrefrsnststypecode,
                                                                                                  upper(addressbookuid) AS addressbookuid,
                                                                                                  ''SRC_SYS4''            AS src_sys ,
                                                                                                  busnstartdate,
                                                                                                  issuekey,
                                                                                                  id
                                                                                  FROM            (
                                                                                                                  /* --------------------------------------------------------------- */
                                                                                                                  SELECT DISTINCT pc_uwissuehistory.updatetime_stg                  AS updatetime,
                                                                                                                                  pc_job.jobnumber_stg                              AS jobnumber,
                                                                                                                                  pctl_job.typecode_stg                             AS typecode,
                                                                                                                                  pc_uwissuetype.code_stg                           AS uwissuetypecode,
                                                                                                                                  pctl_uwissuehistorystatus.typecode_stg            AS uwrefrsnststypecode,
                                                                                                                                  pc_contact.publicid_stg                           AS addressbookuid,
                                                                                                                                  pc_uwissuehistory.createtime_stg                  AS busnstartdate,
                                                                                                                                  pc_uwissuehistory.issuekey_stg COLLATE ''en-ci''     AS issuekey,
                                                                                                                                  pc_uwissuehistory.id_stg                          AS id
                                                                                                                  FROM            db_t_prod_stag.pc_uwissuehistory
                                                                                                                  join            db_t_prod_stag.pc_policyperiod
                                                                                                                  ON              pc_policyperiod.id_stg = pc_uwissuehistory.policyperiodid_stg
                                                                                                                  left join       db_t_prod_stag.pc_job
                                                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                                                  left join       db_t_prod_stag.pctl_job
                                                                                                                  ON              pc_job.subtype_stg = pctl_job.id_stg
                                                                                                                  left join       db_t_prod_stag.pc_uwissuetype
                                                                                                                  ON              pc_uwissuehistory.issuetypeid_stg = pc_uwissuetype.id_stg
                                                                                                                  left join       db_t_prod_stag.pc_user
                                                                                                                  ON              pc_user.id_stg = pc_uwissuehistory.responsibleuser_stg
                                                                                                                  left join       db_t_prod_stag.pc_contact
                                                                                                                  ON              pc_contact.id_stg = pc_user.contactid_stg
                                                                                                                  left join       db_t_prod_stag.pctl_uwissuehistorystatus
                                                                                                                  ON              pctl_uwissuehistorystatus.id_stg=pc_uwissuehistory.status_stg
                                                                                                                  left join       db_t_prod_stag.pctl_uwissueblockingpoint
                                                                                                                  ON              pc_uwissuetype.blockingpoint_stg = pctl_uwissueblockingpoint.id_stg
                                                                                                                  WHERE           ((
                                                                                                                                                                  pc_uwissuehistory.updatetime_stg > ( :START_DTTM)
                                                                                                                                                  AND             pc_uwissuehistory.updatetime_stg <= ( :END_DTTM))
                                                                                                                                  OR              (
                                                                                                                                                                  pc_policyperiod.updatetime_stg > ( :START_DTTM)
                                                                                                                                                  AND             pc_policyperiod.updatetime_stg <= ( :END_DTTM)))
                                                                                                                  AND
                                                                                                                                  /*eim-47608 history fix*/
                                                                                                                                  pctl_job.typecode_stg IN (''Submission'',
                                                                                                                                                            ''PolicyChange'',
                                                                                                                                                            ''Renewal'')
                                                                                                                  /* --------------------------------------------------------------------------- */
                                                                                                                  UNION
                                                                                                                  /* --------------------------------------------------------------------------- */
                                                                                                                  SELECT DISTINCT pc_uwreferralreason.updatetime_stg                                                              AS updatetime ,
                                                                                                                                  pc_job.jobnumber_stg                                                                            AS jobnumber ,
                                                                                                                                  pctl_job.typecode_stg                                                                           AS typecode ,
                                                                                                                                  pc_uwissuetype.code_stg                                                                         AS uwissuetypecode ,
                                                                                                                                  pctl_uwreferralreasonstatus.typecode_stg                                                        AS uwrefrsnststypecode ,
                                                                                                                                  pc_contact.addressbookuid_stg                                                                   AS addressbookuid ,
                                                                                                                                  coalesce(pc_uwreferralreason.createtime_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))) AS busnstartdate ,
                                                                                                                                  cast('''' AS VARCHAR(100))                                                                        AS issuekey ,
                                                                                                                                  pc_uwreferralreason.id_stg                                                                      AS id
                                                                                                                  FROM            db_t_prod_stag.pc_uwreferralreason
                                                                                                                  left join       db_t_prod_stag.pc_uwissuetype
                                                                                                                  ON              pc_uwreferralreason.issuetypeid_stg=pc_uwissuetype.id_stg
                                                                                                                  left join       db_t_prod_stag.pc_policy
                                                                                                                  ON              pc_policy.id_stg=pc_uwreferralreason.policy_stg
                                                                                                                  left join       db_t_prod_stag.pc_policyperiod
                                                                                                                  ON              pc_policyperiod.policyid_stg=pc_policy.id_stg
                                                                                                                  left join       db_t_prod_stag.pc_job
                                                                                                                  ON              pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                                  left join       db_t_prod_stag.pctl_job
                                                                                                                  ON              pc_job.subtype_stg=pctl_job.id_stg
                                                                                                                  left join       db_t_prod_stag.pc_user
                                                                                                                  ON              pc_user.id_stg=pc_uwreferralreason.createuserid_stg
                                                                                                                  left join       db_t_prod_stag.pc_contact
                                                                                                                  ON              pc_contact.id_stg=pc_user.contactid_stg
                                                                                                                  left join       db_t_prod_stag.pctl_uwreferralreasonstatus
                                                                                                                  ON              pctl_uwreferralreasonstatus.id_stg=pc_uwreferralreason.status_stg
                                                                                                                  left join       db_t_prod_stag.pc_credential
                                                                                                                  ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                                                  WHERE           pc_uwreferralreason.updatetime_stg > ( :START_DTTM)
                                                                                                                  AND             pc_uwreferralreason.updatetime_stg <= ( :END_DTTM)
                                                                                                                  AND
                                                                                                                                  /*history fix*/
                                                                                                                                  pctl_job.typecode_stg IN (''Submission'',
                                                                                                                                                            ''PolicyChange'',
                                                                                                                                                            ''Renewal'') )x qualify row_number() over(PARTITION BY jobnumber,typecode,uwissuetypecode,uwrefrsnststypecode,issuekey, addressbookuid,busnstartdate ORDER BY updatetime DESC,busnstartdate DESC, id DESC) = 1 )y )
                  SELECT DISTINCT aplctn.aplctn_id                                                    AS uw_aplctn_id ,
                                  cast(coalesce(xlat_uwr_type.tgt_idntftn_val,''UNK'') AS     VARCHAR(100)) AS uw_reqmt_type_cd ,
                                  cast(coalesce(xlat_uwr_sts_type.tgt_idntftn_val,''UNK'') AS VARCHAR(100)) AS uw_sts_cd ,
                                  busnstartdate                                                           AS uw_aplctn_sts_strt_dttm ,
                                  auw_req_issu.aplctn_undrwrt_req_issu_key_id                             AS uw_issu_key_id ,
                                  coalesce(indiv.indiv_prty_id,9999)                                      AS uw_indiv_prty_id ,
                                  updatetime ,
                                  rnk ,
                                  CASE
                                                  WHEN (
                                                                                  tgt.aplctn_id IS NULL) THEN ''I''
                                  END                                                                       ins_flag ,
                                 -- cast(current_timestamp - (rnk - 1) * interval ''2 second'' AS timestamp) AS edw_strt_dttm
                                 DATEADD(second, -2 * (rnk - 1), CURRENT_TIMESTAMP()) AS edw_strt_dttm
                  FROM            appctnuwsts
                  left join       db_t_prod_core.teradata_etl_ref_xlat AS xlat_job_typecode
                  ON              appctnuwsts.typecode= xlat_job_typecode.src_idntftn_val
                  AND             xlat_job_typecode.tgt_idntftn_nm= ''APLCTN_TYPE''
                  AND             xlat_job_typecode.src_idntftn_nm= ''pctl_job.Typecode''
                  AND             xlat_job_typecode.src_idntftn_sys= ''GW''
                  AND             xlat_job_typecode.expn_dt=''9999-12-31''
                                  /* ----------------------------------------------------------------------------- */
                  left join       db_t_prod_core.teradata_etl_ref_xlat AS xlat_src_sys
                  ON              xlat_src_sys.src_idntftn_val= src_sys
                  AND             xlat_src_sys.tgt_idntftn_nm= ''SRC_SYS''
                  AND             xlat_src_sys.src_idntftn_nm= ''derived''
                  AND             xlat_src_sys.src_idntftn_sys= ''DS''
                  AND             xlat_src_sys.expn_dt=''9999-12-31''
                                  /* ----------------------------------------------------------------------------- */
                                  /*  Fetch APLCTN_ID from DB_T_PROD_CORE.APLCTN */
                  join
                                  (
                                           SELECT   aplctn_id,
                                                    host_aplctn_id,
                                                    src_sys_cd,
                                                    aplctn_type_cd
                                           FROM     db_t_prod_core.aplctn
                                                    /* WHERE APLCTN.HOST_APLCTN_ID=AppctnUWSts.JobNumber */
                                                    qualify row_number () over (PARTITION BY host_aplctn_id,src_sys_cd ORDER BY edw_end_dttm DESC)=1 ) aplctn
                  ON              aplctn.host_aplctn_id= appctnuwsts.jobnumber
                  AND             aplctn.src_sys_cd=xlat_src_sys.tgt_idntftn_val
                  AND             aplctn.aplctn_type_cd = xlat_job_typecode.tgt_idntftn_val
                                  /* ----------------------------------------------------------------------------- */
                  left join       db_t_prod_core.teradata_etl_ref_xlat AS xlat_uwr_sts_type
                  ON              xlat_uwr_sts_type.src_idntftn_val= appctnuwsts.uwrefrsnststypecode
                  AND             xlat_uwr_sts_type.tgt_idntftn_nm= ''UNDRWRTG_STS_TYPE''
                  AND             xlat_uwr_sts_type.src_idntftn_sys=''GW''
                  AND             xlat_uwr_sts_type.expn_dt=''9999-12-31''
                                  /* ------------------------------------------------------------------------------- */
                  left join
                                  (
                                           SELECT   *
                                           FROM     db_t_prod_core.teradata_etl_ref_xlat qualify row_number() over(PARTITION BY src_idntftn_val,tgt_idntftn_nm ORDER BY expn_dt DESC,eff_dt DESC)=1 ) AS xlat_uwr_type
                  ON              xlat_uwr_type.src_idntftn_val= appctnuwsts.uwissuetypecode
                  AND             xlat_uwr_type.tgt_idntftn_nm= ''UNDRWRTG_REQMT_TYPE''
                  AND             xlat_uwr_type.src_idntftn_sys=''GW''
                  AND             xlat_uwr_type.expn_dt=''9999-12-31''
                                  /* -------------------------------------------------------------------------------- */
                  left join
                                  /* EIM-46961 */
                                  /* (select INDIV_PRTY_ID,NK_LINK_ID, NK_PUBLC_ID FROM  DB_T_PROD_CORE.INDIV  QUALIFY ROW_NUMBER() OVER (PARTITION BY NK_LINK_ID ORDER BY INDIV_PRTY_ID DESC)=1) */
                                  (
                                           SELECT   indiv_prty_id,
                                                    nk_link_id,
                                                    nk_publc_id
                                           FROM     db_t_prod_core.indiv qualify row_number() over (PARTITION BY nk_publc_id ORDER BY indiv_prty_id DESC)=1) AS indiv
                                  /* ON INDIV.NK_LINK_ID = AppctnUWSts.AddressBookUID AND INDIV.NK_PUBLC_ID IS NULL */
                  ON              upper(indiv.nk_publc_id) = appctnuwsts.addressbookuid
                                  /* -------------------------------------------------------------------------------- */
                  left join
                                  (
                                                  SELECT DISTINCT host_issu_key,
                                                                  aplctn_undrwrt_req_issu_key_id
                                                  FROM            db_t_prod_core.aplctn_undrwrt_reqmt_issu_key
                                                  WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' ) AS auw_req_issu
                  ON              auw_req_issu.host_issu_key= appctnuwsts.issuekey
                                  /* -------------------------------------------------------------------------------- */
                  left join
                                  (
                                           SELECT   *
                                           FROM     db_t_prod_core.aplctn_undrwrt_reqmt_sts qualify row_number() over(PARTITION BY aplctn_id,undrwrtg_reqmt_type_cd,aplctn_undrwrt_req_issu_key_id,undrwrtg_sts_cd, aplctn_undrwrt_sts_strt_dttm,indiv_prty_id ORDER BY edw_end_dttm DESC)=1) tgt
                  ON              tgt.aplctn_id = aplctn.aplctn_id
                  AND             tgt.undrwrtg_reqmt_type_cd = uw_reqmt_type_cd
                  AND             tgt.aplctn_undrwrt_req_issu_key_id = auw_req_issu.aplctn_undrwrt_req_issu_key_id
                  AND             tgt.aplctn_undrwrt_sts_strt_dttm=busnstartdate
                  AND             tgt.undrwrtg_sts_cd=uw_sts_cd
                  AND             tgt.indiv_prty_id=uw_indiv_prty_id
                  WHERE           ins_flag=''I'' ) src ) );
  -- Component exp_passthrough_expression, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_passthrough_expression AS
  (
         SELECT sq_pc_uwissuehistory.uw_aplctn_id                                      AS uw_aplctn_id,
                sq_pc_uwissuehistory.uw_reqmt_type_cd                                  AS uw_reqmt_type_cd,
                sq_pc_uwissuehistory.uw_sts_cd                                         AS uw_sts_cd,
                sq_pc_uwissuehistory.uw_aplctn_sts_strt_dttm                           AS uw_aplctn_sts_strt_dttm,
                sq_pc_uwissuehistory.uw_issu_key_id                                    AS uw_issu_key_id,
                sq_pc_uwissuehistory.uw_indiv_prty_id                                  AS uw_indiv_prty_id,
                :prcs_id                                                               AS prcs_id,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS uw_aplctn_sts_end_dttm,
                sq_pc_uwissuehistory.rnk                                               AS rnk,
                sq_pc_uwissuehistory.edw_strt_dttm                                     AS uw_edw_strt_dttm,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS uw_edw_end_dttm,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS uw_trns_end_dttm,
                sq_pc_uwissuehistory.ins_flag                                          AS ins_flag,
                sq_pc_uwissuehistory.trans_strt_dttm                                   AS trans_strt_dttm,
                sq_pc_uwissuehistory.source_record_id
         FROM   sq_pc_uwissuehistory );
  -- Component rtr_rtr_aplctn_undrwrt_reqmt_sts_insert, Type ROUTER Output Group insert
  CREATE OR replace TEMPORARY TABLE rtr_rtr_aplctn_undrwrt_reqmt_sts_insert as
  SELECT exp_passthrough_expression.uw_aplctn_id            AS uw_aplctn_id,
         exp_passthrough_expression.uw_reqmt_type_cd        AS uw_reqmt_type_cd,
         exp_passthrough_expression.uw_sts_cd               AS uw_sts_cd,
         exp_passthrough_expression.uw_aplctn_sts_strt_dttm AS uw_aplctn_sts_strt_dttm,
         exp_passthrough_expression.uw_issu_key_id          AS uw_issu_key_id,
         exp_passthrough_expression.uw_indiv_prty_id        AS uw_indiv_prty_id,
         exp_passthrough_expression.prcs_id                 AS prcs_id,
         exp_passthrough_expression.uw_aplctn_sts_end_dttm  AS uw_aplctn_sts_end_dttm,
         exp_passthrough_expression.uw_edw_strt_dttm        AS uw_edw_strt_dttm,
         exp_passthrough_expression.uw_edw_end_dttm         AS uw_edw_end_dttm,
         exp_passthrough_expression.uw_trns_end_dttm        AS uw_trns_end_dttm,
         exp_passthrough_expression.rnk                     AS rnk,
         exp_passthrough_expression.ins_flag                AS ins_flag,
         exp_passthrough_expression.trans_strt_dttm         AS trans_strt_dttm,
         exp_passthrough_expression.source_record_id
  FROM   exp_passthrough_expression
  WHERE  exp_passthrough_expression.ins_flag = ''I''
  AND    NOT (
                exp_passthrough_expression.uw_aplctn_id IS NULL
         OR     exp_passthrough_expression.uw_issu_key_id IS NULL
         OR     exp_passthrough_expression.uw_sts_cd IS NULL );
  
  -- Component upd_aplctn_undrwrt_reqmt_sts_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_undrwrt_reqmt_sts_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_aplctn_id            AS aplctn_id,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_reqmt_type_cd        AS undrwrtg_reqmt_type_cd,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_sts_cd               AS undrwrtg_sts_cd,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_aplctn_sts_strt_dttm AS aplctn_undrwrt_sts_strt_dttm,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_issu_key_id          AS aplctn_undrwrt_req_issu_key_id1,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_indiv_prty_id        AS indiv_prty_id1,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.prcs_id                 AS prcs_id,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_aplctn_sts_end_dttm  AS uw_aplctn_sts_end_dttm1,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_edw_strt_dttm        AS uw_edw_strt_dttm1,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_edw_end_dttm         AS uw_edw_end_dttm1,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.uw_trns_end_dttm        AS uw_trns_end_dttm1,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.trans_strt_dttm         AS trans_strt_dttm1,
                0                                                               AS update_strategy_action,
                rtr_rtr_aplctn_undrwrt_reqmt_sts_insert.source_record_id
         FROM   rtr_rtr_aplctn_undrwrt_reqmt_sts_insert );
  -- Component tgt_APLCTN_UNDRWRT_REQMT_STS_ins, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_undrwrt_reqmt_sts
              (
                          aplctn_id,
                          undrwrtg_reqmt_type_cd,
                          aplctn_undrwrt_req_issu_key_id,
                          undrwrtg_sts_cd,
                          aplctn_undrwrt_sts_strt_dttm,
                          indiv_prty_id,
                          prcs_id,
                          aplctn_undrwrt_sts_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT upd_aplctn_undrwrt_reqmt_sts_ins.aplctn_id                       AS aplctn_id,
         upd_aplctn_undrwrt_reqmt_sts_ins.undrwrtg_reqmt_type_cd          AS undrwrtg_reqmt_type_cd,
         upd_aplctn_undrwrt_reqmt_sts_ins.aplctn_undrwrt_req_issu_key_id1 AS aplctn_undrwrt_req_issu_key_id,
         upd_aplctn_undrwrt_reqmt_sts_ins.undrwrtg_sts_cd                 AS undrwrtg_sts_cd,
         upd_aplctn_undrwrt_reqmt_sts_ins.aplctn_undrwrt_sts_strt_dttm    AS aplctn_undrwrt_sts_strt_dttm,
         upd_aplctn_undrwrt_reqmt_sts_ins.indiv_prty_id1                  AS indiv_prty_id,
         upd_aplctn_undrwrt_reqmt_sts_ins.prcs_id                         AS prcs_id,
         upd_aplctn_undrwrt_reqmt_sts_ins.uw_aplctn_sts_end_dttm1         AS aplctn_undrwrt_sts_end_dttm,
         upd_aplctn_undrwrt_reqmt_sts_ins.uw_edw_strt_dttm1               AS edw_strt_dttm,
         upd_aplctn_undrwrt_reqmt_sts_ins.uw_edw_end_dttm1                AS edw_end_dttm,
         upd_aplctn_undrwrt_reqmt_sts_ins.trans_strt_dttm1                AS trans_strt_dttm,
         upd_aplctn_undrwrt_reqmt_sts_ins.uw_trns_end_dttm1               AS trans_end_dttm
  FROM   upd_aplctn_undrwrt_reqmt_sts_ins;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component SQ_pc_uwissuehistory1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_uwissuehistory1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS publicid,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT publicid_stg
                                         FROM   db_t_prod_stag.pc_policyperiod
                                         WHERE  1=2 ) src ) );
  -- Component tgt_APLCTN_UNDRWRT_REQMT_STS_POST_SQL, Type TARGET
  INSERT INTO db_t_prod_core.aplctn_undrwrt_reqmt_sts
              (
                          indiv_prty_id
              )
  SELECT sq_pc_uwissuehistory1.publicid AS indiv_prty_id
  FROM   sq_pc_uwissuehistory1;
  
  -- PIPELINE END FOR 2
  -- Component tgt_APLCTN_UNDRWRT_REQMT_STS_POST_SQL, Type Post SQL
  UPDATE db_t_prod_core.aplctn_undrwrt_reqmt_sts
    SET    edw_end_dttm=tmplead.edw_lead ,
         trans_end_dttm= tmplead.trans_lead
  FROM   (
                         SELECT DISTINCT aplctn_id,
                                         undrwrtg_reqmt_type_cd,
                                         aplctn_undrwrt_sts_strt_dttm,
                                         aplctn_undrwrt_req_issu_key_id,
                                         undrwrtg_sts_cd,
                                         indiv_prty_id,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY aplctn_id,undrwrtg_reqmt_type_cd,aplctn_undrwrt_req_issu_key_id ORDER BY edw_strt_dttm ASC,trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS trans_lead,
                                         max(edw_strt_dttm) over (PARTITION BY aplctn_id,undrwrtg_reqmt_type_cd,aplctn_undrwrt_req_issu_key_id ORDER BY edw_strt_dttm ASC,trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS edw_lead
                         FROM            db_t_prod_core.aplctn_undrwrt_reqmt_sts -- WHERE EDW_END_DTTM=''9999-12-31 23:59:59.999999''
         ) tmplead

  WHERE  aplctn_undrwrt_reqmt_sts.aplctn_id=tmplead.aplctn_id
  AND    aplctn_undrwrt_reqmt_sts.undrwrtg_reqmt_type_cd=tmplead.undrwrtg_reqmt_type_cd
  AND    aplctn_undrwrt_reqmt_sts.undrwrtg_sts_cd=tmplead.undrwrtg_sts_cd
  AND    aplctn_undrwrt_reqmt_sts.aplctn_undrwrt_req_issu_key_id=tmplead.aplctn_undrwrt_req_issu_key_id
  AND    aplctn_undrwrt_reqmt_sts.aplctn_undrwrt_sts_strt_dttm=tmplead.aplctn_undrwrt_sts_strt_dttm
  AND    aplctn_undrwrt_reqmt_sts.indiv_prty_id=tmplead.indiv_prty_id
  AND    aplctn_undrwrt_reqmt_sts.edw_strt_dttm=tmplead.edw_strt_dttm
  AND    aplctn_undrwrt_reqmt_sts.trans_strt_dttm=tmplead.trans_strt_dttm
  AND    tmplead.trans_lead IS NOT NULL
  AND    edw_end_dttm<>tmplead.edw_lead;

END;
';