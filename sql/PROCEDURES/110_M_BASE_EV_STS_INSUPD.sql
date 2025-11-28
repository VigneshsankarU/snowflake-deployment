-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EV_STS_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- PIPELINE START FOR 1
  -- Component SQ_pc_job_PLCYTRANS, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_job_plcytrans AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS tgt_ev_id,
                $2  AS tgt_ev_sts_type_cd,
                $3  AS tgt_ev_sts_strt_dttm,
                $4  AS tgt_ev_sts_txt,
                $5  AS tgt_agmt_id,
                $6  AS tgt_quotn_id,
                $7  AS src_ev_id,
                $8  AS src_ev_sts_type_cd,
                $9  AS src_ev_sts_strt_dttm,
                $10 AS src_ev_sts_txt,
                $11 AS src_agmt_id,
                $12 AS src_quotn_id,
                $13 AS src_trans_strt_dttm,
                $14 AS src_rnk,
                $15 AS src_md5,
                $16 AS tgt_md5,
                $17 AS ins_upd_flag,
                $18 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT tgt_ev_sts.ev_id              AS tgt_ev_id,
                                                                  tgt_ev_sts.ev_sts_type_cd     AS tgt_ev_sts_type_cd ,
                                                                  tgt_ev_sts.ev_sts_strt_dttm   AS tgt_ev_sts_strt_dttm,
                                                                  tgt_ev_sts.ev_sts_txt         AS tgt_ev_sts_txt,
                                                                  tgt_ev_sts.agmt_id            AS tgt_agmt_id,
                                                                  tgt_ev_sts.quotn_id           AS tgt_quotn_id,
                                                                  xlat_src.ev_id                AS src_ev_id,
                                                                  xlat_src.src_ev_sts_type_cd   AS src_ev_sts_type_cd,
                                                                  xlat_src.src_ev_sts_strt_dttm AS src_ev_sts_strt_dttm,
                                                                  xlat_src. src_ev_sts_txt      AS src_ev_sts_txt,
                                                                  xlat_src.agmt_id              AS src_agmt_id,
                                                                  xlat_src.quotn_id             AS src_quotn_id,
                                                                  xlat_src.src_trans_strt_dt    AS src_trans_strt_dttm,
                                                                  xlat_src.rnk                  AS src_rnk,
                                                                  /* Source data */
                                                                  cast((trim(coalesce(xlat_src.agmt_id,0))
                                                                                  || trim(coalesce(xlat_src.quotn_id,0))
                                                                                  || trim(to_char((coalesce(xlat_src.src_ev_sts_strt_dttm, to_date(''12/31/9999'',''MM/DD/YYYY'')))))
                                                                                  || trim(coalesce(xlat_src. src_ev_sts_txt, 0))
                                                                                  || trim(coalesce(xlat_src.src_ev_sts_type_cd, 0))) AS VARCHAR(1100)) AS src_md5,
                                                                  /* target data */
                                                                  cast((trim(coalesce(tgt_ev_sts.agmt_id,0))
                                                                                  || trim(coalesce(tgt_ev_sts.quotn_id,0))
                                                                                  || trim(to_char((coalesce(tgt_ev_sts.ev_sts_strt_dttm, to_date(''12/31/9999'',''MM/DD/YYYY'')))))
                                                                                  || trim(coalesce(tgt_ev_sts.ev_sts_txt, 0))
                                                                                  || trim(coalesce(tgt_ev_sts.ev_sts_type_cd, 0))) AS VARCHAR(1100)) AS tgt_md5,
                                                                  /* Flag */
                                                                  CASE
                                                                                  WHEN tgt_ev_sts.ev_id IS NULL
                                                                                  AND             xlat_src.ev_id IS NOT NULL THEN ''I''
                                                                                  WHEN tgt_ev_sts.ev_id IS NOT NULL
                                                                                  AND             src_md5 <> tgt_md5 THEN ''U''
                                                                                  WHEN tgt_ev_sts.ev_id IS NOT NULL
                                                                                  AND             tgt_md5 =tgt_md5
                                                                                  AND             (
                                                                                                                  tgt_ev_sts.trans_end_dttm<>''9999-12-31 23:59:59.999999''
                                                                                                  AND             cast(src_trans_strt_dt AS DATE)>cast(tgt_ev_sts.trans_strt_dttm AS DATE)) THEN ''U''
                                                                                  ELSE ''R''
                                                                  END AS ins_upd_flag
                                                  FROM            (
                                                                                  SELECT DISTINCT xlat_ev_cd.tgt_idntftn_val AS src_ev_act_type_code ,
                                                                                                  key1 ,
                                                                                                  xlat_src_ev_cd.tgt_idntftn_val               AS src_subtype ,
                                                                                                  typecode_riskstatus                          AS src_ev_sts_txt ,
                                                                                                  strt_dt                                      AS src_ev_sts_strt_dttm ,
                                                                                                  coalesce(xlat_ev_sts.tgt_idntftn_val, ''UNK'') AS src_ev_sts_type_cd ,
                                                                                                  branchnumber,
                                                                                                  nk_publicid   AS src_nk_publicid,
                                                                                                  pc_updatetime AS src_trans_strt_dt,
                                                                                                  rnk,
                                                                                                  lkp_ev.ev_id AS ev_id,
                                                                                                  CASE
                                                                                                                  WHEN lkp_quotn.quotn_id IS NULL THEN ''9999''
                                                                                                                  ELSE lkp_quotn.quotn_id
                                                                                                  END AS quotn_id ,
                                                                                                  CASE
                                                                                                                  WHEN lkp_agmt_ppv.agmt_id IS NULL THEN
                                                                                                                                  CASE
                                                                                                                                                  WHEN lkp_agmt_act.agmt_id IS NULL THEN lkp_agmt_inv.agmt_id
                                                                                                                                                  ELSE lkp_agmt_act.agmt_id
                                                                                                                                  END
                                                                                                                  ELSE lkp_agmt_ppv.agmt_id
                                                                                                  END AS agmt_id
                                                                                  FROM           (
                                                                                                                  SELECT DISTINCT ev_act_type_code,
                                                                                                                                  key1,
                                                                                                                                  SUBTYPE,
                                                                                                                                  status,
                                                                                                                                  strt_dt,
                                                                                                                                  typecode_riskstatus,
                                                                                                                                  branchnumber,
                                                                                                                                  nk_publicid,
                                                                                                                                  pc_updatetime,
                                                                                                                                  row_number() over( PARTITION BY ev_act_type_code, key1, SUBTYPE,branchnumber ORDER BY pc_updatetime, pc_createtime ) AS rnk
                                                                                                                  FROM            (
                                                                                                                                                  SELECT DISTINCT pctl_job.typecode_stg                                                                                                                                            AS ev_act_type_code ,
                                                                                                                                                                  pc_job.jobnumber                                                                                                                                                 AS key1 ,
                                                                                                                                                                  ''EV_SBTYPE3''                                                                                                                                                     AS SUBTYPE ,
                                                                                                                                                                  pc_job.typecode_policyperiodstatus                                                                                                                               AS status ,
                                                                                                                                                                  pc_job.updatetime_policyperiod                                                                                                                                   AS strt_dt ,
                                                                                                                                                                  coalesce (pc_job.typecode_rejectreason, pc_job.typecode_cancelreason,pc_job.typecode_reinstatecode, pc_job.typecode_nonrenewalcode, pc_job.typecode_renewalcode) AS reason ,
                                                                                                                                                                  pc_job.typecode_riskstatus ,
                                                                                                                                                                  cast(pc_job.branchnumber AS VARCHAR(255)) AS branchnumber ,
                                                                                                                                                                  pc_job.nk_publicid                        AS nk_publicid ,
                                                                                                                                                                  pc_job.pc_updatetime,
                                                                                                                                                                  pc_job.pc_createtime
                                                                                                                                                  FROM            (
                                                                                                                                                                                  SELECT          pc_job.jobnumber_stg AS jobnumber ,
                                                                                                                                                                                                  CASE
                                                                                                                                                                                                        WHEN pctl_job.typecode_stg IN ( ''Submission'',
                                                                                                                                                                                                        ''Renewal'',
                                                                                                                                                                                                        ''Rewrite'',
                                                                                                                                                                                                        ''Issuance'' ) THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                        WHEN pctl_job.typecode_stg = ''Cancellation'' THEN pc_policyperiod.cancellationdate_stg
                                                                                                                                                                                                        WHEN pctl_job.typecode_stg IN (''PolicyChange'',
                                                                                                                                                                                                        ''Reinstatement'') THEN pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                                  END                                     updatetime_policyperiod ,
                                                                                                                                                                                                  pctl_policyperiodstatus.typecode_stg AS typecode_policyperiodstatus ,
                                                                                                                                                                                                  cancelreason.typecode_stg            AS typecode_cancelreason ,
                                                                                                                                                                                                  rejectreason.typecode_stg            AS typecode_rejectreason ,
                                                                                                                                                                                                  pctl_reinstatecode.typecode_stg      AS typecode_reinstatecode ,
                                                                                                                                                                                                  pctl_renewalcode.typecode_stg        AS typecode_renewalcode ,
                                                                                                                                                                                                  pctl_nonrenewalcode.typecode_stg     AS typecode_nonrenewalcode ,
                                                                                                                                                                                                  pc_policyperiod.branchnumber_stg     AS branchnumber ,
                                                                                                                                                                                                  pc_policyperiod.publicid_stg         AS nk_publicid ,
                                                                                                                                                                                                  pctl_riskstatus_alfa.typecode_stg    AS typecode_riskstatus ,
                                                                                                                                                                                                  pc_policyperiod.updatetime_stg       AS pc_updatetime,
                                                                                                                                                                                                  pc_policyperiod.createtime_stg       AS pc_createtime,
                                                                                                                                                                                                  pc_job.subtype_stg                   AS SUBTYPE
                                                                                                                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                                                                                                                  left join       db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_reasoncode AS rejectreason
                                                                                                                                                                                  ON              rejectreason.id_stg = pc_job.rejectreason_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_reasoncode AS cancelreason
                                                                                                                                                                                  ON              cancelreason.id_stg = pc_job.cancelreasoncode_stg
                                                                                                                                                                                  left join       db_t_prod_stag.pc_policyterm
                                                                                                                                                                                  ON              pc_policyterm.id_stg = pc_policyperiod.policytermid_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_reinstatecode
                                                                                                                                                                                  ON              pctl_reinstatecode.id_stg = pc_job.reinstatecode_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_nonrenewalcode
                                                                                                                                                                                  ON              pctl_nonrenewalcode.id_stg = pc_policyterm.nonrenewreason_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_renewalcode
                                                                                                                                                                                  ON              pctl_renewalcode.id_stg = pc_job.renewalcode_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_policyline
                                                                                                                                                                                  ON              pc_policyline.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                                  inner join      db_t_prod_stag.pctl_job
                                                                                                                                                                                  ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_user
                                                                                                                                                                                  ON              pc_job.createuserid_stg = pc_user.id_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_contact
                                                                                                                                                                                  ON              pc_user.contactid_stg= pc_contact.id_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_quotetype
                                                                                                                                                                                  ON              pctl_quotetype.id_stg=pc_job.quotetype_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_riskstatus_alfa
                                                                                                                                                                                  ON              pctl_riskstatus_alfa.id_stg=pc_job.risk_alfa_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_jobpolicyperiod
                                                                                                                                                                                  ON              pc_job.id_stg = pc_jobpolicyperiod.ownerid_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_policyperiod AS pcp1
                                                                                                                                                                                  ON              pc_jobpolicyperiod.foreignentityid_stg = pcp1.id_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_sourceofbusiness_alfa
                                                                                                                                                                                  ON              pc_effectivedatedfields.sourceofbusiness_alfa_stg = pctl_sourceofbusiness_alfa.id_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pcx_holineratingfactor_alfa
                                                                                                                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                                                                                                                  left outer join db_t_prod_stag.pctl_billingperiodicity
                                                                                                                                                                                  ON              pcx_holineratingfactor_alfa.autolatepaybillingperiodicity_stg=pctl_billingperiodicity.id_stg
                                                                                                                                                                                  left join       db_t_prod_stag.pc_groupuser
                                                                                                                                                                                  ON              pc_user.id_stg = pc_groupuser.userid_stg
                                                                                                                                                                                  left join       db_t_prod_stag.pc_group
                                                                                                                                                                                  ON              pc_groupuser.groupid_stg = pc_group.id_stg
                                                                                                                                                                                  left join       db_t_prod_stag.pctl_grouptype
                                                                                                                                                                                  ON              pc_group.grouptype_stg = pctl_grouptype.id_stg
                                                                                                                                                                                  left join       db_t_prod_stag.pctl_cancellationsource
                                                                                                                                                                                  ON              pc_job.source_stg = pctl_cancellationsource.id_stg
                                                                                                                                                                                  left join
                                                                                                                                                                                                  (
                                                                                                                                                                                                        SELECT DISTINCT jobid_stg
                                                                                                                                                                                                        FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                                                                                                        WHERE           quotematuritylevel_stg IN (2,3) ) vj
                                                                                                                                                                                  ON              pc_job.id_stg=vj.jobid_stg
                                                                                                                                                                                                  /* and pctl_grouptype.TYPECODE in (''region'', ''salesdistrict_alfa'', ''servicecenter_alfa'',''custserv'') */
                                                                                                                                                                                  WHERE           pc_policyperiod.updatetime_stg > ($start_dttm)
                                                                                                                                                                                  AND             pc_policyperiod.updatetime_stg <= ($end_dttm)
                                                                                                                                                                                  AND             pctl_policyperiodstatus.typecode_stg NOT IN(''Temporary'' )
                                                                                                                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL ) pc_job
                                                                                                                                                  inner join      db_t_prod_stag.pctl_job
                                                                                                                                                  ON              pctl_job.id_stg=pc_job.SUBTYPE
                                                                                                                                                  WHERE
                                                                                                                                                                  /*jobnumber = ''J3005745671''
and*/
                                                                                                                                                                  pc_job.nk_publicid IS NOT NULL
                                                                                                                                                  AND             typecode_policyperiodstatus <> ''TEMPORARY''
                                                                                                                                                  AND             updatetime_policyperiod IS NOT NULL ) x ) src
                                                                                                  /* LKP_TERDATA_ETL_XLAT_EV_ACTY_CD */
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                                                                          ''DS'' )
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) xlat_ev_cd
                                                                                  ON              src.ev_act_type_code=xlat_ev_cd.src_idntftn_val
                                                                                                  /* LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD */
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_SBTYPE''
                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) AS xlat_src_ev_cd
                                                                                  ON              src.SUBTYPE=xlat_src_ev_cd.src_idntftn_val
                                                                                                  /* LKP_TERADATA_ETL_REF_XLAT_EV_STS */
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_STS_TYPE''
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )xlat_ev_sts
                                                                                  ON              src.status=xlat_ev_sts.src_idntftn_val
                                                                                                  /*  LOOK UP TABLE DB_T_PROD_CORE.EV  */
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   ev.ev_id            AS ev_id,
                                                                                                                    ev.ev_end_dttm      AS ev_end_dttm,
                                                                                                                    ev.src_trans_id     AS src_trans_id,
                                                                                                                    ev.ev_sbtype_cd     AS ev_sbtype_cd,
                                                                                                                    ev.ev_actvy_type_cd AS ev_actvy_type_cd
                                                                                                           FROM     db_t_prod_core.ev   AS ev qualify row_number() over( PARTITION BY ev.ev_sbtype_cd,ev.ev_actvy_type_cd, ev.src_trans_id ORDER BY ev.edw_end_dttm DESC) = 1 ) lkp_ev
                                                                                  ON              lkp_ev.src_trans_id=src.key1
                                                                                  AND             lkp_ev.ev_sbtype_cd=xlat_src_ev_cd.tgt_idntftn_val
                                                                                  AND             lkp_ev.ev_actvy_type_cd=xlat_ev_cd.tgt_idntftn_val
                                                                                                  /* LOOK UP DB_T_PROD_CORE.INSRNC_QUOTN */
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   insrnc_quotn.quotn_id       AS quotn_id,
                                                                                                                    insrnc_quotn.nk_job_nbr     AS nk_job_nbr,
                                                                                                                    insrnc_quotn.vers_nbr       AS vers_nbr
                                                                                                           FROM     db_t_prod_core.insrnc_quotn AS insrnc_quotn qualify row_number() over( PARTITION BY insrnc_quotn.nk_job_nbr, insrnc_quotn.vers_nbr, insrnc_quotn.src_sys_cd ORDER BY insrnc_quotn.edw_end_dttm DESC) = 1 )lkp_quotn
                                                                                  ON              lkp_quotn.vers_nbr=src.branchnumber
                                                                                  AND             lkp_quotn.nk_job_nbr=src.key1
                                                                                                  /* LOOK UP AGMT_PPV */
                                                                                  left outer join ( select
                                                                                                  CASE
                                                                                                                  WHEN a.agmt_type_cd=''PPV'' THEN agmt_id
                                                                                                                  ELSE NULL
                                                                                                  END AS agmt_id, a.nk_src_key,a.agmt_type_cd FROM(
                                                                                                         SELECT agmt.agmt_id       AS agmt_id,
                                                                                                                agmt.host_agmt_num AS host_agmt_num,
                                                                                                                agmt.nk_src_key    AS nk_src_key,
                                                                                                                agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                                agmt.edw_end_dttm
                                                                                                         FROM   db_t_prod_core.agmt AS agmt
                                                                                                         WHERE  agmt_type_cd IN(''PPV'')
                                                                                                                /* QUALIFY ROW_NUMBER() OVER( PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1 */
                                                                                                         AND    cast(agmt.edw_end_dttm AS DATE) =''9999-12-31'' ) a )lkp_agmt_ppv
                                                                                  ON              lkp_agmt_ppv.nk_src_key=src.nk_publicid
                                                                                  AND             lkp_agmt_ppv.agmt_type_cd=''$p_agmt_type_cd_policy_version''
                                                                                                  /* LOOK UP AGMT_ACT */
                                                                                  left outer join ( select
                                                                                                  CASE
                                                                                                                  WHEN a.agmt_type_cd=''ACT'' THEN agmt_id
                                                                                                                  ELSE NULL
                                                                                                  END AS agmt_id, a.nk_src_key,a.agmt_type_cd FROM(
                                                                                                           SELECT   agmt.agmt_id       AS agmt_id,
                                                                                                                    agmt.host_agmt_num AS host_agmt_num,
                                                                                                                    agmt.nk_src_key    AS nk_src_key,
                                                                                                                    agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                                    agmt.edw_end_dttm
                                                                                                           FROM     db_t_prod_core.agmt AS agmt
                                                                                                           WHERE    agmt_type_cd IN(''ACT'') qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) a )lkp_agmt_act
                                                                                  ON              lkp_agmt_act.nk_src_key=src.nk_publicid
                                                                                  AND             lkp_agmt_act.agmt_type_cd=''ACT''
                                                                                                  /* LOOK UP AGMT_INV */
                                                                                  left outer join ( select
                                                                                                  CASE
                                                                                                                  WHEN a.agmt_type_cd=''INV'' THEN agmt_id
                                                                                                                  ELSE NULL
                                                                                                  END AS agmt_id, a.nk_src_key,a.agmt_type_cd FROM(
                                                                                                         SELECT agmt.agmt_id       AS agmt_id,
                                                                                                                agmt.host_agmt_num AS host_agmt_num,
                                                                                                                agmt.nk_src_key    AS nk_src_key,
                                                                                                                agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                                agmt.edw_end_dttm
                                                                                                         FROM   db_t_prod_core.agmt AS agmt
                                                                                                         WHERE  agmt_type_cd IN(''INV'')
                                                                                                                /* QUALIFY ROW_NUMBER() OVER(  PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1 */
                                                                                                         AND    cast(agmt.edw_end_dttm AS DATE) =''9999-12-31'' ) a )lkp_agmt_inv
                                                                                  ON              lkp_agmt_inv.nk_src_key=src.nk_publicid
                                                                                  AND             lkp_agmt_inv.agmt_type_cd=''INV'' )xlat_src
                                                  left outer join
                                                                  (
                                                                           SELECT   ev_sts.ev_sts_strt_dttm AS ev_sts_strt_dttm,
                                                                                    ev_sts.trans_strt_dttm,
                                                                                    ev_sts.trans_end_dttm,
                                                                                    ev_sts.ev_sts_txt     AS ev_sts_txt,
                                                                                    ev_sts.agmt_id        AS agmt_id,
                                                                                    ev_sts.quotn_id       AS quotn_id,
                                                                                    ev_sts.ev_id          AS ev_id,
                                                                                    ev_sts.ev_sts_type_cd AS ev_sts_type_cd
                                                                           FROM     db_t_prod_core.ev_sts AS ev_sts
                                                                           join     db_t_prod_core.ev     AS ev
                                                                           ON       ev.ev_id=ev_sts.ev_id
                                                                           join
                                                                                    (
                                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_SBTYPE''
                                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31''
                                                                                           AND    src_idntftn_val=''EV_SBTYPE3'') teradata_etl_ref_xlat
                                                                           ON       teradata_etl_ref_xlat.tgt_idntftn_val=ev.ev_sbtype_cd qualify row_number () over ( PARTITION BY ev_sts.ev_id, ev_sts.quotn_id ,ev_sts_type_cd ORDER BY ev_sts.edw_end_dttm DESC)=1
                                                                                    /* WHERE  CAST(EV_STS.EDW_END_DTTM AS DATE) =''9999-12-31'' */
                                                                  ) tgt_ev_sts
                                                  ON              tgt_ev_sts.ev_id=xlat_src.ev_id
                                                  AND             cast(tgt_ev_sts.quotn_id AS       DECIMAL(19,0))=cast(xlat_src.quotn_id AS DECIMAL(19,0))
                                                  AND             cast(tgt_ev_sts.ev_sts_type_cd AS VARCHAR(50))=cast(xlat_src.src_ev_sts_type_cd AS VARCHAR(50)) ) src ) );
  -- Component exp_pass_from_src1_PLCYTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_src1_plcytrans AS
  (
         SELECT sq_pc_job_plcytrans.tgt_ev_id                                          AS tgt_ev_id,
                sq_pc_job_plcytrans.tgt_ev_sts_type_cd                                 AS tgt_ev_sts_type_cd,
                sq_pc_job_plcytrans.tgt_ev_sts_strt_dttm                               AS tgt_ev_sts_strt_dttm,
                sq_pc_job_plcytrans.tgt_ev_sts_txt                                     AS tgt_ev_sts_txt,
                sq_pc_job_plcytrans.tgt_agmt_id                                        AS tgt_agmt_id,
                sq_pc_job_plcytrans.tgt_quotn_id                                       AS tgt_quotn_id,
                NULL                                                                   AS tgt_edw_strt_dttm,
                NULL                                                                   AS tgt_edw_end_dttm,
                sq_pc_job_plcytrans.src_ev_id                                          AS src_ev_id,
                sq_pc_job_plcytrans.src_ev_sts_type_cd                                 AS src_ev_sts_type_cd,
                sq_pc_job_plcytrans.src_ev_sts_strt_dttm                               AS src_ev_sts_strt_dttm,
                sq_pc_job_plcytrans.src_ev_sts_txt                                     AS src_ev_sts_txt,
                sq_pc_job_plcytrans.src_agmt_id                                        AS src_agmt_id,
                sq_pc_job_plcytrans.src_quotn_id                                       AS src_quotn_id,
                sq_pc_job_plcytrans.src_trans_strt_dttm                                AS src_trans_strt_dttm,
                sq_pc_job_plcytrans.src_rnk                                            AS src_rnk,
                sq_pc_job_plcytrans.ins_upd_flag                                       AS ins_upd_flag,
                current_timestamp                                                      AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                $prcs_id                                                               AS out_prcs_id,
                sq_pc_job_plcytrans.source_record_id
         FROM   sq_pc_job_plcytrans );
  -- Component rtr_ev_sts_PLCYTRANS_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_ev_sts_plcytrans_insert as
  SELECT exp_pass_from_src1_plcytrans.tgt_ev_id            AS tgt_ev_id,
         exp_pass_from_src1_plcytrans.tgt_ev_sts_type_cd   AS tgt_ev_sts_type_cd,
         exp_pass_from_src1_plcytrans.tgt_ev_sts_strt_dttm AS tgt_ev_sts_strt_dttm,
         exp_pass_from_src1_plcytrans.tgt_ev_sts_txt       AS tgt_ev_sts_txt,
         exp_pass_from_src1_plcytrans.tgt_agmt_id          AS tgt_agmt_id,
         exp_pass_from_src1_plcytrans.tgt_quotn_id         AS tgt_quotn_id,
         exp_pass_from_src1_plcytrans.tgt_edw_strt_dttm    AS tgt_edw_strt_dttm,
         exp_pass_from_src1_plcytrans.tgt_edw_end_dttm     AS tgt_edw_end_dttm,
         exp_pass_from_src1_plcytrans.src_ev_id            AS src_ev_id,
         exp_pass_from_src1_plcytrans.src_ev_sts_type_cd   AS src_ev_sts_type_cd,
         exp_pass_from_src1_plcytrans.src_ev_sts_strt_dttm AS src_ev_sts_strt_dttm,
         exp_pass_from_src1_plcytrans.src_ev_sts_txt       AS src_ev_sts_txt,
         exp_pass_from_src1_plcytrans.src_agmt_id          AS src_agmt_id,
         exp_pass_from_src1_plcytrans.src_quotn_id         AS src_quotn_id,
         exp_pass_from_src1_plcytrans.src_trans_strt_dttm  AS src_trans_strt_dttm,
         exp_pass_from_src1_plcytrans.src_rnk              AS src_rnk,
         exp_pass_from_src1_plcytrans.ins_upd_flag         AS ins_upd_flag,
         exp_pass_from_src1_plcytrans.out_edw_strt_dttm    AS edw_strt_dttm,
         exp_pass_from_src1_plcytrans.out_edw_end_dttm     AS edw_end_dttm,
         exp_pass_from_src1_plcytrans.out_prcs_id          AS prcs_id,
         exp_pass_from_src1_plcytrans.source_record_id
  FROM   exp_pass_from_src1_plcytrans
  WHERE  exp_pass_from_src1_plcytrans.src_ev_id IS NOT NULL
  AND    (
                exp_pass_from_src1_plcytrans.ins_upd_flag = ''I''
         OR     exp_pass_from_src1_plcytrans.ins_upd_flag = ''U'' );
  
  -- Component upd_ev_sts_ins_PLCYTRANS, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_sts_ins_plcytrans AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_sts_plcytrans_insert.src_ev_id            AS in_ev_id,
                rtr_ev_sts_plcytrans_insert.src_ev_sts_type_cd   AS in_ev_sts_type_cd,
                rtr_ev_sts_plcytrans_insert.src_ev_sts_strt_dttm AS in_ev_sts_strt_dttm,
                rtr_ev_sts_plcytrans_insert.src_ev_sts_txt       AS in_ev_sts_txt,
                rtr_ev_sts_plcytrans_insert.src_agmt_id          AS in_agmt_id,
                rtr_ev_sts_plcytrans_insert.src_quotn_id         AS in_quotn_id,
                rtr_ev_sts_plcytrans_insert.prcs_id              AS in_prcs_id,
                rtr_ev_sts_plcytrans_insert.edw_strt_dttm        AS in_edw_strt_dttm,
                rtr_ev_sts_plcytrans_insert.edw_end_dttm         AS in_edw_end_dttm,
                rtr_ev_sts_plcytrans_insert.src_trans_strt_dttm  AS transaction_eff_date,
                rtr_ev_sts_plcytrans_insert.src_rnk              AS rank1,
                0                                                AS update_strategy_action,
				rtr_ev_sts_plcytrans_insert.source_record_id     AS source_record_id
         FROM   rtr_ev_sts_plcytrans_insert );
  -- Component pass_to_tgt_ins_PLCYTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE pass_to_tgt_ins_plcytrans AS
  (
         SELECT upd_ev_sts_ins_plcytrans.in_ev_id                                                AS ev_id,
                upd_ev_sts_ins_plcytrans.in_ev_sts_type_cd                                       AS ev_sts_type_cd,
                upd_ev_sts_ins_plcytrans.in_ev_sts_strt_dttm                                     AS ev_sts_strt_dttm,
                upd_ev_sts_ins_plcytrans.in_ev_sts_txt                                           AS ev_sts_txt,
                upd_ev_sts_ins_plcytrans.in_agmt_id                                              AS agmt_id,
                upd_ev_sts_ins_plcytrans.in_quotn_id                                             AS quotn_id,
                upd_ev_sts_ins_plcytrans.in_prcs_id                                              AS prcs_id,
                dateadd(''second'', ( 2 * ( upd_ev_sts_ins_plcytrans.rank1 - 1 ) ), current_timestamp) AS out_edw_strt_dttm,
                upd_ev_sts_ins_plcytrans.in_edw_end_dttm                                         AS edw_end_dttm,
                upd_ev_sts_ins_plcytrans.transaction_eff_date                                    AS trans_strt_dttm,
                upd_ev_sts_ins_plcytrans.source_record_id
         FROM   upd_ev_sts_ins_plcytrans );
  -- Component tgt_EV_STS_ins_PLCYTRNS, Type TARGET
  INSERT INTO db_t_prod_core.ev_sts
              (
                          ev_id,
                          ev_sts_type_cd,
                          ev_sts_strt_dttm,
                          ev_sts_txt,
                          agmt_id,
                          quotn_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT pass_to_tgt_ins_plcytrans.ev_id             AS ev_id,
         pass_to_tgt_ins_plcytrans.ev_sts_type_cd    AS ev_sts_type_cd,
         pass_to_tgt_ins_plcytrans.ev_sts_strt_dttm  AS ev_sts_strt_dttm,
         pass_to_tgt_ins_plcytrans.ev_sts_txt        AS ev_sts_txt,
         pass_to_tgt_ins_plcytrans.agmt_id           AS agmt_id,
         pass_to_tgt_ins_plcytrans.quotn_id          AS quotn_id,
         pass_to_tgt_ins_plcytrans.prcs_id           AS prcs_id,
         pass_to_tgt_ins_plcytrans.out_edw_strt_dttm AS edw_strt_dttm,
         pass_to_tgt_ins_plcytrans.edw_end_dttm      AS edw_end_dttm,
         pass_to_tgt_ins_plcytrans.trans_strt_dttm   AS trans_strt_dttm
  FROM   pass_to_tgt_ins_plcytrans;
  
  -- PIPELINE END FOR 1
  -- Component tgt_EV_STS_ins_PLCYTRNS, Type Post SQL
  UPDATE db_t_prod_core.ev_sts
    SET    trans_end_dttm= a.lead1,
         edw_end_dttm = a.lead
  FROM   (
                         SELECT DISTINCT ev_sts.ev_id,
                                         ev_sts.quotn_id,
                                         ev_sts.edw_strt_dttm,
                                         max(ev_sts.edw_strt_dttm) over (PARTITION BY ev_sts.ev_id,ev_sts.quotn_id ORDER BY ev_sts.edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead,
                                         max(ev_sts.trans_strt_dttm) over (PARTITION BY ev_sts.ev_id,ev_sts.quotn_id ORDER BY ev_sts.edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.ev_sts
                         left join       db_t_prod_core.ev
                         ON              ev.ev_id=ev_sts.ev_id
                         WHERE           ev_sbtype_cd=''PLCYTRNS''
                         AND             ev.edw_end_dttm=''9999-12-31 23:59:59.999999'' ) a

  WHERE  ev_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    ev_sts.quotn_id=a.quotn_id
  AND    ev_sts.ev_id=a.ev_id
  AND    ev_sts.trans_strt_dttm <>ev_sts.trans_end_dttm
  AND    lead IS NOT NULL;
  
  -- PIPELINE START FOR 2
  -- Component SQ_pc_job, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_job AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS tgt_ev_id,
                $2  AS tgt_ev_sts_type_cd,
                $3  AS tgt_ev_sts_strt_dttm,
                $4  AS tgt_ev_sts_txt,
                $5  AS tgt_agmt_id,
                $6  AS tgt_quotn_id,
                $7  AS src_ev_id,
                $8  AS src_ev_sts_type_cd,
                $9  AS src_ev_sts_strt_dttm,
                $10 AS src_ev_sts_txt,
                $11 AS src_agmt_id,
                $12 AS src_quotn_id,
                $13 AS src_trans_strt_dttm,
                $14 AS src_rnk,
                $15 AS src_md5,
                $16 AS tgt_md5,
                $17 AS ins_upd_flag,
                $18 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT tgt_ev_sts.ev_id              AS tgt_ev_id,
                                                                  tgt_ev_sts.ev_sts_type_cd     AS tgt_ev_sts_type_cd ,
                                                                  tgt_ev_sts.ev_sts_strt_dttm   AS tgt_ev_sts_strt_dttm,
                                                                  tgt_ev_sts.ev_sts_txt         AS tgt_ev_sts_txt,
                                                                  tgt_ev_sts.agmt_id            AS tgt_agmt_id,
                                                                  tgt_ev_sts.quotn_id           AS tgt_quotn_id,
                                                                  xlat_src.ev_id                AS src_ev_id,
                                                                  xlat_src.src_ev_sts_type_cd   AS src_ev_sts_type_cd,
                                                                  xlat_src.src_ev_sts_strt_dttm AS src_ev_sts_strt_dttm,
                                                                  xlat_src. src_ev_sts_txt      AS src_ev_sts_txt,
                                                                  xlat_src.agmt_id              AS src_agmt_id,
                                                                  xlat_src.quotn_id             AS src_quotn_id,
                                                                  xlat_src.src_trans_strt_dt    AS src_trans_strt_dttm,
                                                                  xlat_src.rnk                  AS src_rnk,
                                                                  /* Source data */
                                                                  cast((trim(coalesce(xlat_src.agmt_id,0))
                                                                                  || trim(coalesce(xlat_src.quotn_id, 0))
                                                                                  || trim(to_char((coalesce(xlat_src.src_ev_sts_strt_dttm, to_date(''12/31/9999'',''MM/DD/YYYY'')))))
                                                                                  || trim(coalesce(xlat_src. src_ev_sts_txt, 0))
                                                                                  || trim(coalesce(xlat_src.src_ev_sts_type_cd, 0))) AS VARCHAR(1100)) AS src_md5,
                                                                  /* Target data */
                                                                  cast((trim(coalesce(tgt_ev_sts.agmt_id,0))
                                                                                  || trim(coalesce(tgt_ev_sts.quotn_id, 0))
                                                                                  || trim(to_char((coalesce(tgt_ev_sts.ev_sts_strt_dttm,to_date(''12/31/9999'', ''MM/DD/YYYY'')))))
                                                                                  || trim(coalesce(tgt_ev_sts.ev_sts_txt, 0))
                                                                                  || trim(coalesce(tgt_ev_sts.ev_sts_type_cd, 0))) AS VARCHAR(1100)) AS tgt_md5,
                                                                  /* Flag */
                                                                  CASE
                                                                                  WHEN tgt_ev_sts.ev_id IS NULL
                                                                                  AND             xlat_src.ev_id IS NOT NULL THEN ''I''
                                                                                  WHEN tgt_ev_sts.ev_id IS NOT NULL
                                                                                  AND             src_md5 <> tgt_md5 THEN ''U''
                                                                                  WHEN tgt_ev_sts.ev_id IS NOT NULL
                                                                                  AND             src_md5 =tgt_md5 THEN ''R''
                                                                  END AS ins_upd_flag
                                                  FROM            (
                                                                                  SELECT DISTINCT xlat_ev_cd.tgt_idntftn_val AS src_ev_act_type_code ,
                                                                                                  key1 ,
                                                                                                  xlat_src_ev_cd.tgt_idntftn_val               AS src_subtype ,
                                                                                                  typecode_riskstatus                          AS src_ev_sts_txt ,
                                                                                                  strt_dt                                      AS src_ev_sts_strt_dttm ,
                                                                                                  coalesce(xlat_ev_sts.tgt_idntftn_val, ''UNK'') AS src_ev_sts_type_cd ,
                                                                                                  branchnumber,
                                                                                                  nk_publicid   AS src_nk_publicid,
                                                                                                  pc_updatetime AS src_trans_strt_dt,
                                                                                                  rnk,
                                                                                                  lkp_ev.ev_id AS ev_id,
                                                                                                  CASE
                                                                                                                  WHEN lkp_quotn.quotn_id IS NULL THEN ''9999''
                                                                                                                  ELSE lkp_quotn.quotn_id
                                                                                                  END AS quotn_id ,
                                                                                                  CASE
                                                                                                                  WHEN lkp_agmt_ppv.agmt_id IS NULL THEN
                                                                                                                                  CASE
                                                                                                                                                  WHEN lkp_agmt_act.agmt_id IS NULL THEN lkp_agmt_inv.agmt_id
                                                                                                                                                  ELSE lkp_agmt_act.agmt_id
                                                                                                                                  END
                                                                                                                  ELSE lkp_agmt_ppv.agmt_id
                                                                                                  END AS agmt_id
                                                                                  FROM            (
                                                                                                                  SELECT DISTINCT ev_act_type_code,
                                                                                                                                  key1,
                                                                                                                                  SUBTYPE,
                                                                                                                                  status,
                                                                                                                                  strt_dt,
                                                                                                                                  typecode_riskstatus,
                                                                                                                                  branchnumber,
                                                                                                                                  nk_publicid,
                                                                                                                                  pc_updatetime,
                                                                                                                                  rank() over(PARTITION BY ev_act_type_code, key1, SUBTYPE ORDER BY pc_updatetime, status ) AS rnk
                                                                                                                  FROM            (
                                                                                                                                         /*************************Claim Check Event*******************/
                                                                                                                                         SELECT ''EV_ACTVY_TYPE24''                                    AS ev_act_type_code,
                                                                                                                                                cast(cc_transaction.id AS VARCHAR(60))               AS key1,
                                                                                                                                                ''EV_SBTYPE2''                                         AS SUBTYPE ,
                                                                                                                                                cast(cctl_transaction.typecode_stg AS VARCHAR (100)) AS status ,
                                                                                                                                                cc_transaction.createtime                            AS strt_dt ,
                                                                                                                                                ''Payment''                                            AS reason ,
                                                                                                                                                cast( '''' AS VARCHAR(60))                             AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS VARCHAR(60))                             AS branchnumber
                                                                                                                                                /* ,cast(cc_transaction.agmt_host_id as varchar (100)) as NK_PublicID */
                                                                                                                                                ,
                                                                                                                                                cast(ltrim(rtrim(cc_transaction.agmt_host_id)) AS VARCHAR (100)) AS nk_publicid
                                                                                                                                                /* --As per EIM-29712 */
                                                                                                                                                ,
                                                                                                                                                cc_transaction.updatetime AS pc_updatetime
                                                                                                                                         FROM   (
                                                                                                                                                                SELECT          a.id_stg AS id,
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN cc_policy.updatetime_stg>a.updatetime_stg THEN cc_policy.updatetime_stg
                                                                                                                                                                                                ELSE a.updatetime_stg
                                                                                                                                                                                END              AS updatetime,
                                                                                                                                                                                a.createtime_stg AS createtime,
                                                                                                                                                                                /* EIM-34393 */
                                                                                                                                                                                cast(
                                                                                                                                                                                CASE
                                                                                                                                                                                                        /* when cc_policy.Verified_stg=0 then '' '' */
                                                                                                                                                                                                WHEN cc_policy.verified_stg=0 THEN cc_policy.id_stg
                                                                                                                                                                                                        /* As per EIM-29712 */
                                                                                                                                                                                                WHEN cc_policy.verified_stg <> 0 THEN pp.publicid_stg
                                                                                                                                                                                END AS VARCHAR(50)) AS agmt_host_id ,
                                                                                                                                                                                cc_policy.verified_stg,
                                                                                                                                                                                a.subtype_stg ,
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                                                                                AND             cc.payload_new_stg=''voided_11'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''voided_15'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''transferred_11''THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''transferred_13'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg ='' transferred''
                                                                                                                                                                                                AND             cc.payload_new_stg=''cleared_13'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg=''recoded_11'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg = ''recoded_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg=''issued_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''cleared_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''requested_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''voided_14'' THEN ''N''
                                                                                                                                                                                                ELSE ''Y''
                                                                                                                                                                                END AS eligible
                                                                                                                                                                FROM            db_t_prod_stag.cc_transaction a
                                                                                                                                                                join            db_t_prod_stag.cctl_transactionstatus
                                                                                                                                                                ON              a .status_stg= cctl_transactionstatus.id_stg
                                                                                                                                                                join
                                                                                                                                                                                (
                                                                                                                                                                                           SELECT     cc_claim.*
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                                                ON              cc_claim.id_stg=a.claimid_stg
                                                                                                                                                                join            db_t_prod_stag.cc_policy
                                                                                                                                                                ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_exposure
                                                                                                                                                                ON              cc_exposure.id_stg=a.exposureid_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_check
                                                                                                                                                                ON              cc_check.id_stg = a.checkid_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_user
                                                                                                                                                                ON              a.createuserid_stg = cc_user.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_contact
                                                                                                                                                                ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                left join       db_t_prod_stag.gl_eventstaging_cc cc
                                                                                                                                                                ON              cc.publicid_stg=a.publicid_stg
                                                                                                                                                                left join       db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                ON              pp.id_stg=cc_policy.policysystemperiodid_stg
                                                                                                                                                                WHERE           ((
                                                                                                                                                                                                        a.updatetime_stg >($start_dttm)
                                                                                                                                                                                                AND             a.updatetime_stg <= ($end_dttm))
                                                                                                                                                                                OR              (
                                                                                                                                                                                                        cc_check.updatetime_stg >($start_dttm)
                                                                                                                                                                                                AND             cc_check.updatetime_stg <= ($end_dttm))
                                                                                                                                                                                OR              (
                                                                                                                                                                                                        cc_policy.updatetime_stg >($start_dttm)
                                                                                                                                                                                                AND             cc_policy.updatetime_stg <= ($end_dttm))
                                                                                                                                                                                                /* EIM-34393 */
                                                                                                                                                                                ) ) cc_transaction
                                                                                                                                         join   db_t_prod_stag.cctl_transaction
                                                                                                                                         ON     cctl_transaction.id_stg=cc_transaction.subtype_stg
                                                                                                                                         WHERE  cctl_transaction.typecode_stg=''Payment''
                                                                                                                                         AND    eligible=''Y''
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE23''                                    AS ev_act_type_code,
                                                                                                                                                cast(cc_transaction.id AS VARCHAR(60))               AS key1,
                                                                                                                                                ''EV_SBTYPE2''                                         AS SUBTYPE ,
                                                                                                                                                cast(cctl_transaction.typecode_stg AS VARCHAR (100)) AS status ,
                                                                                                                                                cc_transaction.createtime                            AS strt_dt ,
                                                                                                                                                ''Recovery''                                           AS reason ,
                                                                                                                                                cast( '''' AS VARCHAR(60))                             AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS VARCHAR(60))                             AS branchnumber
                                                                                                                                                /* ,cast(cc_transaction.agmt_host_id as varchar (100)) as NK_PublicID */
                                                                                                                                                ,
                                                                                                                                                cast(ltrim(rtrim(cc_transaction.agmt_host_id)) AS VARCHAR (100)) AS nk_publicid
                                                                                                                                                /* --As per EIM-29712 */
                                                                                                                                                ,
                                                                                                                                                cc_transaction.updatetime AS pc_updatetime
                                                                                                                                         FROM   (
                                                                                                                                                                SELECT          a.id_stg         AS id,
                                                                                                                                                                                a.updatetime_stg AS updatetime,
                                                                                                                                                                                a.createtime_stg AS createtime,
                                                                                                                                                                                cast(
                                                                                                                                                                                CASE
                                                                                                                                                                                                        /* when cc_policy.Verified_stg=0 then '' '' */
                                                                                                                                                                                                WHEN cc_policy.verified_stg=0 THEN cc_policy.id_stg
                                                                                                                                                                                                        /* As per EIM-29712 */
                                                                                                                                                                                                WHEN cc_policy.verified_stg <> 0 THEN pp.publicid_stg
                                                                                                                                                                                END AS VARCHAR(100)) AS agmt_host_id ,
                                                                                                                                                                                cc_policy.verified_stg,
                                                                                                                                                                                a.subtype_stg ,
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                                                                                AND             cc.payload_new_stg=''voided_11'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''voided_15'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''transferred_11''THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''transferred_13'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg ='' transferred''
                                                                                                                                                                                                AND             cc.payload_new_stg=''cleared_13'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg=''recoded_11'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg = ''recoded_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg=''issued_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''cleared_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''requested_14'' THEN ''N''
                                                                                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                                                                                AND             cc.payload_new_stg= ''voided_14'' THEN ''N''
                                                                                                                                                                                                ELSE ''Y''
                                                                                                                                                                                END AS eligible
                                                                                                                                                                FROM            db_t_prod_stag.cc_transaction a
                                                                                                                                                                join            db_t_prod_stag.cctl_transactionstatus
                                                                                                                                                                ON              a .status_stg= cctl_transactionstatus.id_stg
                                                                                                                                                                join
                                                                                                                                                                                (
                                                                                                                                                                                           SELECT     cc_claim.*
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                                                ON              cc_claim.id_stg=a.claimid_stg
                                                                                                                                                                join            db_t_prod_stag.cc_policy
                                                                                                                                                                ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_exposure
                                                                                                                                                                ON              cc_exposure.id_stg=a.exposureid_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_check
                                                                                                                                                                ON              cc_check.id_stg = a.checkid_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_user
                                                                                                                                                                ON              a.createuserid_stg = cc_user.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_contact
                                                                                                                                                                ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                left join       db_t_prod_stag.gl_eventstaging_cc cc
                                                                                                                                                                ON              cc.publicid_stg=a.publicid_stg
                                                                                                                                                                left join       db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                ON              pp.id_stg=cc_policy.policysystemperiodid_stg
                                                                                                                                                                WHERE           ((
                                                                                                                                                                                                        a.updatetime_stg >($start_dttm)
                                                                                                                                                                                                AND             a.updatetime_stg <= ($end_dttm))
                                                                                                                                                                                OR              (
                                                                                                                                                                                                        cc_check.updatetime_stg >($start_dttm)
                                                                                                                                                                                                AND             cc_check.updatetime_stg <= ($end_dttm)))
                                                                                                                                                                AND             checknum_alfa_stg IS NOT NULL ) cc_transaction
                                                                                                                                         join   db_t_prod_stag.cctl_transaction
                                                                                                                                         ON     cctl_transaction.id_stg=cc_transaction.subtype_stg
                                                                                                                                         WHERE  cctl_transaction.typecode_stg=''Recovery''
                                                                                                                                         AND    eligible=''Y''
                                                                                                                                         UNION
                                                                                                                                         /****************************************Payment Request****************************/
                                                                                                                                         SELECT          cast(''EV_ACTVY_TYPE35'' AS               VARCHAR(50))   AS ev_act_type_code ,
                                                                                                                                                         cast( bc_invoicestream.id AS            VARCHAR(60))   AS key1 ,
                                                                                                                                                         cast(''EV_SBTYPE2'' AS                    VARCHAR(50))   AS SUBTYPE ,
                                                                                                                                                         cast(bctl_invoicestatus.typecode_stg AS VARCHAR (100)) AS status ,
                                                                                                                                                         bc_invoicestream.createtime                            AS strt_dt ,
                                                                                                                                                         ''PaymentRequest''                                       AS reason ,
                                                                                                                                                         cast( '''' AS VARCHAR(60))                               AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS VARCHAR(60))                               AS branchnumber ,
                                                                                                                                                         billingreferencenumber_alfa                            AS nk_publicid ,
                                                                                                                                                         bc_invoicestream.updatetime
                                                                                                                                         FROM            (
                                                                                                                                                                SELECT bc_invoicestream.createtime_stg                  AS createtime,
                                                                                                                                                                       bc_invoicestream.updatetime_stg                  AS updatetime ,
                                                                                                                                                                       bc_invoicestream.id_stg                          AS id,
                                                                                                                                                                       bc_invoicestream.overridingpayer_alfa_stg        AS overridingpayer_alfa,
                                                                                                                                                                       bc_invoicestream.billingreferencenumber_alfa_stg AS billingreferencenumber_alfa
                                                                                                                                                                FROM   db_t_prod_stag.bc_invoicestream
                                                                                                                                                                WHERE  bc_invoicestream.updatetime_stg > ($start_dttm)
                                                                                                                                                                AND    bc_invoicestream.updatetime_stg <= ($end_dttm) ) bc_invoicestream
                                                                                                                                         left outer join
                                                                                                                                                         (
                                                                                                                                                                SELECT bc_invoice.invoicestreamid_stg AS invoicestreamid,
                                                                                                                                                                       bc_invoice.status_stg          AS status
                                                                                                                                                                FROM   db_t_prod_stag.bc_invoice
                                                                                                                                                                WHERE  bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                                                                AND    bc_invoice.updatetime_stg <= ($end_dttm) ) bc_invoice
                                                                                                                                         ON              bc_invoice.invoicestreamid=bc_invoicestream.id
                                                                                                                                         left outer join db_t_prod_stag.bctl_invoicestatus
                                                                                                                                         ON              bc_invoice.status=bctl_invoicestatus.id_stg
                                                                                                                                         WHERE           overridingpayer_alfa IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         SELECT    cast(''EV_ACTVY_TYPE35'' AS         VARCHAR(50)) AS ev_act_type_code ,
                                                                                                                                                   cast( bc_paymentrequest.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                                                                   cast(''EV_SBTYPE2'' AS              VARCHAR(50)) AS SUBTYPE
                                                                                                                                                   /* ,cast(bctl_paymentrequeststatus.typecode as varchar (100)) as status */
                                                                                                                                                   ,
                                                                                                                                                   bctl_paymentrequeststatus.typecode_stg AS status ,
                                                                                                                                                   bc_paymentrequest.statusdate_stg       AS strt_dt
                                                                                                                                                   /* ,bc_paymentrequest.createtime as strt_dt */
                                                                                                                                                   ,
                                                                                                                                                   ''PaymentRequest''                                                         AS reason ,
                                                                                                                                                   cast( '''' AS                                               VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                   cast( '''' AS                                               VARCHAR(60))   AS branchnumber ,
                                                                                                                                                   cast(bc_paymentrequest.billingreferencenumber_alfa_stg AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                   bc_paymentrequest.updatetime_stg
                                                                                                                                         FROM      db_t_prod_stag.bc_paymentrequest
                                                                                                                                         join      db_t_prod_stag.bctl_paymentrequeststatus
                                                                                                                                         ON        bc_paymentrequest.status_stg = bctl_paymentrequeststatus.id_stg
                                                                                                                                         left join db_t_prod_stag.bc_paymentinstrument
                                                                                                                                         ON        bc_paymentrequest.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                                                                         left join db_t_prod_stag.bctl_paymentmethod
                                                                                                                                         ON        bc_paymentinstrument.paymentmethod_stg = bctl_paymentmethod.id_stg
                                                                                                                                         left join db_t_prod_stag.bc_invoice
                                                                                                                                         ON        bc_paymentrequest.invoiceid_stg=bc_invoice.id_stg
                                                                                                                                         WHERE     bc_paymentrequest.updatetime_stg > ($start_dttm)
                                                                                                                                         AND       bc_paymentrequest.updatetime_stg <= ($end_dttm)
                                                                                                                                         /* join DB_T_PROD_STAG.bctl_paymentrequeststatus on bctl_paymentrequeststatus.ID=bc_paymentrequest.Status */
                                                                                                                                         UNION
                                                                                                                                         /****************************************bc_basemoneyreceived****************************/
                                                                                                                                         SELECT          cast(''EV_ACTVY_TYPE14'' AS            VARCHAR(50)) AS ev_act_type_code ,
                                                                                                                                                         cast( bc_basemoneyreceived.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                                                                         cast(''EV_SBTYPE2'' AS                 VARCHAR(50)) AS SUBTYPE ,
                                                                                                                                                         ''EV_STS_TYPE2''                                    AS status ,
                                                                                                                                                         bc_basemoneyreceived.receiveddate_stg
                                                                                                                                                         /* ,bc_basemoneyreceived.createtime as strt_dt */
                                                                                                                                                         ,
                                                                                                                                                         ''Billing''                                                                   AS reason ,
                                                                                                                                                         cast( '''' AS                                                  VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS                                                  VARCHAR(60))   AS branchnumber ,
                                                                                                                                                         cast(bc_basemoneyreceived.billingreferencenumber_alfa_stg AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                         bc_basemoneyreceived.updatetime_stg
                                                                                                                                         FROM            (
                                                                                                                                                                         SELECT          bc_basemoneyreceived.receiveddate_stg,
                                                                                                                                                                                         bc_basemoneyreceived.reversalreason_stg,
                                                                                                                                                                                         bc_basemoneyreceived.updatetime_stg,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                        WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                                                        ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                                         END billingreferencenumber_alfa_stg,
                                                                                                                                                                                         bc_basemoneyreceived.subtype_stg,
                                                                                                                                                                                         bc_basemoneyreceived.id_stg
                                                                                                                                                                         FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                                                         left outer join db_t_prod_stag.bc_unappliedfund
                                                                                                                                                                         ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_user
                                                                                                                                                                         ON              bc_user.id_stg=bc_unappliedfund.createuserid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_contact
                                                                                                                                                                         ON              bc_contact.id_stg=bc_user.contactid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_invoicestream
                                                                                                                                                                         ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                                                                                                         ON              bc_basemoneyreceived.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                                                                                                         ON              bctl_paymentmethod.id_stg=bc_paymentinstrument.paymentmethod_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_dbmoneyrcvdcontext
                                                                                                                                                                         ON              bc_basemoneyreceived.id_stg = bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_transaction
                                                                                                                                                                         ON              bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bctl_transaction
                                                                                                                                                                         ON              bc_transaction.subtype_stg=bctl_transaction.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bctl_paymentsource_alfa
                                                                                                                                                                         ON              bc_basemoneyreceived.paymentsource_alfa_stg=bctl_paymentsource_alfa.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_account
                                                                                                                                                                         ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_revtrans a
                                                                                                                                                                         ON              a.ownerid_stg = bc_transaction.id_stg
                                                                                                                                                                                         /* ownerid is the trans that is reversing another trans(revrse)(4,2017) */
                                                                                                                                                                         left outer join db_t_prod_stag.bc_revtrans b
                                                                                                                                                                         ON              b.foreignentityid_stg = bc_transaction.id_stg
                                                                                                                                                                                         /* recieve payment (3,2017) */
                                                                                                                                                                         WHERE           bc_basemoneyreceived.updatetime_stg > ($start_dttm)
                                                                                                                                                                         AND             bc_basemoneyreceived.updatetime_stg <= ($end_dttm) ) bc_basemoneyreceived
                                                                                                                                         inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                                                                         ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                                                                         left outer join db_t_prod_stag.bctl_paymentreversalreason
                                                                                                                                         ON              bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.reversalreason_stg
                                                                                                                                         WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                                                                                 ''DirectBillMoneyRcvd'',
                                                                                                                                                                                                 ''ZeroDollarDMR'',
                                                                                                                                                                                                 ''ZeroDollarReversal'')
                                                                                                                                         UNION
                                                                                                                                         SELECT          ''EV_ACTVY_TYPE25''                                 AS ev_act_type_code ,
                                                                                                                                                         cast( bc_basemoneyreceived.id_stg AS VARCHAR(60)) AS key1 ,
                                                                                                                                                         ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                                                                                                         ''EV_STS_TYPE1''                                    AS status ,
                                                                                                                                                         bc_basemoneyreceived.reversaldate_stg
                                                                                                                                                         /* ,bc_basemoneyreceived.createtime as strt_dt */
                                                                                                                                                         ,
                                                                                                                                                         bctl_paymentreversalreason.typecode_stg AS reason ,
                                                                                                                                                         cast( '''' AS                                                  VARCHAR(60))                AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS                                                  VARCHAR(60))                AS branchnumber ,
                                                                                                                                                         cast(bc_basemoneyreceived.billingreferencenumber_alfa_stg AS VARCHAR (100))              AS nk_publicid ,
                                                                                                                                                         bc_basemoneyreceived.updatetime_stg
                                                                                                                                         FROM            (
                                                                                                                                                                         SELECT          bc_basemoneyreceived.receiveddate_stg,
                                                                                                                                                                                         bc_basemoneyreceived.reversalreason_stg,
                                                                                                                                                                                         bc_basemoneyreceived.updatetime_stg,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                        WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                                                        ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                                         END billingreferencenumber_alfa_stg,
                                                                                                                                                                                         bc_basemoneyreceived.subtype_stg,
                                                                                                                                                                                         bc_basemoneyreceived.id_stg ,
                                                                                                                                                                                         bc_basemoneyreceived.reversaldate_stg
                                                                                                                                                                         FROM            db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                                                         left outer join db_t_prod_stag.bc_unappliedfund
                                                                                                                                                                         ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_user
                                                                                                                                                                         ON              bc_user.id_stg=bc_unappliedfund.createuserid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_contact
                                                                                                                                                                         ON              bc_contact.id_stg=bc_user.contactid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_invoicestream
                                                                                                                                                                         ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                                                                                                         ON              bc_basemoneyreceived.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                                                                                                         ON              bctl_paymentmethod.id_stg=bc_paymentinstrument.paymentmethod_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_dbmoneyrcvdcontext
                                                                                                                                                                         ON              bc_basemoneyreceived.id_stg = bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_transaction
                                                                                                                                                                         ON              bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bctl_transaction
                                                                                                                                                                         ON              bc_transaction.subtype_stg=bctl_transaction.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bctl_paymentsource_alfa
                                                                                                                                                                         ON              bc_basemoneyreceived.paymentsource_alfa_stg=bctl_paymentsource_alfa.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_account
                                                                                                                                                                         ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_revtrans a
                                                                                                                                                                         ON              a.ownerid_stg = bc_transaction.id_stg
                                                                                                                                                                                         /* ownerid is the trans that is reversing another trans(revrse)(4,2017) */
                                                                                                                                                                         left outer join db_t_prod_stag.bc_revtrans b
                                                                                                                                                                         ON              b.foreignentityid_stg = bc_transaction.id_stg
                                                                                                                                                                                         /* recieve payment (3,2017) */
                                                                                                                                                                         WHERE           bc_basemoneyreceived.updatetime_stg > ($start_dttm)
                                                                                                                                                                         AND             bc_basemoneyreceived.updatetime_stg <= ($end_dttm) ) bc_basemoneyreceived
                                                                                                                                         inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                                                                                                         ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                                                                                                         left outer join db_t_prod_stag.bctl_paymentreversalreason
                                                                                                                                         ON              bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.reversalreason_stg
                                                                                                                                         WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                                                                                                 ''DirectBillMoneyRcvd'',
                                                                                                                                                                                                 ''ZeroDollarDMR'',
                                                                                                                                                                                                 ''ZeroDollarReversal'')
                                                                                                                                         AND             bc_basemoneyreceived.reversaldate_stg IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         /***********************************Outgoing Payment Status********************************************/
                                                                                                                                         SELECT     ''EV_ACTVY_TYPE31''                               AS ev_act_type_code ,
                                                                                                                                                    cast( bc_outgoingpayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                    ''EV_SBTYPE2''                                    AS subtype_stg ,
                                                                                                                                                    bctl_outgoingpaymentstatus.typecode_stg         AS status,
                                                                                                                                                    CASE
                                                                                                                                                               WHEN bctl_outgoingpaymentstatus.typecode_stg= ''issued''
                                                                                                                                                               AND        issuedate_stg IS NOT NULL THEN issuedate_stg
                                                                                                                                                               WHEN bctl_outgoingpaymentstatus.typecode_stg= ''cleared''
                                                                                                                                                               AND        cleareddate_alfa_stg IS NOT NULL THEN cleareddate_alfa_stg
                                                                                                                                                               ELSE bc_outgoingpayment.createtime_stg
                                                                                                                                                    END                                                                       AS strt_dt ,
                                                                                                                                                    cast('''' AS                                                 VARCHAR(60))   AS reason ,
                                                                                                                                                    cast( '''' AS                                                VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                    cast( '''' AS                                                VARCHAR(60))   AS branchnumber ,
                                                                                                                                                    cast(bc_outgoingpayment.billingreferencenumber_alfa_stg AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                    bc_outgoingpayment.updatetime_stg
                                                                                                                                         FROM       (
                                                                                                                                                                    SELECT          a.issuedate_stg ,
                                                                                                                                                                                    a.createtime_stg ,
                                                                                                                                                                                    a.updatetime_stg ,
                                                                                                                                                                                    a.id_stg ,
                                                                                                                                                                                    a.rejecteddate_stg ,
                                                                                                                                                                                    a.status_stg ,
                                                                                                                                                                                    CASE
                                                                                                                                                                                                    WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN acc.accountnumber_stg
                                                                                                                                                                                                    ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                                    END AS billingreferencenumber_alfa_stg,
                                                                                                                                                                                    a.cleareddate_alfa_stg
                                                                                                                                                                    FROM            (
                                                                                                                                                                                                    SELECT          bc_outgoingpayment.*,
                                                                                                                                                                                                        bc_paymentinstrument.paymentmethod_stg AS paymentmethod_stg,
                                                                                                                                                                                                        bctl_paymentmethod.typecode_stg        AS fund_trnsfr_mthd_typ_stg
                                                                                                                                                                                                    FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                                                                                                                                    left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                                                                                                                                    ON              bc_outgoingpayment.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                                                                                                                                    left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                                                                                                                                    ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg) a ,
                                                                                                                                                                                    (
                                                                                                                                                                                                    SELECT          bc_outgoingpayment.*,
                                                                                                                                                                                                        bc_disbursement.status_stg AS bcdisbursementstatus_stg
                                                                                                                                                                                                    FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                                                                                                                                    left outer join db_t_prod_stag.bc_disbursement
                                                                                                                                                                                                    ON              bc_outgoingpayment.disbursementid_stg = bc_disbursement.id_stg) b
                                                                                                                                                                    left join       db_t_prod_stag.bc_disbursement
                                                                                                                                                                    ON              bc_disbursement.id_stg=b.disbursementid_stg
                                                                                                                                                                    left join       db_t_prod_stag.bc_unappliedfund
                                                                                                                                                                    ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                                                                                                                                    left join       db_t_prod_stag.bc_invoicestream
                                                                                                                                                                    ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                                    left join       db_t_prod_stag.bc_account
                                                                                                                                                                    ON              (
                                                                                                                                                                                                    bc_account.id_stg = bc_invoicestream. policyid_stg
                                                                                                                                                                                    AND             bc_disbursement.accountid_stg = bc_account.id_stg)
                                                                                                                                                                                    /* left Join DB_T_PROD_STAG.bc_Account on bc_disbursement.AccountID = bc_account.id */
                                                                                                                                                                    left join       db_t_prod_stag.bc_accountcontact
                                                                                                                                                                    ON              bc_account.id_stg = bc_accountcontact.accountid_stg
                                                                                                                                                                    left join       db_t_prod_stag.bc_contact
                                                                                                                                                                    ON              bc_accountcontact.contactid_stg = bc_contact.id_stg
                                                                                                                                                                    left join       db_t_prod_stag.bc_account acc
                                                                                                                                                                    ON              bc_disbursement.accountid_stg = acc.id_stg
                                                                                                                                                                    left outer join db_t_prod_stag.bctl_contact
                                                                                                                                                                    ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                    WHERE           a.id_stg=b.id_stg
                                                                                                                                                                    AND             a.updatetime_stg > ($start_dttm)
                                                                                                                                                                    AND             a.updatetime_stg <= ($end_dttm)) bc_outgoingpayment
                                                                                                                                         inner join db_t_prod_stag.bctl_outgoingpaymentstatus
                                                                                                                                         ON         bctl_outgoingpaymentstatus.id_stg=bc_outgoingpayment.status_stg
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE31''                              AS ev_act_type_code ,
                                                                                                                                                cast(bc_outgoingpayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE7''                                 AS status ,
                                                                                                                                                CASE
                                                                                                                                                       WHEN bc_outgoingpayment.rejecteddate_stg IS NOT NULL THEN rejecteddate_stg
                                                                                                                                                       ELSE current_date-1
                                                                                                                                                END AS strt_dt
                                                                                                                                                /* , bc_outgoingpayment.RejectedDate AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS                                                 VARCHAR(60))   AS reason ,
                                                                                                                                                cast( '''' AS                                                VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS                                                VARCHAR(60))   AS branchnumber ,
                                                                                                                                                cast(bc_outgoingpayment.billingreferencenumber_alfa_stg AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                bc_outgoingpayment.updatetime_stg
                                                                                                                                         FROM   (
                                                                                                                                                                SELECT          a.issuedate_stg ,
                                                                                                                                                                                a.createtime_stg ,
                                                                                                                                                                                a.updatetime_stg ,
                                                                                                                                                                                a.id_stg ,
                                                                                                                                                                                a.rejecteddate_stg ,
                                                                                                                                                                                a.status_stg ,
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN acc.accountnumber_stg
                                                                                                                                                                                                ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                                END AS billingreferencenumber_alfa_stg,
                                                                                                                                                                                a.cleareddate_alfa_stg
                                                                                                                                                                FROM            (
                                                                                                                                                                                                SELECT          bc_outgoingpayment.*,
                                                                                                                                                                                                        bc_paymentinstrument.paymentmethod_stg AS paymentmethod_stg,
                                                                                                                                                                                                        bctl_paymentmethod.typecode_stg        AS fund_trnsfr_mthd_typ_stg
                                                                                                                                                                                                FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                                                                                                                                left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                                                                                                                                ON              bc_outgoingpayment.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                                                                                                                                left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                                                                                                                                ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg) a ,
                                                                                                                                                                                (
                                                                                                                                                                                                SELECT          bc_outgoingpayment.*,
                                                                                                                                                                                                        bc_disbursement.status_stg AS bcdisbursementstatus_stg
                                                                                                                                                                                                FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                                                                                                                                left outer join db_t_prod_stag.bc_disbursement
                                                                                                                                                                                                ON              bc_outgoingpayment.disbursementid_stg = bc_disbursement.id_stg) b
                                                                                                                                                                left join       db_t_prod_stag.bc_disbursement
                                                                                                                                                                ON              bc_disbursement.id_stg=b.disbursementid_stg
                                                                                                                                                                left join       db_t_prod_stag.bc_unappliedfund
                                                                                                                                                                ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                                                                                                                                left join       db_t_prod_stag.bc_invoicestream
                                                                                                                                                                ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                                left join       db_t_prod_stag.bc_account
                                                                                                                                                                ON              (
                                                                                                                                                                                                bc_account.id_stg = bc_invoicestream. policyid_stg
                                                                                                                                                                                AND             bc_disbursement.accountid_stg = bc_account.id_stg)
                                                                                                                                                                                /* left Join DB_T_PROD_STAG.bc_Account on bc_disbursement.AccountID = bc_account.id */
                                                                                                                                                                left join       db_t_prod_stag.bc_accountcontact
                                                                                                                                                                ON              bc_account.id_stg = bc_accountcontact.accountid_stg
                                                                                                                                                                left join       db_t_prod_stag.bc_contact
                                                                                                                                                                ON              bc_accountcontact.contactid_stg = bc_contact.id_stg
                                                                                                                                                                left join       db_t_prod_stag.bc_account acc
                                                                                                                                                                ON              bc_disbursement.accountid_stg = acc.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.bctl_contact
                                                                                                                                                                ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                WHERE           a.id_stg=b.id_stg
                                                                                                                                                                AND             a.updatetime_stg > ($start_dttm)
                                                                                                                                                                AND             a.updatetime_stg <= ($end_dttm) ) bc_outgoingpayment
                                                                                                                                         WHERE  bc_outgoingpayment.rejecteddate_stg IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         /***************************bc_suspensepayment****************************/
                                                                                                                                         SELECT          ''EV_ACTVY_TYPE30''                               AS ev_act_type_code ,
                                                                                                                                                         cast( bc_suspensepayment.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                         ''EV_SBTYPE2''                                    AS SUBTYPE ,
                                                                                                                                                         bctl_suspensepaymentstatus.typecode_stg         AS status ,
                                                                                                                                                         bc_suspensepayment.createtime_stg               AS strt_dt ,
                                                                                                                                                         cast('''' AS                                VARCHAR(60))                         AS reason ,
                                                                                                                                                         cast( '''' AS                               VARCHAR(60))                         AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS                               VARCHAR(60))                         AS branchnumber ,
                                                                                                                                                         cast(bc_suspensepayment.agmthostid_stg AS VARCHAR (100))                       AS nk_publicid ,
                                                                                                                                                         bc_suspensepayment.updatetime_stg
                                                                                                                                         FROM            (
                                                                                                                                                                         SELECT          bc_suspensepayment.createtime_stg,
                                                                                                                                                                                         bc_suspensepayment.updatetime_stg,
                                                                                                                                                                                         bc_suspensepayment.id_stg,
                                                                                                                                                                                         bc_suspensepayment.status_stg,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                        WHEN bc_suspensepayment.policynumber_stg IS NOT NULL THEN bc_suspensepayment.policynumber_stg
                                                                                                                                                                                                        WHEN bc_suspensepayment.billingreferencenumber_alfa_stg IS NOT NULL THEN bc_suspensepayment.billingreferencenumber_alfa_stg
                                                                                                                                                                                                        WHEN bc_suspensepayment.accountnumber_stg IS NOT NULL THEN bc_suspensepayment.accountnumber_stg
                                                                                                                                                                                                        ELSE NULL
                                                                                                                                                                                         END AS agmthostid_stg
                                                                                                                                                                         FROM            db_t_prod_stag.bc_suspensepayment
                                                                                                                                                                         left outer join db_t_prod_stag.bc_policyperiod
                                                                                                                                                                         ON              bc_suspensepayment.policyperiodappliedtoid_stg=bc_policyperiod.id_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_account
                                                                                                                                                                         ON              bc_account.id_stg=bc_suspensepayment.accountappliedtoid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_user
                                                                                                                                                                         ON              bc_user.id_stg=bc_suspensepayment.createuserid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_contact
                                                                                                                                                                         ON              bc_contact.id_stg=bc_user.contactid_stg
                                                                                                                                                                         WHERE           bc_suspensepayment.updatetime_stg > ($start_dttm)
                                                                                                                                                                         AND             bc_suspensepayment.updatetime_stg <= ($end_dttm) ) bc_suspensepayment
                                                                                                                                         left outer join db_t_prod_stag.bctl_suspensepaymentstatus
                                                                                                                                         ON              bctl_suspensepaymentstatus.id_stg=bc_suspensepayment.status_stg
                                                                                                                                         UNION
                                                                                                                                         /***********************************disbursement status**************************************************/
                                                                                                                                         SELECT          ''EV_ACTVY_TYPE32''                           AS ev_act_type_code,
                                                                                                                                                         cast(bc_disbursement.id_stg AS VARCHAR(50)) AS key1 ,
                                                                                                                                                         ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                                                                                                         bctl_disbursementstatus.typecode_stg        AS status ,
                                                                                                                                                         bc_disbursement.updatetime_stg              AS strt_dt
                                                                                                                                                         /* ,bc_disbursement.UpdateTime AS strt_dt */
                                                                                                                                                         ,
                                                                                                                                                         cast('''' AS                                              VARCHAR(60))   AS reason ,
                                                                                                                                                         cast( '''' AS                                             VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS                                             VARCHAR(60))   AS branchnumber ,
                                                                                                                                                         cast(bc_disbursement.billingreferencenumber_alfa_stg AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                         bc_disbursement.updatetime_stg
                                                                                                                                         FROM            (
                                                                                                                                                                         SELECT          bc_disbursement.updatetime_stg,
                                                                                                                                                                                         bc_disbursement.id_stg,
                                                                                                                                                                                         bc_disbursement.status_stg,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                        WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                                                        ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                                         END AS billingreferencenumber_alfa_stg
                                                                                                                                                                         FROM            db_t_prod_stag.bc_disbursement
                                                                                                                                                                         left outer join db_t_prod_stag.bc_user
                                                                                                                                                                         ON              bc_user.id_stg=bc_disbursement.createuserid_stg
                                                                                                                                                                         left outer join db_t_prod_stag.bc_contact
                                                                                                                                                                         ON              bc_contact.id_stg=bc_user.contactid_stg
                                                                                                                                                                         left join       db_t_prod_stag.bc_unappliedfund
                                                                                                                                                                         ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                                                                                                                                         left join       db_t_prod_stag.bc_invoicestream
                                                                                                                                                                         ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                                         left join       db_t_prod_stag.bc_account
                                                                                                                                                                         ON              bc_disbursement.accountid_stg = bc_account.id_stg
                                                                                                                                                                         WHERE           bc_disbursement.updatetime_stg > ($start_dttm)
                                                                                                                                                                         AND             bc_disbursement.updatetime_stg <= ($end_dttm) ) bc_disbursement
                                                                                                                                         left outer join db_t_prod_stag.bctl_disbursementstatus
                                                                                                                                         ON              bctl_disbursementstatus.id_stg=bc_disbursement.status_stg
                                                                                                                                         UNION
                                                                                                                                         SELECT          ''EV_ACTVY_TYPE33''                    AS ev_act_type_code ,
                                                                                                                                                         cast( bc_writeoff.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                         ''EV_SBTYPE2''                         AS SUBTYPE ,
                                                                                                                                                         bctl_approvalstatus.typecode_stg     AS status ,
                                                                                                                                                         bc_writeoff.approvaldate             AS strt_dt
                                                                                                                                                         /* , bc_writeoff.createtime AS strt_dt */
                                                                                                                                                         ,
                                                                                                                                                         cast('''' AS                     VARCHAR(60))   AS reason ,
                                                                                                                                                         cast( '''' AS                    VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS                    VARCHAR(60))   AS branchnumber ,
                                                                                                                                                         cast(bc_writeoff.agmthostid AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                         bc_writeoff.updatetime
                                                                                                                                         FROM            (
                                                                                                                                                                   SELECT    bc_writeoff.id_stg           AS id,
                                                                                                                                                                             bc_writeoff.approvaldate_stg AS approvaldate,
                                                                                                                                                                             CASE
                                                                                                                                                                                       WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                                       ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                             END                            AS agmthostid,
                                                                                                                                                                             bc_writeoff.updatetime_stg     AS updatetime,
                                                                                                                                                                             bc_writeoff.approvalstatus_stg AS approvalstatus,
                                                                                                                                                                             ''WRITEOFF''                     AS writeoffflag
                                                                                                                                                                   FROM      db_t_prod_stag.bc_writeoff
                                                                                                                                                                   left join db_t_prod_stag.bc_invoiceitem
                                                                                                                                                                   ON        bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_invoice
                                                                                                                                                                   ON        bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_invoicestream
                                                                                                                                                                   ON        bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_taccountcontainer
                                                                                                                                                                   ON        bc_taccountcontainer.id_stg=bc_writeoff.taccountcontainerid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_policyperiod a
                                                                                                                                                                   ON        a.id_stg=bc_invoiceitem.policyperiodid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_policyperiod b
                                                                                                                                                                   ON        b.hiddentaccountcontainerid_stg=bc_taccountcontainer.id_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_policy
                                                                                                                                                                   ON        bc_policy.id_stg=b.policyid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_account
                                                                                                                                                                   ON        bc_account.id_stg=bc_policy.accountid_stg
                                                                                                                                                                   WHERE     bc_writeoff.id_stg NOT IN
                                                                                                                                                                             (
                                                                                                                                                                                    SELECT ownerid_stg
                                                                                                                                                                                    FROM   db_t_prod_stag.bc_revwriteoff)
                                                                                                                                                                   AND       bc_writeoff.updatetime_stg > ($start_dttm)
                                                                                                                                                                   AND       bc_writeoff.updatetime_stg <= ($end_dttm) ) bc_writeoff
                                                                                                                                         left outer join db_t_prod_stag.bctl_approvalstatus
                                                                                                                                         ON              bctl_approvalstatus.id_stg=bc_writeoff.approvalstatus
                                                                                                                                         WHERE           bc_writeoff.approvaldate IS NOT NULL
                                                                                                                                         AND             writeoffflag=''WRITEOFF''
                                                                                                                                         UNION
                                                                                                                                         /**********************************write off reversal status******************************************************/
                                                                                                                                         SELECT          ''EV_ACTVY_TYPE34''                   AS ev_act_type_code ,
                                                                                                                                                         cast(bc_writeoff.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                         ''EV_SBTYPE2''                        AS SUBTYPE ,
                                                                                                                                                         bctl_approvalstatus.typecode_stg    AS status ,
                                                                                                                                                         approvaldate                        AS strt_dt
                                                                                                                                                         /* -, bc_writeoffreversal.createtime AS strt_dt */
                                                                                                                                                         ,
                                                                                                                                                         cast('''' AS         VARCHAR(60))   AS reason ,
                                                                                                                                                         cast( '''' AS        VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                         cast( '''' AS        VARCHAR(60))   AS branchnumber ,
                                                                                                                                                         cast(agmthostid AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                         updatetime
                                                                                                                                         FROM            (
                                                                                                                                                                   SELECT    bc_writeoff.id_stg           AS id,
                                                                                                                                                                             bc_writeoff.approvaldate_stg AS approvaldate,
                                                                                                                                                                             CASE
                                                                                                                                                                                       WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                                       ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                             END                            AS agmthostid,
                                                                                                                                                                             bc_writeoff.updatetime_stg     AS updatetime,
                                                                                                                                                                             bc_writeoff.approvalstatus_stg AS approvalstatus,
                                                                                                                                                                             ''REVWRITEOFF''                  AS writeoffflag
                                                                                                                                                                   FROM      db_t_prod_stag.bc_writeoff
                                                                                                                                                                   left join db_t_prod_stag.bc_invoiceitem
                                                                                                                                                                   ON        bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_invoice
                                                                                                                                                                   ON        bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_invoicestream
                                                                                                                                                                   ON        bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_taccountcontainer
                                                                                                                                                                   ON        bc_taccountcontainer.id_stg=bc_writeoff.taccountcontainerid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_policyperiod a
                                                                                                                                                                   ON        a.id_stg=bc_invoiceitem.policyperiodid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_policyperiod b
                                                                                                                                                                   ON        b.hiddentaccountcontainerid_stg=bc_taccountcontainer.id_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_policy
                                                                                                                                                                   ON        bc_policy.id_stg=b.policyid_stg
                                                                                                                                                                   left join db_t_prod_stag.bc_account
                                                                                                                                                                   ON        bc_account.id_stg=bc_policy.accountid_stg
                                                                                                                                                                   WHERE     bc_writeoff.id_stg IN
                                                                                                                                                                             (
                                                                                                                                                                                    SELECT ownerid_stg
                                                                                                                                                                                    FROM   db_t_prod_stag.bc_revwriteoff)
                                                                                                                                                                   AND       bc_writeoff.updatetime_stg > ($start_dttm)
                                                                                                                                                                   AND       bc_writeoff.updatetime_stg <= ($end_dttm) ) bc_writeoff
                                                                                                                                         left outer join db_t_prod_stag.bctl_approvalstatus
                                                                                                                                         ON              bctl_approvalstatus.id_stg=bc_writeoff.approvalstatus
                                                                                                                                         WHERE           approvaldate IS NOT NULL
                                                                                                                                         AND             writeoffflag=''REVWRITEOFF''
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE34''                   AS ev_act_type_code ,
                                                                                                                                                cast(bc_writeoff.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                        AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE1''                      AS status ,
                                                                                                                                                createtime                          AS strt_dt
                                                                                                                                                /* , bc_writeoffreversal.createtime AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS         VARCHAR(60))   AS reason ,
                                                                                                                                                cast( '''' AS        VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS        VARCHAR(60))   AS branchnumber ,
                                                                                                                                                cast(agmthostid AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                updatetime
                                                                                                                                         FROM   (
                                                                                                                                                          SELECT    bc_writeoff.id_stg           AS id,
                                                                                                                                                                    bc_writeoff.approvaldate_stg AS approvaldate,
                                                                                                                                                                    CASE
                                                                                                                                                                              WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                              ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                    END                            AS agmthostid,
                                                                                                                                                                    bc_writeoff.updatetime_stg     AS updatetime,
                                                                                                                                                                    bc_writeoff.approvalstatus_stg AS approvalstatus,
                                                                                                                                                                    bc_writeoff.createtime_stg     AS createtime,
                                                                                                                                                                    bc_writeoff.reversed_stg       AS reversed,
                                                                                                                                                                    ''WRITEOFF''                     AS writeoffflag
                                                                                                                                                          FROM      db_t_prod_stag.bc_writeoff
                                                                                                                                                          left join db_t_prod_stag.bc_invoiceitem
                                                                                                                                                          ON        bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                                                                                                                          left join db_t_prod_stag.bc_invoice
                                                                                                                                                          ON        bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_invoicestream
                                                                                                                                                          ON        bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_taccountcontainer
                                                                                                                                                          ON        bc_taccountcontainer.id_stg=bc_writeoff.taccountcontainerid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policyperiod a
                                                                                                                                                          ON        a.id_stg=bc_invoiceitem.policyperiodid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policyperiod b
                                                                                                                                                          ON        b.hiddentaccountcontainerid_stg=bc_taccountcontainer.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policy
                                                                                                                                                          ON        bc_policy.id_stg=b.policyid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_account
                                                                                                                                                          ON        bc_account.id_stg=bc_policy.accountid_stg
                                                                                                                                                          WHERE     bc_writeoff.id_stg NOT IN
                                                                                                                                                                    (
                                                                                                                                                                           SELECT ownerid_stg
                                                                                                                                                                           FROM   db_t_prod_stag.bc_revwriteoff)
                                                                                                                                                          AND       bc_writeoff.updatetime_stg > ($start_dttm)
                                                                                                                                                          AND       bc_writeoff.updatetime_stg <= ($end_dttm)
                                                                                                                                                          UNION
                                                                                                                                                          SELECT    bc_writeoff.id_stg           AS id,
                                                                                                                                                                    bc_writeoff.approvaldate_stg AS approvaldate,
                                                                                                                                                                    CASE
                                                                                                                                                                              WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                              ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                    END                            AS agmthostid,
                                                                                                                                                                    bc_writeoff.updatetime_stg     AS updatetime,
                                                                                                                                                                    bc_writeoff.approvalstatus_stg AS approvalstatus,
                                                                                                                                                                    bc_writeoff.createtime_stg     AS createtime,
                                                                                                                                                                    bc_writeoff.reversed_stg       AS reversed,
                                                                                                                                                                    ''REVWRITEOFF''                  AS writeoffflag
                                                                                                                                                          FROM      db_t_prod_stag.bc_writeoff
                                                                                                                                                          left join db_t_prod_stag.bc_invoiceitem
                                                                                                                                                          ON        bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                                                                                                                          left join db_t_prod_stag.bc_invoice
                                                                                                                                                          ON        bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_invoicestream
                                                                                                                                                          ON        bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_taccountcontainer
                                                                                                                                                          ON        bc_taccountcontainer.id_stg=bc_writeoff.taccountcontainerid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policyperiod a
                                                                                                                                                          ON        a.id_stg=bc_invoiceitem.policyperiodid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policyperiod b
                                                                                                                                                          ON        b.hiddentaccountcontainerid_stg=bc_taccountcontainer.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policy
                                                                                                                                                          ON        bc_policy.id_stg=b.policyid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_account
                                                                                                                                                          ON        bc_account.id_stg=bc_policy.accountid_stg
                                                                                                                                                          WHERE     bc_writeoff.id_stg IN
                                                                                                                                                                    (
                                                                                                                                                                           SELECT ownerid_stg
                                                                                                                                                                           FROM   db_t_prod_stag.bc_revwriteoff)
                                                                                                                                                          AND       bc_writeoff.updatetime_stg > ($start_dttm)
                                                                                                                                                          AND       bc_writeoff.updatetime_stg <= ($end_dttm) ) bc_writeoff
                                                                                                                                         WHERE  createtime IS NOT NULL
                                                                                                                                         AND    reversed=1
                                                                                                                                         UNION
                                                                                                                                         /***************************bc_basenonrecdistitem****************************/
                                                                                                                                         SELECT ''EV_ACTVY_TYPE28''                              AS ev_act_type_code ,
                                                                                                                                                cast( bc_basenonrecdistitem.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE4''                                 AS status ,
                                                                                                                                                executeddate                                   AS strt_dt
                                                                                                                                                /* ,  createtime              AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS                                 VARCHAR(60))   AS reason ,
                                                                                                                                                cast( '''' AS                                VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS                                VARCHAR(60))   AS branchnumber ,
                                                                                                                                                cast(bc_basenonrecdistitem.agmt_host_id AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                bc_basenonrecdistitem.updatetime
                                                                                                                                         FROM  (
                                                                                                                                                          SELECT    bc_basenonrecdistitem.id_stg           AS id,
                                                                                                                                                                    bc_basenonrecdistitem.executeddate_stg AS executeddate,
                                                                                                                                                                    CASE
                                                                                                                                                                              WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                              ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                    END                                  AS agmt_host_id,
                                                                                                                                                                    bc_basenonrecdistitem.updatetime_stg AS updatetime
                                                                                                                                                          FROM      db_t_prod_stag.bc_basenonrecdistitem
                                                                                                                                                          left join db_t_prod_stag.bc_basedist
                                                                                                                                                          ON        bc_basedist.id_stg=bc_basenonrecdistitem.activedistid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                                          ON        bc_basemoneyreceived.basedistid_stg=bc_basedist.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_unappliedfund
                                                                                                                                                          ON        bc_basemoneyreceived.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_invoicestream
                                                                                                                                                          ON        bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_account
                                                                                                                                                          ON        bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                                                                                          WHERE     bc_basenonrecdistitem.updatetime_stg > ($start_dttm)
                                                                                                                                                          AND       bc_basenonrecdistitem.updatetime_stg <= ($end_dttm) ) bc_basenonrecdistitem
                                                                                                                                         WHERE  executeddate IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE29''                              AS ev_act_type_code ,
                                                                                                                                                cast( bc_basenonrecdistitem.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE1''                                 AS status ,
                                                                                                                                                reverseddate                                   AS strt_dt
                                                                                                                                                /* ,createtime AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS                                 VARCHAR(60))   AS reason ,
                                                                                                                                                cast( '''' AS                                VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS                                VARCHAR(60))   AS branchnumber ,
                                                                                                                                                cast(bc_basenonrecdistitem.agmt_host_id AS VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                bc_basenonrecdistitem.updatetime
                                                                                                                                         FROM   (
                                                                                                                                                          SELECT    bc_basenonrecdistitem.id_stg           AS id,
                                                                                                                                                                    bc_basenonrecdistitem.reverseddate_stg AS reverseddate,
                                                                                                                                                                    CASE
                                                                                                                                                                              WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                                                                                                              ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                                                                                                                    END                                  AS agmt_host_id,
                                                                                                                                                                    bc_basenonrecdistitem.updatetime_stg AS updatetime
                                                                                                                                                          FROM      db_t_prod_stag.bc_basenonrecdistitem
                                                                                                                                                          left join db_t_prod_stag.bc_basedist
                                                                                                                                                          ON        bc_basedist.id_stg=bc_basenonrecdistitem.activedistid_stg
                                                                                                                                                          left join db_t_prod_stag.bc_basemoneyreceived
                                                                                                                                                          ON        bc_basemoneyreceived.basedistid_stg=bc_basedist.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_unappliedfund
                                                                                                                                                          ON        bc_basemoneyreceived.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_invoicestream
                                                                                                                                                          ON        bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_account
                                                                                                                                                          ON        bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                                                                                                                          WHERE     bc_basenonrecdistitem.updatetime_stg > ($start_dttm)
                                                                                                                                                          AND       bc_basenonrecdistitem.updatetime_stg <= ($end_dttm) ) bc_basenonrecdistitem
                                                                                                                                         WHERE  bc_basenonrecdistitem.reverseddate IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE26''                       AS ev_act_type_code ,
                                                                                                                                                cast(bc_basedistitem.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE9''                          AS status ,
                                                                                                                                                bc_basedistitem.executeddate            AS strt_dt
                                                                                                                                                /* ,  bc_basedistitem.createtime AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS  VARCHAR(60))   AS reason ,
                                                                                                                                                cast( '''' AS VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS VARCHAR(60))   AS branchnumber ,
                                                                                                                                                cast('''' AS  VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                bc_basedistitem.updatetime
                                                                                                                                         FROM   (
                                                                                                                                                          SELECT    bc_basedistitem.id_stg           AS id,
                                                                                                                                                                    bc_basedistitem.updatetime_stg   AS updatetime,
                                                                                                                                                                    bc_basedistitem.executeddate_stg AS executeddate,
                                                                                                                                                                    reverseddistid_stg               AS reverseddistid
                                                                                                                                                          FROM      db_t_prod_stag.bc_basedistitem
                                                                                                                                                          join      db_t_prod_stag.bc_invoiceitem
                                                                                                                                                          ON        bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                                                                                          join      db_t_prod_stag.bc_invoice
                                                                                                                                                          ON        bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                                                                          join      db_t_prod_stag.bctl_invoiceitemtype
                                                                                                                                                          ON        bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                                                                                          join      db_t_prod_stag.bc_charge
                                                                                                                                                          ON        bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                                                                                          join      db_t_prod_stag.bc_chargepattern
                                                                                                                                                          ON        bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                                                                                          join      db_t_prod_stag.bctl_chargecategory
                                                                                                                                                          ON        bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policyperiod
                                                                                                                                                          ON        bc_basedistitem.policyperiodid_stg=bc_policyperiod.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_chargeinstancecontext
                                                                                                                                                          ON        bc_chargeinstancecontext.directbillpaymentitemid_stg = bc_basedistitem.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_transaction
                                                                                                                                                          ON        bc_transaction.id_stg=bc_chargeinstancecontext.transactionid_stg
                                                                                                                                                          WHERE     bc_basedistitem.updatetime_stg > ($start_dttm)
                                                                                                                                                          AND       bc_basedistitem.updatetime_stg <= ($end_dttm) ) bc_basedistitem
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE27''                       AS ev_act_type_code ,
                                                                                                                                                cast(bc_basedistitem.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE8''                          AS status ,
                                                                                                                                                bc_basedistitem.reverseddate            AS strt_dt
                                                                                                                                                /* , bc_basedistitem.createtime AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS  VARCHAR(60))   AS reason ,
                                                                                                                                                cast( '''' AS VARCHAR(60))   AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS VARCHAR(60))   AS branchnumber ,
                                                                                                                                                cast('''' AS  VARCHAR (100)) AS nk_publicid ,
                                                                                                                                                bc_basedistitem.updatetime
                                                                                                                                         FROM   (
                                                                                                                                                          SELECT    bc_basedistitem.id_stg           AS id,
                                                                                                                                                                    bc_basedistitem.updatetime_stg   AS updatetime,
                                                                                                                                                                    bc_basedistitem.executeddate_stg AS executeddate,
                                                                                                                                                                    reverseddistid_stg               AS reverseddistid,
                                                                                                                                                                    bc_basedistitem.reverseddate_stg AS reverseddate
                                                                                                                                                          FROM      db_t_prod_stag.bc_basedistitem
                                                                                                                                                          join      db_t_prod_stag.bc_invoiceitem
                                                                                                                                                          ON        bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                                                                                                                          join      db_t_prod_stag.bc_invoice
                                                                                                                                                          ON        bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                                                                          join      db_t_prod_stag.bctl_invoiceitemtype
                                                                                                                                                          ON        bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                                                                                          join      db_t_prod_stag.bc_charge
                                                                                                                                                          ON        bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                                                                                                                          join      db_t_prod_stag.bc_chargepattern
                                                                                                                                                          ON        bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                                                                                                                          join      db_t_prod_stag.bctl_chargecategory
                                                                                                                                                          ON        bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                                                                                                                          left join db_t_prod_stag.bc_policyperiod
                                                                                                                                                          ON        bc_basedistitem.policyperiodid_stg=bc_policyperiod.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_chargeinstancecontext
                                                                                                                                                          ON        bc_chargeinstancecontext.directbillpaymentitemid_stg = bc_basedistitem.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_transaction
                                                                                                                                                          ON        bc_transaction.id_stg=bc_chargeinstancecontext.transactionid_stg
                                                                                                                                                          WHERE     bc_basedistitem.updatetime_stg > ($start_dttm)
                                                                                                                                                          AND       bc_basedistitem.updatetime_stg <= ($end_dttm)
                                                                                                                                                          AND       reverseddistid_stg IS NOT NULL ) bc_basedistitem
                                                                                                                                         WHERE  bc_basedistitem.reverseddate IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         SELECT ''EV_ACTVY_TYPE36''                        AS ev_act_type_code ,
                                                                                                                                                cast(bc_unappliedfund.id AS VARCHAR(50)) AS key1 ,
                                                                                                                                                ''EV_SBTYPE2''                             AS SUBTYPE ,
                                                                                                                                                ''EV_STS_TYPE8''                           AS status ,
                                                                                                                                                bc_unappliedfund.updatetime              AS strt_dt
                                                                                                                                                /* , bc_basedistitem.createtime AS strt_dt */
                                                                                                                                                ,
                                                                                                                                                cast('''' AS  VARCHAR(60)) AS reason ,
                                                                                                                                                cast( '''' AS VARCHAR(60)) AS typecode_riskstatus ,
                                                                                                                                                cast( '''' AS VARCHAR(60)) AS branchnumber ,
                                                                                                                                                accountnumber            AS nk_publicid ,
                                                                                                                                                bc_unappliedfund.updatetime
                                                                                                                                         FROM   (
                                                                                                                                                          SELECT    bc_unappliedfund.id_stg         AS id,
                                                                                                                                                                    bc_unappliedfund.updatetime_stg AS updatetime,
                                                                                                                                                                    bc_account.accountnumber_stg    AS accountnumber
                                                                                                                                                          FROM      db_t_prod_stag.bc_unappliedfund
                                                                                                                                                          left join db_t_prod_stag.bc_account
                                                                                                                                                          ON        bc_unappliedfund.accountid_stg = bc_account.id_stg
                                                                                                                                                          left join db_t_prod_stag.bc_taccount
                                                                                                                                                          ON        bc_unappliedfund.taccountid_stg = bc_taccount.id_stg
                                                                                                                                                          WHERE     (
                                                                                                                                                                              bc_unappliedfund.updatetime_stg > ($start_dttm)
                                                                                                                                                                    AND       bc_unappliedfund.updatetime_stg <= ($end_dttm))
                                                                                                                                                          OR        (
                                                                                                                                                                              bc_taccount.updatetime_stg > ($start_dttm)
                                                                                                                                                                    AND       bc_taccount.updatetime_stg <= ($end_dttm)) ) bc_unappliedfund ) x ) src
                                                                                                  /* LKP_TERDATA_ETL_XLAT_EV_ACTY_CD */
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                                                                          ''DS'' )
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) xlat_ev_cd
                                                                                  ON              src.ev_act_type_code=xlat_ev_cd.src_idntftn_val
                                                                                                  /* LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD */
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_SBTYPE''
                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) AS xlat_src_ev_cd
                                                                                  ON              src.SUBTYPE=xlat_src_ev_cd.src_idntftn_val
                                                                                                  /* LKP_TERADATA_ETL_REF_XLAT_EV_STS */
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_STS_TYPE''
                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' )xlat_ev_sts
                                                                                  ON              src.status=xlat_ev_sts.src_idntftn_val
                                                                                                  /*  LOOK UP TABLE DB_T_PROD_CORE.EV  */
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   ev.ev_id            AS ev_id,
                                                                                                                    ev.ev_end_dttm      AS ev_end_dttm,
                                                                                                                    ev.src_trans_id     AS src_trans_id,
                                                                                                                    ev.ev_sbtype_cd     AS ev_sbtype_cd,
                                                                                                                    ev.ev_actvy_type_cd AS ev_actvy_type_cd
                                                                                                           FROM     db_t_prod_core.ev   AS ev qualify row_number() over(PARTITION BY ev.ev_sbtype_cd,ev.ev_actvy_type_cd, ev.src_trans_id ORDER BY ev.edw_end_dttm DESC) = 1 ) lkp_ev
                                                                                  ON              lkp_ev.src_trans_id=src.key1
                                                                                  AND             lkp_ev.ev_sbtype_cd=xlat_src_ev_cd.tgt_idntftn_val
                                                                                  AND             lkp_ev.ev_actvy_type_cd=xlat_ev_cd.tgt_idntftn_val
                                                                                                  /* LOOK UP DB_T_PROD_CORE.INSRNC_QUOTN */
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   insrnc_quotn.quotn_id       AS quotn_id,
                                                                                                                    insrnc_quotn.nk_job_nbr     AS nk_job_nbr,
                                                                                                                    insrnc_quotn.vers_nbr       AS vers_nbr
                                                                                                           FROM     db_t_prod_core.insrnc_quotn AS insrnc_quotn qualify row_number() over(PARTITION BY insrnc_quotn.nk_job_nbr, insrnc_quotn.vers_nbr, insrnc_quotn.src_sys_cd ORDER BY insrnc_quotn.edw_end_dttm DESC) = 1 )lkp_quotn
                                                                                  ON              lkp_quotn.vers_nbr=src.branchnumber
                                                                                  AND             lkp_quotn.nk_job_nbr=src.key1
                                                                                                  /* LOOK UP AGMT_PPV */
                                                                                  left outer join ( select
                                                                                                  CASE
                                                                                                                  WHEN a.agmt_type_cd=''PPV'' THEN agmt_id
                                                                                                                  ELSE NULL
                                                                                                  END AS agmt_id, a.nk_src_key,a.agmt_type_cd FROM (
                                                                                                         SELECT agmt.agmt_id       AS agmt_id,
                                                                                                                agmt.host_agmt_num AS host_agmt_num,
                                                                                                                agmt.nk_src_key    AS nk_src_key,
                                                                                                                agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                                agmt.edw_end_dttm
                                                                                                         FROM   db_t_prod_core.agmt AS agmt
                                                                                                         WHERE  agmt_type_cd IN(''PPV'')
                                                                                                                /* QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1 */
                                                                                                         AND    cast(agmt.edw_end_dttm AS DATE) =''9999-12-31'' ) a )lkp_agmt_ppv
                                                                                  ON              lkp_agmt_ppv.nk_src_key=src.nk_publicid
                                                                                  AND             lkp_agmt_ppv.agmt_type_cd=''$p_agmt_type_cd_policy_version''
                                                                                                  /* LOOK UP AGMT_ACT */
                                                                                  left outer join ( select
                                                                                                  CASE
                                                                                                                  WHEN a.agmt_type_cd=''ACT'' THEN agmt_id
                                                                                                                  ELSE NULL
                                                                                                  END AS agmt_id, a.nk_src_key,a.agmt_type_cd FROM (
                                                                                                           SELECT   agmt.agmt_id       AS agmt_id,
                                                                                                                    agmt.host_agmt_num AS host_agmt_num,
                                                                                                                    agmt.nk_src_key    AS nk_src_key,
                                                                                                                    agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                                    agmt.edw_end_dttm
                                                                                                           FROM     db_t_prod_core.agmt AS agmt
                                                                                                           WHERE    agmt_type_cd IN(''ACT'') qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) a )lkp_agmt_act
                                                                                  ON              lkp_agmt_act.nk_src_key=src.nk_publicid
                                                                                  AND             lkp_agmt_act.agmt_type_cd=''ACT''
                                                                                                  /* LOOK UP AGMT_INV */
                                                                                  left outer join ( select
                                                                                                  CASE
                                                                                                                  WHEN a.agmt_type_cd=''INV'' THEN agmt_id
                                                                                                                  ELSE NULL
                                                                                                  END AS agmt_id, a.nk_src_key,a.agmt_type_cd FROM (
                                                                                                         SELECT agmt.agmt_id       AS agmt_id,
                                                                                                                agmt.host_agmt_num AS host_agmt_num,
                                                                                                                agmt.nk_src_key    AS nk_src_key,
                                                                                                                agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                                                agmt.edw_end_dttm
                                                                                                         FROM   db_t_prod_core.agmt AS agmt
                                                                                                         WHERE  agmt_type_cd IN(''INV'')
                                                                                                                /* QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM ORDER BY AGMT.EDW_END_DTTM desc) = 1 */
                                                                                                         AND    cast(agmt.edw_end_dttm AS DATE) =''9999-12-31'' ) a )lkp_agmt_inv
                                                                                  ON              lkp_agmt_inv.nk_src_key=src.nk_publicid
                                                                                  AND             lkp_agmt_inv.agmt_type_cd=''INV'' )xlat_src
                                                  left outer join
                                                                  (
                                                                           SELECT   ev_sts.ev_sts_strt_dttm AS ev_sts_strt_dttm,
                                                                                    ev_sts.ev_sts_txt       AS ev_sts_txt,
                                                                                    ev_sts.agmt_id          AS agmt_id,
                                                                                    ev_sts.quotn_id         AS quotn_id,
                                                                                    ev_sts.ev_id            AS ev_id,
                                                                                    ev_sts.ev_sts_type_cd   AS ev_sts_type_cd
                                                                           FROM     db_t_prod_core.ev_sts   AS ev_sts
                                                                           join     db_t_prod_core.ev       AS ev
                                                                           ON       ev.ev_id=ev_sts.ev_id
                                                                           join
                                                                                    (
                                                                                           SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                  teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                           FROM   db_t_prod_core.teradata_etl_ref_xlat  AS teradata_etl_ref_xlat
                                                                                           WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''EV_SBTYPE''
                                                                                           AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                           AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                           AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31''
                                                                                           AND    src_idntftn_val=''EV_SBTYPE2'') teradata_etl_ref_xlat
                                                                           ON       teradata_etl_ref_xlat.tgt_idntftn_val=ev.ev_sbtype_cd qualify row_number () over (PARTITION BY ev_sts.ev_id, ev_sts.quotn_id ORDER BY ev_sts.edw_end_dttm DESC)=1
                                                                                    
                                                                  ) tgt_ev_sts
                                                  ON              tgt_ev_sts.ev_id=xlat_src.ev_id
                                                  AND             cast(tgt_ev_sts.quotn_id AS DECIMAL(19,0))=cast(xlat_src.quotn_id AS DECIMAL(19,0)) ) src ) );
  -- Component exp_pass_from_src, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_src AS
  (
         SELECT sq_pc_job.tgt_ev_id                                                    AS tgt_ev_id,
                sq_pc_job.tgt_ev_sts_type_cd                                           AS tgt_ev_sts_type_cd,
                sq_pc_job.tgt_ev_sts_strt_dttm                                         AS tgt_ev_sts_strt_dttm,
                sq_pc_job.tgt_ev_sts_txt                                               AS tgt_ev_sts_txt,
                sq_pc_job.tgt_agmt_id                                                  AS tgt_agmt_id,
                sq_pc_job.tgt_quotn_id                                                 AS tgt_quotn_id,
                NULL                                                                   AS tgt_edw_strt_dttm,
                NULL                                                                   AS tgt_edw_end_dttm,
                sq_pc_job.src_ev_id                                                    AS src_ev_id,
                sq_pc_job.src_ev_sts_type_cd                                           AS src_ev_sts_type_cd,
                sq_pc_job.src_ev_sts_strt_dttm                                         AS src_ev_sts_strt_dttm,
                sq_pc_job.src_ev_sts_txt                                               AS src_ev_sts_txt,
                sq_pc_job.src_agmt_id                                                  AS src_agmt_id,
                sq_pc_job.src_quotn_id                                                 AS src_quotn_id,
                sq_pc_job.src_trans_strt_dttm                                          AS src_trans_strt_dttm,
                sq_pc_job.src_rnk                                                      AS src_rnk,
                sq_pc_job.ins_upd_flag                                                 AS ins_upd_flag,
                current_timestamp                                                      AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                $prcs_id                                                               AS out_prcs_id,
                sq_pc_job.source_record_id
         FROM   sq_pc_job );
  -- Component rtr_ev_sts_Insert, Type ROUTER Output Group Insert
  create or replace TEMPORARY TABLE rtr_ev_sts_insert AS
  SELECT exp_pass_from_src.tgt_ev_id            AS tgt_ev_id,
         exp_pass_from_src.tgt_ev_sts_type_cd   AS tgt_ev_sts_type_cd,
         exp_pass_from_src.tgt_ev_sts_strt_dttm AS tgt_ev_sts_strt_dttm,
         exp_pass_from_src.tgt_ev_sts_txt       AS tgt_ev_sts_txt,
         exp_pass_from_src.tgt_agmt_id          AS tgt_agmt_id,
         exp_pass_from_src.tgt_quotn_id         AS tgt_quotn_id,
         exp_pass_from_src.tgt_edw_strt_dttm    AS tgt_edw_strt_dttm,
         exp_pass_from_src.tgt_edw_end_dttm     AS tgt_edw_end_dttm,
         exp_pass_from_src.src_ev_id            AS src_ev_id,
         exp_pass_from_src.src_ev_sts_type_cd   AS src_ev_sts_type_cd,
         exp_pass_from_src.src_ev_sts_strt_dttm AS src_ev_sts_strt_dttm,
         exp_pass_from_src.src_ev_sts_txt       AS src_ev_sts_txt,
         exp_pass_from_src.src_agmt_id          AS src_agmt_id,
         exp_pass_from_src.src_quotn_id         AS src_quotn_id,
         exp_pass_from_src.src_trans_strt_dttm  AS src_trans_strt_dttm,
         exp_pass_from_src.src_rnk              AS src_rnk,
         exp_pass_from_src.ins_upd_flag         AS ins_upd_flag,
         exp_pass_from_src.out_edw_strt_dttm    AS edw_strt_dttm,
         exp_pass_from_src.out_edw_end_dttm     AS edw_end_dttm,
         exp_pass_from_src.out_prcs_id          AS prcs_id,
         exp_pass_from_src.source_record_id
  FROM   exp_pass_from_src
  WHERE  exp_pass_from_src.src_ev_id IS NOT NULL
  AND    (
                exp_pass_from_src.ins_upd_flag = ''I''
         OR     exp_pass_from_src.ins_upd_flag = ''U'' );
  
  -- Component upd_ev_sts_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_sts_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_sts_insert.src_ev_id            AS in_ev_id,
                rtr_ev_sts_insert.src_ev_sts_type_cd   AS in_ev_sts_type_cd,
                rtr_ev_sts_insert.src_ev_sts_strt_dttm AS in_ev_sts_strt_dttm,
                rtr_ev_sts_insert.src_ev_sts_txt       AS in_ev_sts_txt1,
                rtr_ev_sts_insert.src_agmt_id          AS in_agmt_id,
                rtr_ev_sts_insert.src_quotn_id         AS in_quotn_id,
                rtr_ev_sts_insert.prcs_id              AS in_prcs_id,
                rtr_ev_sts_insert.edw_strt_dttm        AS in_edw_strt_dttm,
                rtr_ev_sts_insert.edw_end_dttm         AS in_edw_end_dttm,
                rtr_ev_sts_insert.src_trans_strt_dttm  AS transaction_eff_date,
                rtr_ev_sts_insert.src_rnk              AS rank1,
                0                                      AS update_strategy_action,
				rtr_ev_sts_insert.source_record_id
         FROM   rtr_ev_sts_insert );
  -- Component pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE pass_to_tgt_ins AS
  (
         SELECT upd_ev_sts_ins.in_ev_id                                                AS ev_id,
                upd_ev_sts_ins.in_ev_sts_type_cd                                       AS ev_sts_type_cd,
                upd_ev_sts_ins.in_ev_sts_strt_dttm                                     AS ev_sts_strt_dttm,
                upd_ev_sts_ins.in_ev_sts_txt1                                          AS ev_sts_txt,
                upd_ev_sts_ins.in_agmt_id                                              AS agmt_id,
                upd_ev_sts_ins.in_quotn_id                                             AS quotn_id,
                upd_ev_sts_ins.in_prcs_id                                              AS prcs_id,
                dateadd(''second'', ( 2 * ( upd_ev_sts_ins.rank1 - 1 ) ), current_timestamp) AS out_edw_strt_dttm,
                upd_ev_sts_ins.in_edw_end_dttm                                         AS edw_end_dttm,
                upd_ev_sts_ins.transaction_eff_date                                    AS trans_strt_dttm,
                upd_ev_sts_ins.source_record_id
         FROM   upd_ev_sts_ins );
  -- Component tgt_EV_STS_ins, Type TARGET
  INSERT INTO db_t_prod_core.ev_sts
              (
                          ev_id,
                          ev_sts_type_cd,
                          ev_sts_strt_dttm,
                          ev_sts_txt,
                          agmt_id,
                          quotn_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT pass_to_tgt_ins.ev_id             AS ev_id,
         pass_to_tgt_ins.ev_sts_type_cd    AS ev_sts_type_cd,
         pass_to_tgt_ins.ev_sts_strt_dttm  AS ev_sts_strt_dttm,
         pass_to_tgt_ins.ev_sts_txt        AS ev_sts_txt,
         pass_to_tgt_ins.agmt_id           AS agmt_id,
         pass_to_tgt_ins.quotn_id          AS quotn_id,
         pass_to_tgt_ins.prcs_id           AS prcs_id,
         pass_to_tgt_ins.out_edw_strt_dttm AS edw_strt_dttm,
         pass_to_tgt_ins.edw_end_dttm      AS edw_end_dttm,
         pass_to_tgt_ins.trans_strt_dttm   AS trans_strt_dttm
  FROM   pass_to_tgt_ins;
  
  -- PIPELINE END FOR 2
  -- Component tgt_EV_STS_ins, Type Post SQL
  UPDATE db_t_prod_core.ev_sts
    SET    trans_end_dttm= a.lead1,
         edw_end_dttm = a.lead
  FROM   (
                         SELECT DISTINCT ev_sts.ev_id,
                                         ev_sts.edw_strt_dttm,
                                         nvl(ev_sts.quotn_id,9999)                                                                                                                                               AS quotn_id,
                                         max(ev_sts.edw_strt_dttm) over (PARTITION BY ev_sts.ev_id ORDER BY ev_sts.edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead,
                                         max(ev_sts.trans_strt_dttm) over (PARTITION BY ev_sts.ev_id ORDER BY ev_sts.edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.ev_sts
                         join
                                         (
                                                  SELECT   ev_id
                                                  FROM     db_t_prod_core.ev
                                                  WHERE    ev.ev_sbtype_cd<>''PLCYTRNS'' qualify row_number () over (PARTITION BY ev.ev_id ORDER BY edw_end_dttm,edw_strt_dttm DESC)=1 )ev
                         ON              ev.ev_id=ev_sts.ev_id ) a

  WHERE  ev_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    ev_sts.ev_id=a.ev_id
  AND    nvl(ev_sts.quotn_id,9999)=nvl(a.quotn_id,9999)
  AND    ev_sts.trans_strt_dttm <>ev_sts.trans_end_dttm
  AND    lead IS NOT NULL;

END;
';