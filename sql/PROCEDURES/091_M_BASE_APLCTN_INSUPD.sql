-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_pc_job, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_job AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_aplctn_id,
                $2  AS lkp_host_aplctn_id,
                $3  AS lkp_aplctn_type_cd,
                $4  AS lkp_edw_strt_dt,
                $5  AS lkp_trans_strt_dt,
                $6  AS lkp_edw_end_dttm,
                $7  AS lkp_sys_src_cd,
                $8  AS in_host_aplctn_id,
                $9  AS in_aplctn_type_cd,
                $10 AS in_aplctn_cmpltd_dt,
                $11 AS in_aplctn_recvd_dt,
                $12 AS in_agmt_objtv_type_cd,
                $13 AS in_prod_grp_id,
                $14 AS in_prod_id,
                $15 AS edw_strt_dttm,
                $16 AS trans_strt_dttm,
                $17 AS edw_end_dttm,
                $18 AS trans_end_dttm,
                $19 AS in_sys_src_cd,
                $20 AS in_aplctn_quot_type,
                $21 AS chnl_type_cd,
                $22 AS retired,
                $23 AS in_policynumber1,
                $24 AS in_sys_src_cd11,
                $25 AS out_flag,
                $26 AS aplctn_id,
                $27 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( select b.aplctn_id,b.host_aplctn_id,b.aplctn_type_cd,b.edw_strt_dttm,b.trans_strt_dttm1,b.edw_end_dttm,b.src_sys_cd1,in_host_aplctn_id,o_aplctn_type_cd AS in_aplctn_type_cd, closedate AS in_aplctn_cmpltd_dt, createdate2 AS in_aplctn_recvd_dt,out_agmt_objtv_type_cd AS in_agmt_objtv_type_cd,out_prod_grp_id AS in_prod_grp_id,prod_id_src AS in_prod_id, out_edw_strt_dttm,trans_strt_dttm,out_edw_end_dttm,out_trans_end_dttm,out_sys_src_cd,aplctn_quot_type_cd_src AS in_aplctn_quot_type_cd, chnl_type_cd1,retired_stg,policynumber_stg,sys_src_cd,
                                  CASE
                                           WHEN aplctn_id IS NULL THEN ''I''
                                           WHEN tgt_md5=scr_md5 THEN ''R''
                                           ELSE ''U''
                                  END AS out_flag,aplctn_id1 FROM (
                                            SELECT    cast(substring(cast(updatedt AS VARCHAR(50)) ,1,20)
                                                                ||''000000'' AS timestamp)             AS trans_strt_dttm,
                                                      cast(''9999-12-31 23:59:59.999999'' AS timestamp)   closedate,
                                                      jobnumber                                      AS in_host_aplctn_id,
                                                      CASE
                                                                WHEN createdt IS NULL THEN cast(''1900-01-01 00:00:00.000000''AS timestamp)
                                                                ELSE createdt
                                                      END AS createdate1,
                                                      cast(substring(cast(createdate1 AS VARCHAR(50)) ,1,20)
                                                                ||''000000'' AS timestamp) AS createdate2,
                                                      typecode_stg,
                                                      productname,
                                                      sys_src_cd,
                                                      aplctn_quote_type,
                                                      prod_subtype_cd,
                                                      CASE
                                                                WHEN typecode_grouptype_new= ''region''
                                                                OR        typecode_grouptype_new= ''salesdistrict_alfa''
                                                                OR        typecode_grouptype_new= ''servicecenter_alfa'' THEN ''CHNL_TYPE5''
                                                                WHEN typecode_grouptype_new=''custserv'' THEN ''phone''
                                                                ELSE ''Other/Unknown''
                                                      END AS in_grouptype ,
                                                      retired_stg,
                                                      policynumber_stg,
                                                      CASE
                                                                WHEN apltn_lkp.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                ELSE apltn_lkp.tgt_idntftn_val
                                                      END                          AS o_aplctn_type_cd,
                                                      src_sys_lkp.tgt_idntftn_val1 AS out_sys_src_cd,
                                                      CASE
                                                                WHEN quot_type_lkp.tgt_idntftn_val2 IS NULL THEN ''UNK''
                                                                ELSE quot_type_lkp.tgt_idntftn_val2
                                                      END AS aplctn_quot_type_cd_src,
                                                      CASE
                                                                WHEN chnl_type_lkp.tgt_idntftn_val3 IS NULL THEN ''UNK''
                                                                ELSE chnl_type_lkp.tgt_idntftn_val3
                                                      END                                             AS out_grouptype,
                                                      ''UNK''                                              out_agmt_objtv_type_cd,
                                                      9999                                               out_prod_grp_id,
                                                      cast(current_timestamp AS timestamp)               out_edw_strt_dttm,
                                                      cast(''9999-12-31 23:59:59.999999'' AS timestamp)    out_edw_end_dttm,
                                                      cast(''9999-12-31 23:59:59.999999'' AS timestamp)    out_trans_end_dttm,
                                                      prd1.prod_id                                       prod_id_src,
                                                      tgt_lkp.aplctn_id                               AS aplctn_id,
                                                      tgt_lkp.aplctn_cmpltd_dttm                      AS aplctn_cmpltd_dttm,
                                                      tgt_lkp.aplctn_recvd_dttm                       AS aplctn_recvd_dttm,
                                                      tgt_lkp.agmt_objtv_type_cd                      AS agmt_objtv_type_cd,
                                                      tgt_lkp.aplctn_quot_type_cd                     AS aplctn_quot_type_cd1,
                                                      tgt_lkp.prod_grp_id                             AS prod_grp_id,
                                                      tgt_lkp.prod_id                                 AS prod_id_tgt,
                                                      tgt_lkp.chnl_type_cd                            AS chnl_type_cd,
                                                      tgt_lkp.host_aplctn_num                         AS host_aplctn_num,
                                                      tgt_lkp.edw_strt_dttm                           AS edw_strt_dttm,
                                                      tgt_lkp.edw_end_dttm                            AS edw_end_dttm,
                                                      tgt_lkp.host_aplctn_id                          AS host_aplctn_id,
                                                      tgt_lkp.src_sys_cd                              AS src_sys_cd1,
                                                      tgt_lkp.aplctn_type_cd                          AS aplctn_type_cd,
                                                      tgt_lkp.trans_strt_dttm                         AS trans_strt_dttm1,
                                                      CASE
                                                                WHEN out_grouptype=''UNK''
                                                                AND       tgt_lkp.chnl_type_cd IS NOT NULL THEN tgt_lkp.chnl_type_cd
                                                                ELSE out_grouptype
                                                      END AS chnl_type_cd1,
                                                      CASE
                                                                WHEN out_grouptype=''UNK''
                                                                OR        out_grouptype IS NULL THEN tgt_lkp.chnl_type_cd
                                                                ELSE out_grouptype
                                                      END AS chnl_type_cd2,
                                                      CASE
                                                                WHEN tgt_lkp.aplctn_quot_type_cd IS NULL THEN ''UNK''
                                                                ELSE tgt_lkp.aplctn_quot_type_cd
                                                      END AS aplctn_quot_type_cd_tgt,
                                                      cast (cast(tgt_lkp.aplctn_cmpltd_dttm AS VARCHAR(100))
                                                                ||cast(cast(substring(cast(tgt_lkp.aplctn_recvd_dttm AS VARCHAR(50)) ,1,20)
                                                                ||''000000'' AS timestamp) AS           VARCHAR(100) )
                                                                ||cast(tgt_lkp.agmt_objtv_type_cd AS  VARCHAR(10))
                                                                || cast(tgt_lkp.prod_grp_id AS        VARCHAR(10))
                                                                || cast( coalesce( prod_id_tgt ,0) AS VARCHAR(10))
                                                                ||cast(aplctn_quot_type_cd_tgt AS     VARCHAR(40))
                                                                ||cast(tgt_lkp.chnl_type_cd AS        VARCHAR(50))
                                                                ||cast(tgt_lkp.host_aplctn_num AS     VARCHAR(100))AS VARCHAR(1000)) AS tgt_md5,
                                                      cast(cast((closedate ) AS                       VARCHAR(100))
                                                                ||cast(cast(substring(cast(createdate1 AS VARCHAR(50)) ,1,20)
                                                                ||''000000'' AS timestamp) AS         VARCHAR(100) )
                                                                ||cast(out_agmt_objtv_type_cd AS    VARCHAR(10))
                                                                ||cast(out_prod_grp_id AS           VARCHAR(10))
                                                                ||cast(coalesce( prod_id_src ,0) AS VARCHAR(10))
                                                                || cast(aplctn_quot_type_cd_src AS  VARCHAR(40))
                                                                ||cast(chnl_type_cd2 AS             VARCHAR(50))
                                                                ||cast(policynumber_stg AS          VARCHAR(100)) AS VARCHAR(1000)) AS scr_md5,
                                                      dir_lkp.aplctn_id1
                                            FROM      (
                                                                      SELECT DISTINCT src.pc_updatetime_stg                                                                                                                                                                                         AS updatedt ,
                                                                                      src.closedate_stg                                                                                                                                                                                             AS closedt ,
                                                                                      src.jobnumber_stg                                                                                                                                                                                             AS jobnumber ,
                                                                                      src.createtime_stg                                                                                                                                                                                            AS createdt ,
                                                                                      pctl_job.typecode_stg                                                                                                                                                                                         AS typecode_stg,
                                                                                      upper(coalesce(pctl_papolicytype_alfa.typecode_stg, pctl_hopolicytype_hoe.typecode_stg, pctl_bp7policytype_alfa.typecode_stg,pctl_puppolicytype.typecode_stg, src.prod_name,pctl_foppolicytype.typecode_stg)) AS productname ,
                                                                                      ''SRC_SYS4''                                                                                                                                                                                                    AS sys_src_cd ,
                                                                                      src.typecode_quote                                                                                                                                                                                            AS aplctn_quote_type
                                                                                      /* ,Typecode_Quote as Aplctn_quote_Type */
                                                                                      ,
                                                                                      ''PLCYTYPE'' AS prod_subtype_cd
                                                                                      /* ,Typecode_GroupType  AS Typecode_GroupType */
                                                                                      ,
                                                                                      CASE
                                                                                                      WHEN src.typecode_grouptype IN (''region'',
                                                                                                                                      ''salesdistrict_alfa'',
                                                                                                                                      ''servicecenter_alfa'') THEN ''CHNL_TYPE5''
                                                                                                      WHEN src.typecode_grouptype = ''custserv'' THEN ''phone''
                                                                                                      ELSE ''Other/Unknown''
                                                                                      END AS typecode_grouptype_new ,
                                                                                      src.retired_stg ,
                                                                                      src.policynumber_stg
                                                                      FROM
                                                                                      /* DB_T_PROD_STAG.pc_job */
                                                                                      (
                                                                                                      SELECT DISTINCT pc_job.nottakennotifdate_stg ,
                                                                                                                      pc_job.archivestate_stg ,
                                                                                                                      pc_job.archiveschemainfo_stg ,
                                                                                                                      pc_job.updatetime_stg ,
                                                                                                                      pc_job.notificationdate_stg ,
                                                                                                                      pc_job.id_stg ,
                                                                                                                      pc_job.source_stg ,
                                                                                                                      pc_job.excludereason_stg ,
                                                                                                                      pc_job.nextpurgecheckdate_stg ,
                                                                                                                      pc_job.createuserid_stg ,
                                                                                                                      pc_job.archivefailureid_stg ,
                                                                                                                      pc_job.rejectreason_stg ,
                                                                                                                      pc_job.closedate_stg ,
                                                                                                                      pc_job.beanversion_stg,
                                                                                                                      pc_job.retired_stg ,
                                                                                                                      pc_job.cancelreasoncode_stg ,
                                                                                                                      pc_job.changepolicynumber_stg ,
                                                                                                                      pc_job.updateuserid_stg ,
                                                                                                                      pc_job.primaryinsurednamedenorm_stg ,
                                                                                                                      pc_job.nonrenewalnotifdate_stg ,
                                                                                                                      pc_job.primaryinsuredname_stg ,
                                                                                                                      pc_job.quotetype_stg ,
                                                                                                                      pc_job.datequoteneeded_stg ,
                                                                                                                      pc_job.publicid_stg AS publicid_pcj ,
                                                                                                                      pc_job.sidebyside_stg ,
                                                                                                                      pc_job.jobnumber_stg ,
                                                                                                                      pc_job.rewritetype_stg ,
                                                                                                                      pc_job.createtime_stg ,
                                                                                                                      pc_job.auditinformationid_stg ,
                                                                                                                      pc_job.policyid_stg ,
                                                                                                                      pc_job.excludedfromarchive_stg,
                                                                                                                      /*  pc_job.RejectReasonText_stg , */
                                                                                                                      pc_job.archivefailuredetailsid_stg ,
                                                                                                                      pc_job.rescindnotificationdate_stg ,
                                                                                                                      pc_job.purgestatus_stg ,
                                                                                                                      pc_job.initialnotificationdate_stg ,
                                                                                                                      pc_job.lastnotifiedcancellationdate_stg ,
                                                                                                                      pc_job.jobgroup_stg ,
                                                                                                                      pc_job.cancelprocessdate_stg ,
                                                                                                                      pc_job.renewalcode_stg ,
                                                                                                                      pc_job.escalateafterholdreleased_stg ,
                                                                                                                      pc_job.reinstatecode_stg ,
                                                                                                                      pc_job.renewalnotifdate_stg ,
                                                                                                                      pc_job.paymentreceived_stg ,
                                                                                                                      pc_job.archivepartition_stg ,
                                                                                                                      pc_job.paymentreceived_cur_stg ,
                                                                                                                      pc_job.notificationackdate_stg ,
                                                                                                                      pc_job.archivedate_stg ,
                                                                                                                      pc_job.bindoption_stg ,
                                                                                                                      pc_job.nonrenewalcode_stg ,
                                                                                                                      pc_job.subtype_stg ,
                                                                                                                      pc_job.submissiondate_stg ,
                                                                                                                      /* pc_job.Description_stg , */
                                                                                                                      CASE
                                                                                                                                      WHEN pctl_job.typecode_stg IN ( ''Submission'',
                                                                                                                                                                     ''Renewal'',
                                                                                                                                                                     ''Rewrite'',
                                                                                                                                                                     ''Issuance'' ) THEN pc_policyperiod.periodstart_stg
                                                                                                                                      WHEN pctl_job.typecode_stg = ''Cancellation'' THEN pc_policyperiod.cancellationdate_stg
                                                                                                                                      WHEN pctl_job.typecode_stg IN (''PolicyChange'',
                                                                                                                                                                     ''Reinstatement'') THEN pc_policyperiod.editeffectivedate_stg
                                                                                                                      END ev_strt ,
                                                                                                                      /*pctl_policyperiodstatus.TYPECODE_stg ,
cancelreason.TYPECODE_stg ,
rejectreason.TYPECODE_stg ,
pctl_reinstatecode.TYPECODE_stg ,
pctl_renewalcode.TYPECODE_stg ,
pctl_nonrenewalcode.TYPECODE_stg , */
                                                                                                                      pc_policyperiod.branchnumber_stg ,
                                                                                                                      pc_policyperiod.totalpremiumrpt_stg ,
                                                                                                                      pc_policyperiod.totalpremadjrpt_alfa_stg,
                                                                                                                      pc_policyperiod.transactionpremiumrpt_stg ,
                                                                                                                      pc_policyperiod.editeffectivedate_stg ,
                                                                                                                      pc_policyperiod.periodend_stg ,
                                                                                                                      pc_policyperiod.policynumber_stg ,
                                                                                                                      pc_policyline.papolicytype_alfa_stg ,
                                                                                                                      pc_policyline.hopolicytype_stg ,
                                                                                                                      pc_policyline.claimsfreeind_alfa_stg,
                                                                                                                      pc_policyperiod.publicid_stg AS publicid_pcpl,
                                                                                                                      pc_policyperiod.rateasofdate_stg ,
                                                                                                                      pc_contact.addressbookuid_stg AS userpartyid_stg ,
                                                                                                                      pctl_quotetype.typecode_stg   AS typecode_quote,
                                                                                                                      /* TYPECODE_Quote --74 */
                                                                                                                      /* pctl_riskstatus_alfa.TYPECODE_stg ,  */
                                                                                                                      pctl_grouptype.typecode_stg AS typecode_grouptype,
                                                                                                                      /* Typecode_GroupType --76 */
                                                                                                                      pc_policyperiod.updatetime_stg           AS pc_updatetime_stg,
                                                                                                                      pc_policyperiod.generalplustier_alfa_stg AS generalplustier_alfa,
                                                                                                                      pc_policyperiod.retired_stg              AS policy_retired,
                                                                                                                      CASE
                                                                                                                                      WHEN pcp1.publicid_stg=pc_policyperiod.publicid_stg THEN ''Y''
                                                                                                                                      ELSE ''N''
                                                                                                                      END                                                              AS selectvesionofquote,
                                                                                                                      coalesce(pc_effectivedatedfields.overridecreditscore_alfa_stg,0) AS ratedinsurancescore,
                                                                                                                      pc_effectivedatedfields.continuousservicedate_alfa_stg,
                                                                                                                      /* ($start_dttm) as start_dttm, */
                                                                                                                      /*  ($end_dttm) as end_dttm, */
                                                                                                                      pc_effectivedatedfields.previnsurance_alfa_stg,
                                                                                                                      pctl_billingperiodicity.typecode_stg AS autolatepaybillingperiodicity,
                                                                                                                      pctl_cancellationsource.typecode_stg AS bill_payment_src,
                                                                                                                      pc_policyperiod.totalcostrpt_stg,
                                                                                                                      pc_policyperiod.transactioncostrpt_stg,
                                                                                                                      pc_policyperiod.totaldiscountpremrpt_alfa_stg,
                                                                                                                      pc_policyperiod.totalsurchargepremrpt_alfa_stg,
                                                                                                                      pctl_sourceofbusiness_alfa.typecode_stg AS src_of_busn_cd,
                                                                                                                      CASE
                                                                                                                                      WHEN pc_policyperiod. quotematuritylevel_stg = 1
                                                                                                                                      AND             vj.jobid_stg > 0 THEN 0
                                                                                                                                      ELSE 1
                                                                                                                      END                            AS validquote,
                                                                                                                      pc_policyperiod.createtime_stg AS createtime,
                                                                                                                      pc_policyterm.updatetime_stg   AS pc_policyterm_updatetime ,
                                                                                                                      pc_policyline.bp7policytype_alfa_stg ,
                                                                                                                      pc_effectivedatedfields.overridecreditscoredate_alfa_stg ,
                                                                                                                      pc_policyperiod.isquoteonline_alfa_stg
                                                                                                                      /*Added as part of  EIM-20110*/
                                                                                                                      ,
                                                                                                                      pc_effectivedatedfields.retentionscore_alfa_stg
                                                                                                                      /*Added as part of EIM-18360*/
                                                                                                                      ,
                                                                                                                      pc_policyline.puppolicytype_stg
                                                                                                                      /* PMOP--54877--Added as part of DB_T_STAG_MEMBXREF_PROD.Umbrella */
                                                                                                                      ,
                                                                                                                      pc_policyline.foppolicytype_stg
                                                                                                                      /*EIM-48779 ADDED AS PART OF FARM*/
                                                                                                                      ,
                                                                                                                      CASE
                                                                                                                                      WHEN (
                                                                                                                                                                      pc_policyline.machinerycoverableexists_stg = 1
                                                                                                                                                      OR              pc_policyline.livestockcoverableexists_stg = 1)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.blanketcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.blanketcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.dwellingcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.dwellingcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.feedandseedcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.feedandseedcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.liabilitycoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.liabilitycoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.outbuildingcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.outbuildingcoverableexists_stg IS NULL) THEN ''Machinery''
                                                                                                                                      WHEN (
                                                                                                                                                                      pc_policyline.liabilitycoverableexists_stg = 1)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.blanketcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.blanketcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.dwellingcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.dwellingcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.feedandseedcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.feedandseedcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.machinerycoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.machinerycoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.livestockcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.livestockcoverableexists_stg IS NULL)
                                                                                                                                      AND             (
                                                                                                                                                                      pc_policyline.outbuildingcoverableexists_stg =0
                                                                                                                                                      OR              pc_policyline.outbuildingcoverableexists_stg IS NULL) THEN ''FCL''
                                                                                                                      END prod_name
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
                                                                                                      ON              pc_user.contactid_stg = pc_contact.id_stg
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
                                                                                                                                      WHERE           quotematuritylevel_stg IN (2,3)) vj
                                                                                                      ON              pc_job.id_stg=vj.jobid_stg
                                                                                                                      /* and pctl_grouptype.TYPECODE in (''region'', ''salesdistrict_alfa'', ''servicecenter_alfa'',''custserv'') */
                                                                                                      WHERE           pc_policyperiod.updatetime_stg > ($start_dttm)
                                                                                                      AND             pc_policyperiod.updatetime_stg <= ($end_dttm )
                                                                                                      AND             pctl_policyperiodstatus.typecode_stg <>''Temporary''
                                                                                                      AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                      AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                                      AND             pc_policyline.expirationdate_stg IS NULL ) src
                                                                      inner join      db_t_prod_stag.pctl_job
                                                                      ON              src.subtype_stg=pctl_job.id_stg
                                                                      left outer join db_t_prod_stag.pctl_papolicytype_alfa
                                                                      ON              src.papolicytype_alfa_stg=pctl_papolicytype_alfa.id_stg
                                                                      left outer join db_t_prod_stag.pctl_hopolicytype_hoe
                                                                      ON              src.hopolicytype_stg=pctl_hopolicytype_hoe.id_stg
                                                                      left outer join db_t_prod_stag.pctl_bp7policytype_alfa
                                                                      ON              src.bp7policytype_alfa_stg=pctl_bp7policytype_alfa.id_stg
                                                                      left outer join db_t_prod_stag.pctl_puppolicytype
                                                                      ON              src.puppolicytype_stg= pctl_puppolicytype.id_stg
                                                                                      /* --PMOP--54877--Added as part of DB_T_STAG_MEMBXREF_PROD.Umbrella */
                                                                      left join       db_t_prod_stag.pctl_foppolicytype
                                                                      ON              pctl_foppolicytype.id_stg = src.foppolicytype_stg
                                                                                      /* EIM-48779 FARM CHANGES */
                                                                      WHERE           pctl_job.typecode_stg IN (''Submission'',
                                                                                                                ''PolicyChange'',
                                                                                                                ''Renewal'')
                                                                      AND             src.policynumber_stg IS NOT NULL
                                                                                      /* AND pc_job.JobNumber in (''J0113405626'') */
                                                                                      /* group by pc_job.JobNumber, pctl_job.TYPECODE, pc_job.retired,policynumber */
                                                                                      qualify row_number () over (PARTITION BY src.jobnumber_stg, pctl_job.typecode_stg ORDER BY src.pc_updatetime_stg DESC,
                                                                                      CASE
                                                                                                      WHEN (
                                                                                                                                      typecode_grouptype_new IN (''phone'') ) THEN 1
                                                                                                      WHEN (
                                                                                                                                      typecode_grouptype_new IN (''CHNL_TYPE5'') ) THEN 2
                                                                                                      ELSE 3
                                                                                      END, productname DESC) =1 )a
                                            left join
                                                      (
                                                             SELECT *
                                                             FROM   db_t_prod_core.prod AS prd
                                                             WHERE  edw_end_dttm=''9999-12-31 23:59:59.999999'') prd1
                                            ON        upper(a.productname)=upper(prd1.prod_name)
                                                      /* --PMOP--54877--Added as part of DB_T_STAG_MEMBXREF_PROD.Umbrella */
                                            AND       a.prod_subtype_cd=prd1.prod_sbtype_cd
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''APLCTN_TYPE''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_job.Typecode''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys= ''GW''
                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')apltn_lkp
                                            ON        a.typecode_stg=apltn_lkp.src_idntftn_val
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val1 ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val1
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')src_sys_lkp
                                            ON        a.sys_src_cd=src_sys_lkp.src_idntftn_val1
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val2 ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val2
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''APLCTN_QUOT_TYPE''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_quotetype.typecode''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys= ''GW''
                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')quot_type_lkp
                                            ON        a.aplctn_quote_type=quot_type_lkp.src_idntftn_val2
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val3 ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val3
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''CHNL_TYPE''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''cctl_howreportedtype.typecode'',
                                                                                                             ''derived'',
                                                                                                             ''pctl_grouptype.typecode'')
                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                              ''DS'')
                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')chnl_type_lkp
                                            ON        chnl_type_lkp.src_idntftn_val3=a.typecode_grouptype_new
                                            left join
                                                      (
                                                               SELECT   aplctn.aplctn_id           AS aplctn_id,
                                                                        aplctn.aplctn_cmpltd_dttm  AS aplctn_cmpltd_dttm,
                                                                        aplctn.aplctn_recvd_dttm   AS aplctn_recvd_dttm,
                                                                        aplctn.agmt_objtv_type_cd  AS agmt_objtv_type_cd,
                                                                        aplctn.aplctn_quot_type_cd AS aplctn_quot_type_cd,
                                                                        aplctn.prod_grp_id         AS prod_grp_id,
                                                                        aplctn.prod_id             AS prod_id,
                                                                        aplctn.chnl_type_cd        AS chnl_type_cd,
                                                                        aplctn.host_aplctn_num     AS host_aplctn_num,
                                                                        aplctn.edw_strt_dttm       AS edw_strt_dttm,
                                                                        aplctn.edw_end_dttm        AS edw_end_dttm,
                                                                        aplctn.host_aplctn_id      AS host_aplctn_id,
                                                                        aplctn.src_sys_cd          AS src_sys_cd,
                                                                        aplctn.aplctn_type_cd      AS aplctn_type_cd,
                                                                        aplctn.trans_strt_dttm     AS trans_strt_dttm,
                                                                        aplctn.trans_end_dttm      AS trans_end_dttm
                                                               FROM     db_t_prod_core.aplctn qualify row_number () over (PARTITION BY host_aplctn_id,src_sys_cd ORDER BY edw_end_dttm DESC)=1 )tgt_lkp
                                            ON        o_aplctn_type_cd=tgt_lkp.aplctn_type_cd
                                            AND       jobnumber=tgt_lkp.host_aplctn_id
                                            AND       out_sys_src_cd=tgt_lkp.src_sys_cd
                                            left join
                                                      (
                                                             SELECT dir_aplctn.aplctn_id                    AS aplctn_id1,
                                                                    ltrim(rtrim(dir_aplctn.host_aplctn_id)) AS host_aplctn_id,
                                                                    dir_aplctn.vers_nbr                     AS vers_nbr,
                                                                    ltrim(rtrim(dir_aplctn.dir_type_val))   AS dir_type_val,
                                                                    ltrim(rtrim(dir_aplctn.aplctn_type_cd)) AS aplctn_type_cd,
                                                                    ltrim(rtrim(dir_aplctn.src_sys_cd))     AS src_sys_cd
                                                             FROM   db_t_prod_core.dir_aplctn  )dir_lkp
                                            ON        dir_lkp.host_aplctn_id= in_host_aplctn_id
                                            AND       dir_lkp.aplctn_type_cd=o_aplctn_type_cd
                                            AND       dir_lkp.src_sys_cd =out_sys_src_cd
                                            WHERE     dir_lkp.vers_nbr IS NULL
                                            AND       dir_lkp.dir_type_val=''APLCTN'' )b ) src ) );
  -- Component exp_evaluatate_ins_upd_flags, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_evaluatate_ins_upd_flags AS
  (
         SELECT sq_pc_job.lkp_aplctn_id         AS lkp_aplctn_id,
                sq_pc_job.lkp_host_aplctn_id    AS lkp_host_aplctn_id,
                sq_pc_job.lkp_aplctn_type_cd    AS lkp_aplctn_type_cd,
                sq_pc_job.lkp_edw_strt_dt       AS lkp_edw_strt_dt,
                sq_pc_job.lkp_trans_strt_dt     AS lkp_trans_strt_dt,
                sq_pc_job.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
                sq_pc_job.lkp_sys_src_cd        AS lkp_sys_src_cd,
                sq_pc_job.in_host_aplctn_id     AS in_host_aplctn_id,
                sq_pc_job.in_aplctn_type_cd     AS in_aplctn_type_cd,
                sq_pc_job.in_aplctn_cmpltd_dt   AS in_aplctn_cmpltd_dt,
                sq_pc_job.in_aplctn_recvd_dt    AS in_aplctn_recvd_dt,
                sq_pc_job.in_agmt_objtv_type_cd AS in_agmt_objtv_type_cd,
                sq_pc_job.in_prod_grp_id        AS in_prod_grp_id,
                sq_pc_job.in_prod_id            AS in_prod_id,
                $prcs_id                        AS out_prcs_id,
                sq_pc_job.edw_strt_dttm         AS edw_strt_dttm,
                sq_pc_job.trans_strt_dttm       AS trans_strt_dttm,
                sq_pc_job.edw_end_dttm          AS edw_end_dttm,
                sq_pc_job.trans_end_dttm        AS trans_end_dttm,
                sq_pc_job.in_sys_src_cd         AS in_sys_src_cd,
                sq_pc_job.in_aplctn_quot_type   AS in_aplctn_quot_type,
                sq_pc_job.chnl_type_cd          AS chnl_type_cd,
                sq_pc_job.retired               AS retired,
                sq_pc_job.in_policynumber1      AS in_policynumber1,
                sq_pc_job.in_sys_src_cd11       AS in_sys_src_cd11,
                sq_pc_job.out_flag              AS out_flag,
                sq_pc_job.aplctn_id             AS aplctn_id,
                sq_pc_job.source_record_id
         FROM   sq_pc_job );
  -- Component rtr_aplctn_insupd_INSERT, Type ROUTER Output Group INSERT
  create  or replace temporary table rtr_aplctn_insupd_INSERT as
  SELECT exp_evaluatate_ins_upd_flags.lkp_aplctn_id         AS lkp_aplctn_id,
         exp_evaluatate_ins_upd_flags.lkp_host_aplctn_id    AS lkp_host_aplctn_id,
         exp_evaluatate_ins_upd_flags.lkp_aplctn_type_cd    AS lkp_aplctn_type_cd,
         exp_evaluatate_ins_upd_flags.lkp_edw_strt_dt       AS lkp_edw_strt_dt,
         exp_evaluatate_ins_upd_flags.lkp_trans_strt_dt     AS lkp_trans_strt_dt,
         exp_evaluatate_ins_upd_flags.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_evaluatate_ins_upd_flags.lkp_sys_src_cd        AS lkp_sys_src_cd,
         exp_evaluatate_ins_upd_flags.in_host_aplctn_id     AS in_host_aplctn_id,
         exp_evaluatate_ins_upd_flags.in_aplctn_type_cd     AS in_aplctn_type_cd,
         exp_evaluatate_ins_upd_flags.in_aplctn_cmpltd_dt   AS in_aplctn_cmpltd_dt,
         exp_evaluatate_ins_upd_flags.in_aplctn_recvd_dt    AS in_aplctn_recvd_dt,
         exp_evaluatate_ins_upd_flags.in_agmt_objtv_type_cd AS in_agmt_objtv_type_cd,
         exp_evaluatate_ins_upd_flags.in_prod_grp_id        AS in_prod_grp_id,
         exp_evaluatate_ins_upd_flags.in_prod_id            AS in_prod_id,
         exp_evaluatate_ins_upd_flags.out_prcs_id           AS out_prcs_id,
         exp_evaluatate_ins_upd_flags.edw_strt_dttm         AS edw_strt_dttm,
         exp_evaluatate_ins_upd_flags.trans_strt_dttm       AS trans_strt_dttm,
         exp_evaluatate_ins_upd_flags.edw_end_dttm          AS edw_end_dttm,
         exp_evaluatate_ins_upd_flags.trans_end_dttm        AS trans_end_dttm,
         exp_evaluatate_ins_upd_flags.in_sys_src_cd         AS in_sys_src_cd,
         exp_evaluatate_ins_upd_flags.in_aplctn_quot_type   AS in_aplctn_quot_type_cd,
         exp_evaluatate_ins_upd_flags.chnl_type_cd          AS chnl_type_cd,
         exp_evaluatate_ins_upd_flags.retired               AS retired,
         exp_evaluatate_ins_upd_flags.in_policynumber1      AS in_policynumber,
         exp_evaluatate_ins_upd_flags.in_sys_src_cd11       AS in_sys_src_cd11,
         exp_evaluatate_ins_upd_flags.out_flag              AS o_flag,
         exp_evaluatate_ins_upd_flags.aplctn_id             AS aplctn_id1,
         exp_evaluatate_ins_upd_flags.source_record_id
  FROM   exp_evaluatate_ins_upd_flags
  WHERE  exp_evaluatate_ins_upd_flags.out_flag = ''I''
  OR     (
                exp_evaluatate_ins_upd_flags.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_evaluatate_ins_upd_flags.retired = 0 );
  
  -- Component rtr_aplctn_insupd_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_aplctn_insupd_RETIRE as
  SELECT exp_evaluatate_ins_upd_flags.lkp_aplctn_id         AS lkp_aplctn_id,
         exp_evaluatate_ins_upd_flags.lkp_host_aplctn_id    AS lkp_host_aplctn_id,
         exp_evaluatate_ins_upd_flags.lkp_aplctn_type_cd    AS lkp_aplctn_type_cd,
         exp_evaluatate_ins_upd_flags.lkp_edw_strt_dt       AS lkp_edw_strt_dt,
         exp_evaluatate_ins_upd_flags.lkp_trans_strt_dt     AS lkp_trans_strt_dt,
         exp_evaluatate_ins_upd_flags.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_evaluatate_ins_upd_flags.lkp_sys_src_cd        AS lkp_sys_src_cd,
         exp_evaluatate_ins_upd_flags.in_host_aplctn_id     AS in_host_aplctn_id,
         exp_evaluatate_ins_upd_flags.in_aplctn_type_cd     AS in_aplctn_type_cd,
         exp_evaluatate_ins_upd_flags.in_aplctn_cmpltd_dt   AS in_aplctn_cmpltd_dt,
         exp_evaluatate_ins_upd_flags.in_aplctn_recvd_dt    AS in_aplctn_recvd_dt,
         exp_evaluatate_ins_upd_flags.in_agmt_objtv_type_cd AS in_agmt_objtv_type_cd,
         exp_evaluatate_ins_upd_flags.in_prod_grp_id        AS in_prod_grp_id,
         exp_evaluatate_ins_upd_flags.in_prod_id            AS in_prod_id,
         exp_evaluatate_ins_upd_flags.out_prcs_id           AS out_prcs_id,
         exp_evaluatate_ins_upd_flags.edw_strt_dttm         AS edw_strt_dttm,
         exp_evaluatate_ins_upd_flags.trans_strt_dttm       AS trans_strt_dttm,
         exp_evaluatate_ins_upd_flags.edw_end_dttm          AS edw_end_dttm,
         exp_evaluatate_ins_upd_flags.trans_end_dttm        AS trans_end_dttm,
         exp_evaluatate_ins_upd_flags.in_sys_src_cd         AS in_sys_src_cd,
         exp_evaluatate_ins_upd_flags.in_aplctn_quot_type   AS in_aplctn_quot_type_cd,
         exp_evaluatate_ins_upd_flags.chnl_type_cd          AS chnl_type_cd,
         exp_evaluatate_ins_upd_flags.retired               AS retired,
         exp_evaluatate_ins_upd_flags.in_policynumber1      AS in_policynumber,
         exp_evaluatate_ins_upd_flags.in_sys_src_cd11       AS in_sys_src_cd11,
         exp_evaluatate_ins_upd_flags.out_flag              AS o_flag,
         exp_evaluatate_ins_upd_flags.aplctn_id             AS aplctn_id1,
         exp_evaluatate_ins_upd_flags.source_record_id
  FROM   exp_evaluatate_ins_upd_flags
  WHERE  exp_evaluatate_ins_upd_flags.out_flag = ''R''
  AND    exp_evaluatate_ins_upd_flags.retired != 0
  AND    exp_evaluatate_ins_upd_flags.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_aplctn_insupd_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_aplctn_insupd_UPDATE as
  SELECT exp_evaluatate_ins_upd_flags.lkp_aplctn_id         AS lkp_aplctn_id,
         exp_evaluatate_ins_upd_flags.lkp_host_aplctn_id    AS lkp_host_aplctn_id,
         exp_evaluatate_ins_upd_flags.lkp_aplctn_type_cd    AS lkp_aplctn_type_cd,
         exp_evaluatate_ins_upd_flags.lkp_edw_strt_dt       AS lkp_edw_strt_dt,
         exp_evaluatate_ins_upd_flags.lkp_trans_strt_dt     AS lkp_trans_strt_dt,
         exp_evaluatate_ins_upd_flags.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_evaluatate_ins_upd_flags.lkp_sys_src_cd        AS lkp_sys_src_cd,
         exp_evaluatate_ins_upd_flags.in_host_aplctn_id     AS in_host_aplctn_id,
         exp_evaluatate_ins_upd_flags.in_aplctn_type_cd     AS in_aplctn_type_cd,
         exp_evaluatate_ins_upd_flags.in_aplctn_cmpltd_dt   AS in_aplctn_cmpltd_dt,
         exp_evaluatate_ins_upd_flags.in_aplctn_recvd_dt    AS in_aplctn_recvd_dt,
         exp_evaluatate_ins_upd_flags.in_agmt_objtv_type_cd AS in_agmt_objtv_type_cd,
         exp_evaluatate_ins_upd_flags.in_prod_grp_id        AS in_prod_grp_id,
         exp_evaluatate_ins_upd_flags.in_prod_id            AS in_prod_id,
         exp_evaluatate_ins_upd_flags.out_prcs_id           AS out_prcs_id,
         exp_evaluatate_ins_upd_flags.edw_strt_dttm         AS edw_strt_dttm,
         exp_evaluatate_ins_upd_flags.trans_strt_dttm       AS trans_strt_dttm,
         exp_evaluatate_ins_upd_flags.edw_end_dttm          AS edw_end_dttm,
         exp_evaluatate_ins_upd_flags.trans_end_dttm        AS trans_end_dttm,
         exp_evaluatate_ins_upd_flags.in_sys_src_cd         AS in_sys_src_cd,
         exp_evaluatate_ins_upd_flags.in_aplctn_quot_type   AS in_aplctn_quot_type_cd,
         exp_evaluatate_ins_upd_flags.chnl_type_cd          AS chnl_type_cd,
         exp_evaluatate_ins_upd_flags.retired               AS retired,
         exp_evaluatate_ins_upd_flags.in_policynumber1      AS in_policynumber,
         exp_evaluatate_ins_upd_flags.in_sys_src_cd11       AS in_sys_src_cd11,
         exp_evaluatate_ins_upd_flags.out_flag              AS o_flag,
         exp_evaluatate_ins_upd_flags.aplctn_id             AS aplctn_id1,
         exp_evaluatate_ins_upd_flags.source_record_id
  FROM   exp_evaluatate_ins_upd_flags
  WHERE  exp_evaluatate_ins_upd_flags.out_flag = ''U''
  AND    exp_evaluatate_ins_upd_flags.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component exp_pass_to_tgt_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_update AS
  (
         SELECT rtr_aplctn_insupd_update.lkp_aplctn_id                       AS lkp_aplctn_id3,
                rtr_aplctn_insupd_update.lkp_host_aplctn_id                  AS lkp_host_aplctn_id3,
                rtr_aplctn_insupd_update.lkp_aplctn_type_cd                  AS lkp_aplctn_type_cd3,
                rtr_aplctn_insupd_update.edw_strt_dttm                       AS edw_strt_dt3,
                rtr_aplctn_insupd_update.lkp_edw_strt_dt                     AS lkp_edw_strt_dt3,
                rtr_aplctn_insupd_update.lkp_sys_src_cd                      AS lkp_sys_src_cd3,
                dateadd(''second'', - 1, rtr_aplctn_insupd_update.edw_strt_dttm)   AS out_edw_end_dttm,
                dateadd(''second'', - 1, rtr_aplctn_insupd_update.trans_strt_dttm) AS out_trans_strt_dttm,
                rtr_aplctn_insupd_update.source_record_id
         FROM   rtr_aplctn_insupd_update );
  -- Component exp_pass_to_tgt_upd_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd_ins AS
  (
         SELECT rtr_aplctn_insupd_update.in_host_aplctn_id      AS host_aplctn_id,
                rtr_aplctn_insupd_update.in_aplctn_type_cd      AS aplctn_type_cd,
                rtr_aplctn_insupd_update.in_aplctn_cmpltd_dt    AS aplctn_cmpltd_dt,
                rtr_aplctn_insupd_update.in_aplctn_recvd_dt     AS aplctn_recvd_dt,
                rtr_aplctn_insupd_update.in_agmt_objtv_type_cd  AS agmt_objtv_type_cd,
                rtr_aplctn_insupd_update.in_prod_grp_id         AS prod_grp_id,
                rtr_aplctn_insupd_update.in_prod_id             AS prod_id,
                rtr_aplctn_insupd_update.out_prcs_id            AS prcs_id,
                rtr_aplctn_insupd_update.in_sys_src_cd          AS in_sys_src_cd1,
                rtr_aplctn_insupd_update.in_aplctn_quot_type_cd AS in_aplctn_quot_type_cd1,
                rtr_aplctn_insupd_update.chnl_type_cd           AS chnl_type_cd1,
                rtr_aplctn_insupd_update.in_policynumber        AS in_policynumber1,
                rtr_aplctn_insupd_update.edw_strt_dttm          AS edw_strt_dttm,
                CASE
                       WHEN rtr_aplctn_insupd_update.retired = 0 THEN rtr_aplctn_insupd_update.edw_end_dttm
                       ELSE rtr_aplctn_insupd_update.edw_strt_dttm
                END                                      AS out_edw_end_dttm,
                rtr_aplctn_insupd_update.trans_strt_dttm AS trans_strt_dttm,
                CASE
                       WHEN rtr_aplctn_insupd_update.retired = 0 THEN rtr_aplctn_insupd_update.trans_end_dttm
                       ELSE rtr_aplctn_insupd_update.trans_strt_dttm
                END                                 AS out_trans_end_dttm,
                rtr_aplctn_insupd_update.aplctn_id1 AS out_aplctn_id,
                rtr_aplctn_insupd_update.source_record_id
         FROM   rtr_aplctn_insupd_update );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT rtr_aplctn_insupd_insert.in_host_aplctn_id      AS host_aplctn_id,
                rtr_aplctn_insupd_insert.in_aplctn_type_cd      AS aplctn_type_cd,
                rtr_aplctn_insupd_insert.in_aplctn_cmpltd_dt    AS aplctn_cmpltd_dt,
                rtr_aplctn_insupd_insert.in_aplctn_recvd_dt     AS aplctn_recvd_dt,
                rtr_aplctn_insupd_insert.in_agmt_objtv_type_cd  AS agmt_objtv_type_cd,
                rtr_aplctn_insupd_insert.in_prod_grp_id         AS prod_grp_id,
                rtr_aplctn_insupd_insert.in_prod_id             AS prod_id,
                rtr_aplctn_insupd_insert.out_prcs_id            AS prcs_id,
                rtr_aplctn_insupd_insert.in_sys_src_cd          AS in_sys_src_cd1,
                rtr_aplctn_insupd_insert.in_aplctn_quot_type_cd AS in_aplctn_quot_type_cd1,
                rtr_aplctn_insupd_insert.chnl_type_cd           AS chnl_type_cd1,
                rtr_aplctn_insupd_insert.in_policynumber        AS in_policynumber1,
                rtr_aplctn_insupd_insert.edw_strt_dttm          AS edw_strt_dttm,
                CASE
                       WHEN rtr_aplctn_insupd_insert.retired = 0 THEN rtr_aplctn_insupd_insert.edw_end_dttm
                       ELSE rtr_aplctn_insupd_insert.edw_strt_dttm
                END                                      AS out_edw_end_dttm,
                rtr_aplctn_insupd_insert.trans_strt_dttm AS trans_strt_dttm,
                CASE
                       WHEN rtr_aplctn_insupd_insert.retired = 0 THEN rtr_aplctn_insupd_insert.trans_end_dttm
                       ELSE rtr_aplctn_insupd_insert.trans_strt_dttm
                END                                 AS out_trans_end_dttm,
                rtr_aplctn_insupd_insert.aplctn_id1 AS out_aplctn_id,
                rtr_aplctn_insupd_insert.source_record_id
         FROM   rtr_aplctn_insupd_insert );
  -- Component upd_aplctn_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_ins.host_aplctn_id          AS host_aplctn_id,
                exp_pass_to_tgt_ins.aplctn_type_cd          AS aplctn_type_cd,
                exp_pass_to_tgt_ins.aplctn_cmpltd_dt        AS aplctn_cmpltd_dt,
                exp_pass_to_tgt_ins.aplctn_recvd_dt         AS aplctn_recvd_dt,
                exp_pass_to_tgt_ins.agmt_objtv_type_cd      AS agmt_objtv_type_cd,
                exp_pass_to_tgt_ins.prod_grp_id             AS prod_grp_id,
                exp_pass_to_tgt_ins.prod_id                 AS prod_id,
                exp_pass_to_tgt_ins.prcs_id                 AS prcs_id,
                exp_pass_to_tgt_ins.edw_strt_dttm           AS edw_strt_dttm,
                exp_pass_to_tgt_ins.out_edw_end_dttm        AS edw_end_dttm,
                exp_pass_to_tgt_ins.in_sys_src_cd1          AS in_sys_src_cd1,
                exp_pass_to_tgt_ins.in_aplctn_quot_type_cd1 AS in_aplctn_quot_type_cd1,
                exp_pass_to_tgt_ins.chnl_type_cd1           AS chnl_type_cd1,
                exp_pass_to_tgt_ins.in_policynumber1        AS in_policynumber1,
                exp_pass_to_tgt_ins.out_trans_end_dttm      AS trans_end_dttm,
                exp_pass_to_tgt_ins.trans_strt_dttm         AS trans_strt_dttm,
                exp_pass_to_tgt_ins.out_aplctn_id           AS aplctn_id,
                0                                           AS update_strategy_action,
                exp_pass_to_tgt_ins.source_record_id
         FROM   exp_pass_to_tgt_ins );
  -- Component upd_aplctn_update, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_update AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_update.lkp_aplctn_id3      AS lkp_aplctn_id,
                exp_pass_to_tgt_update.lkp_host_aplctn_id3 AS lkp_host_aplctn_id3,
                exp_pass_to_tgt_update.lkp_aplctn_type_cd3 AS lkp_aplctn_type_cd3,
                exp_pass_to_tgt_update.edw_strt_dt3        AS edw_strt_dt3,
                exp_pass_to_tgt_update.lkp_edw_strt_dt3    AS lkp_edw_strt_dt3,
                exp_pass_to_tgt_update.lkp_sys_src_cd3     AS lkp_sys_src_cd3,
                exp_pass_to_tgt_update.out_edw_end_dttm    AS out_edw_end_dttm,
                exp_pass_to_tgt_update.out_trans_strt_dttm AS out_trans_end_dttm,
                1                                          AS update_strategy_action,
                exp_pass_to_tgt_update.source_record_id
         FROM   exp_pass_to_tgt_update );
  -- Component exp_pass_to_tgt_retire, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_retire AS
  (
         SELECT rtr_aplctn_insupd_retire.lkp_aplctn_id      AS lkp_aplctn_id3,
                rtr_aplctn_insupd_retire.lkp_host_aplctn_id AS lkp_host_aplctn_id3,
                rtr_aplctn_insupd_retire.lkp_aplctn_type_cd AS lkp_aplctn_type_cd3,
                rtr_aplctn_insupd_retire.lkp_edw_strt_dt    AS lkp_edw_strt_dt3,
                rtr_aplctn_insupd_retire.lkp_sys_src_cd     AS lkp_sys_src_cd3,
                rtr_aplctn_insupd_retire.out_prcs_id        AS out_prcs_id3,
                current_timestamp                           AS out_edw_end_dttm,
                rtr_aplctn_insupd_retire.trans_strt_dttm    AS out_trans_strt_dttm,
                rtr_aplctn_insupd_retire.source_record_id
         FROM   rtr_aplctn_insupd_retire );
  -- Component upd_aplctn_ins_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_ins_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_upd_ins.host_aplctn_id          AS host_aplctn_id,
                exp_pass_to_tgt_upd_ins.aplctn_type_cd          AS aplctn_type_cd,
                exp_pass_to_tgt_upd_ins.aplctn_cmpltd_dt        AS aplctn_cmpltd_dt,
                exp_pass_to_tgt_upd_ins.aplctn_recvd_dt         AS aplctn_recvd_dt,
                exp_pass_to_tgt_upd_ins.agmt_objtv_type_cd      AS agmt_objtv_type_cd,
                exp_pass_to_tgt_upd_ins.prod_grp_id             AS prod_grp_id,
                exp_pass_to_tgt_upd_ins.prod_id                 AS prod_id,
                exp_pass_to_tgt_upd_ins.prcs_id                 AS prcs_id,
                exp_pass_to_tgt_upd_ins.edw_strt_dttm           AS edw_strt_dttm,
                exp_pass_to_tgt_upd_ins.out_edw_end_dttm        AS edw_end_dttm,
                exp_pass_to_tgt_upd_ins.in_sys_src_cd1          AS in_sys_src_cd1,
                exp_pass_to_tgt_upd_ins.in_aplctn_quot_type_cd1 AS in_aplctn_quot_type_cd1,
                exp_pass_to_tgt_upd_ins.chnl_type_cd1           AS chnl_type_cd1,
                exp_pass_to_tgt_upd_ins.in_policynumber1        AS in_policynumber1,
                exp_pass_to_tgt_upd_ins.out_trans_end_dttm      AS trans_end_dttm,
                exp_pass_to_tgt_upd_ins.trans_strt_dttm         AS trans_strt_dttm,
                exp_pass_to_tgt_upd_ins.out_aplctn_id           AS aplctn_id,
                0                                               AS update_strategy_action,
                exp_pass_to_tgt_upd_ins.source_record_id
         FROM   exp_pass_to_tgt_upd_ins );
  -- Component tgt_APLCTN_ins, Type TARGET
  INSERT INTO db_t_prod_core.aplctn
              (
                          aplctn_id,
                          aplctn_cmpltd_dttm,
                          aplctn_recvd_dttm,
                          prod_grp_id,
                          aplctn_type_cd,
                          agmt_objtv_type_cd,
                          host_aplctn_id,
                          host_aplctn_num,
                          aplctn_quot_type_cd,
                          prcs_id,
                          prod_id,
                          chnl_type_cd,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT upd_aplctn_ins.aplctn_id               AS aplctn_id,
         upd_aplctn_ins.aplctn_cmpltd_dt        AS aplctn_cmpltd_dttm,
         upd_aplctn_ins.aplctn_recvd_dt         AS aplctn_recvd_dttm,
         upd_aplctn_ins.prod_grp_id             AS prod_grp_id,
         upd_aplctn_ins.aplctn_type_cd          AS aplctn_type_cd,
         upd_aplctn_ins.agmt_objtv_type_cd      AS agmt_objtv_type_cd,
         upd_aplctn_ins.host_aplctn_id          AS host_aplctn_id,
         upd_aplctn_ins.in_policynumber1        AS host_aplctn_num,
         upd_aplctn_ins.in_aplctn_quot_type_cd1 AS aplctn_quot_type_cd,
         upd_aplctn_ins.prcs_id                 AS prcs_id,
         upd_aplctn_ins.prod_id                 AS prod_id,
         upd_aplctn_ins.chnl_type_cd1           AS chnl_type_cd,
         upd_aplctn_ins.in_sys_src_cd1          AS src_sys_cd,
         upd_aplctn_ins.edw_strt_dttm           AS edw_strt_dttm,
         upd_aplctn_ins.edw_end_dttm            AS edw_end_dttm,
         upd_aplctn_ins.trans_strt_dttm         AS trans_strt_dttm,
         upd_aplctn_ins.trans_end_dttm          AS trans_end_dttm
  FROM   upd_aplctn_ins;
  
  -- Component upd_aplctn_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_aplctn_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_retire.lkp_aplctn_id3      AS lkp_aplctn_id,
                exp_pass_to_tgt_retire.lkp_host_aplctn_id3 AS lkp_host_aplctn_id3,
                exp_pass_to_tgt_retire.lkp_aplctn_type_cd3 AS lkp_aplctn_type_cd3,
                exp_pass_to_tgt_retire.lkp_edw_strt_dt3    AS lkp_edw_strt_dt3,
                exp_pass_to_tgt_retire.lkp_sys_src_cd3     AS lkp_sys_src_cd3,
                exp_pass_to_tgt_retire.out_prcs_id3        AS out_prcs_id3,
                exp_pass_to_tgt_retire.out_edw_end_dttm    AS edw_end_dttm,
                exp_pass_to_tgt_retire.out_trans_strt_dttm AS trans_strt_dttm,
                1                                          AS update_strategy_action,
                exp_pass_to_tgt_retire.source_record_id
         FROM   exp_pass_to_tgt_retire );
  -- Component tgt_APLCTN_update, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.aplctn
  USING        upd_aplctn_update
  ON (
                            update_strategy_action = 1
               AND          aplctn.aplctn_id = upd_aplctn_update.lkp_aplctn_id
               AND          aplctn.aplctn_type_cd = upd_aplctn_update.lkp_aplctn_type_cd3
               AND          aplctn.host_aplctn_id = upd_aplctn_update.lkp_host_aplctn_id3
               AND          aplctn.src_sys_cd = upd_aplctn_update.lkp_sys_src_cd3
               AND          aplctn.edw_strt_dttm = upd_aplctn_update.lkp_edw_strt_dt3)
  WHEN matched THEN
  UPDATE
  SET    edw_end_dttm = upd_aplctn_update.out_edw_end_dttm,
         trans_end_dttm = upd_aplctn_update.out_trans_end_dttm ;
  
  -- Component tgt_APLCTN_update, Type Post SQL
  /*UPDATE   B  FROM  APLCTN B,
(SELECT distinct  APLCTN_ID,APLCTN_TYPE_CD,SRC_SYS_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM, max(EDW_STRT_DTTM) over (partition by  APLCTN_ID,APLCTN_TYPE_CD,SRC_SYS_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1'' SECOND
as lead1
,max(TRANS_STRT_DTTM) over (partition by  APLCTN_ID,APLCTN_TYPE_CD,SRC_SYS_CD ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following)  - INTERVAL ''1'' SECOND  as lead2
FROM APLCTN
) a
set EDW_END_DTTM=A.lead1
, TRANS_END_DTTM=a.lead2
where  B.EDW_STRT_DTTM = A.EDW_STRT_DTTM
and B.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM
and B.APLCTN_ID=A.APLCTN_ID
and B.APLCTN_TYPE_CD=A.APLCTN_TYPE_CD
AND B.SRC_SYS_CD=A.SRC_SYS_CD
and CAST(B.EDW_END_DTTM AS DATE)=''9999-12-31''
and CAST(B.TRANS_END_DTTM AS DATE)=''9999-12-31''
and lead1 is not null and lead2 is not null*/
--  ;
  -- Component tgt_APLCTN_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.aplctn
              (
                          aplctn_id,
                          aplctn_cmpltd_dttm,
                          aplctn_recvd_dttm,
                          prod_grp_id,
                          aplctn_type_cd,
                          agmt_objtv_type_cd,
                          host_aplctn_id,
                          host_aplctn_num,
                          aplctn_quot_type_cd,
                          prcs_id,
                          prod_id,
                          chnl_type_cd,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT upd_aplctn_ins_upd.aplctn_id               AS aplctn_id,
         upd_aplctn_ins_upd.aplctn_cmpltd_dt        AS aplctn_cmpltd_dttm,
         upd_aplctn_ins_upd.aplctn_recvd_dt         AS aplctn_recvd_dttm,
         upd_aplctn_ins_upd.prod_grp_id             AS prod_grp_id,
         upd_aplctn_ins_upd.aplctn_type_cd          AS aplctn_type_cd,
         upd_aplctn_ins_upd.agmt_objtv_type_cd      AS agmt_objtv_type_cd,
         upd_aplctn_ins_upd.host_aplctn_id          AS host_aplctn_id,
         upd_aplctn_ins_upd.in_policynumber1        AS host_aplctn_num,
         upd_aplctn_ins_upd.in_aplctn_quot_type_cd1 AS aplctn_quot_type_cd,
         upd_aplctn_ins_upd.prcs_id                 AS prcs_id,
         upd_aplctn_ins_upd.prod_id                 AS prod_id,
         upd_aplctn_ins_upd.chnl_type_cd1           AS chnl_type_cd,
         upd_aplctn_ins_upd.in_sys_src_cd1          AS src_sys_cd,
         upd_aplctn_ins_upd.edw_strt_dttm           AS edw_strt_dttm,
         upd_aplctn_ins_upd.edw_end_dttm            AS edw_end_dttm,
         upd_aplctn_ins_upd.trans_strt_dttm         AS trans_strt_dttm,
         upd_aplctn_ins_upd.trans_end_dttm          AS trans_end_dttm
  FROM   upd_aplctn_ins_upd;
  
  -- Component tgt_APLCTN_retire, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.aplctn
  USING        upd_aplctn_retire
  ON (
                            update_strategy_action = 1
               AND          aplctn.aplctn_id = upd_aplctn_retire.lkp_aplctn_id
               AND          aplctn.aplctn_type_cd = upd_aplctn_retire.lkp_aplctn_type_cd3
               AND          aplctn.host_aplctn_id = upd_aplctn_retire.lkp_host_aplctn_id3
               AND          aplctn.src_sys_cd = upd_aplctn_retire.lkp_sys_src_cd3
               AND          aplctn.edw_strt_dttm = upd_aplctn_retire.lkp_edw_strt_dt3)
  WHEN matched THEN
  UPDATE
  SET    edw_end_dttm = upd_aplctn_retire.edw_end_dttm,
         trans_end_dttm = upd_aplctn_retire.trans_strt_dttm ;
  
  -- Component tgt_APLCTN_retire, Type Post SQL
  /*UPDATE   B  FROM  APLCTN B,
(SELECT distinct  APLCTN_ID,APLCTN_TYPE_CD,SRC_SYS_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM, max(EDW_STRT_DTTM) over (partition by  APLCTN_ID,APLCTN_TYPE_CD,SRC_SYS_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1'' SECOND
as lead1
,max(TRANS_STRT_DTTM) over (partition by  APLCTN_ID,APLCTN_TYPE_CD,SRC_SYS_CD ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following)  - INTERVAL ''1'' SECOND  as lead2
FROM APLCTN
) a
set EDW_END_DTTM=A.lead1
, TRANS_END_DTTM=a.lead2
where  B.EDW_STRT_DTTM = A.EDW_STRT_DTTM
and B.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM
and B.APLCTN_ID=A.APLCTN_ID
and B.APLCTN_TYPE_CD=A.APLCTN_TYPE_CD
AND B.SRC_SYS_CD=A.SRC_SYS_CD
and CAST(B.EDW_END_DTTM AS DATE)=''9999-12-31''
and CAST(B.TRANS_END_DTTM AS DATE)=''9999-12-31''
and lead1 is not null and lead2 is not null*/
--  ;
END;
';