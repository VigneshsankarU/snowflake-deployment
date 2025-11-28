-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EV_STS_RSN_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component SQ_pc_ev_sts_rsn_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_ev_sts_rsn_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS ev_id,
                $2  AS ev_sts_type_cd,
                $3  AS ev_sts_rsn_cd,
                $4  AS ev_sts_rsn_strt_dttm,
                $5  AS ev_sts_rsn_end_dttm,
                $6  AS edw_strt_dttm,
                $7  AS edw_end_dttm,
                $8  AS ev_sts_rsn_ind,
                $9  AS updatetime,
                $10 AS o_rank,
                $11 AS out_cancel_flag,
                $12 AS tgt_ev_id,
                $13 AS tgt_ev_sts_type_cd,
                $14 AS tgt_ev_sts_rsn_cd,
                $15 AS tgt_ev_sts_rsn_strt_dttm,
                $16 AS tgt_edw_strt_dttm,
                $17 AS sourcedata,
                $18 AS targetdata,
                $19 AS ins_upd_flag,
                $20 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT k.ev_id                    AS ev_id,
                                                                  k.ev_sts_type_cd           AS ev_sts_type_cd,
                                                                  k.ev_sts_rsn_cd            AS ev_sts_rsn_cd,
                                                                  k.in_ev_sts_rsn_strt_dttm  AS ev_sts_rsn_strt_dttm,
                                                                  k.in_ev_sts_rsn_end_dttm   AS ev_sts_rsn_end_dttm,
                                                                  k.edw_strt_dttm            AS edw_strt_dttm,
                                                                  k.edw_end_dttm             AS edw_end_dttm,
                                                                  k.ev_sts_rsn_ind           AS ev_sts_rsn_ind,
                                                                  k.updatetime               AS updatetime,
                                                                  k.o_rank                   AS o_rank,
                                                                  k.out_cancel_flag          AS out_cancel_flag,
                                                                  k.tgt_ev_id                AS tgt_ev_id,
                                                                  k.tgt_ev_sts_type_cd       AS tgt_ev_sts_type_cd,
                                                                  k.tgt_ev_sts_rsn_cd        AS tgt_ev_sts_rsn_cd,
                                                                  k.tgt_ev_sts_rsn_strt_dttm AS tgt_ev_sts_rsn_strt_dttm,
                                                                  k.tgt_edw_strt_dttm        AS tgt_edw_strt_dttm,
                                                                  CASE
                                                                                  WHEN k.out_cancel_flag=''y'' THEN cast(to_char(cast(in_ev_sts_rsn_strt_dttm AS timestamp))
                                                                                                                  ||to_char(cast(in_ev_sts_rsn_end_dttm AS timestamp))
                                                                                                                  ||trim(coalesce(cast(ev_sts_rsn_ind AS VARCHAR(10)),''0'')) AS VARCHAR(100))
                                                                                  WHEN k.out_cancel_flag=''N'' THEN cast(trim(ev_sts_rsn_cd)
                                                                                                                  ||to_char(cast(in_ev_sts_rsn_strt_dttm AS timestamp))
                                                                                                                  ||to_char(cast(in_ev_sts_rsn_end_dttm AS timestamp))
                                                                                                                  ||trim(coalesce(cast(ev_sts_rsn_ind AS VARCHAR(10)),''0''))AS VARCHAR(100))
                                                                  END AS sourcedata,
                                                                  CASE
                                                                                  WHEN k.out_cancel_flag=''y'' THEN cast(to_char(cast(tgt_ev_sts_rsn_strt_dttm AS timestamp))
                                                                                                                  ||to_char(cast(tgt_ev_sts_rsn_end_dttm AS timestamp))
                                                                                                                  ||trim(coalesce(cast(tgt_ev_sts_rsn_ind AS VARCHAR(10)),''0'')) AS VARCHAR(100))
                                                                                  WHEN k.out_cancel_flag=''N'' THEN cast(trim(tgt_ev_sts_rsn_cd)
                                                                                                                  ||to_char(cast(tgt_ev_sts_rsn_strt_dttm AS timestamp))
                                                                                                                  ||to_char(cast(tgt_ev_sts_rsn_end_dttm AS timestamp))
                                                                                                                  ||trim(coalesce(cast(tgt_ev_sts_rsn_ind AS VARCHAR(10)),''0''))AS VARCHAR(100))
                                                                  END AS targetdata,
                                                                  CASE
                                                                                  WHEN targetdata IS NULL THEN ''I''
                                                                                  WHEN sourcedata=targetdata THEN ''R''
                                                                                  WHEN sourcedata<>targetdata THEN ''U''
                                                                  END AS ins_upd_flag
                                                  FROM            (
                                                                                  SELECT          tgt_ev_sts_rsn.ev_id                                                                                   AS tgt_ev_id,
                                                                                                  tgt_ev_sts_rsn.ev_sts_type_cd                                                                          AS tgt_ev_sts_type_cd,
                                                                                                  tgt_ev_sts_rsn.ev_sts_rsn_cd                                                                           AS tgt_ev_sts_rsn_cd,
                                                                                                  tgt_ev_sts_rsn.ev_sts_rsn_strt_dttm                                                                    AS tgt_ev_sts_rsn_strt_dttm,
                                                                                                  tgt_ev_sts_rsn.ev_sts_rsn_end_dttm                                                                     AS tgt_ev_sts_rsn_end_dttm,
                                                                                                  tgt_ev_sts_rsn.ev_sts_rsn_ind                                                                          AS tgt_ev_sts_rsn_ind,
                                                                                                  tgt_ev_sts_rsn.edw_strt_dttm                                                                           AS tgt_edw_strt_dttm,
                                                                                                  tgt_ev_sts_rsn.edw_end_dttm                                                                            AS tgt_edw_end_dttm,
                                                                                                  ev_lkp.ev_id                                                                                           AS ev_id,
                                                                                                  coalesce(xlat_ev_sts.tgt_idntftn_val,''UNK'')                                                            AS ev_sts_type_cd,
                                                                                                  coalesce(xlat_ev_sts_rsn.tgt_idntftn_val,''UNK'')                                                        AS ev_sts_rsn_cd,
                                                                                                  coalesce(src.busn_dt,to_timestamp_ntz(''01/01/1900 00:00:00.000000'' , ''MM/DD/YYYYBHH:MI:SS.S(6)''))AS in_ev_sts_rsn_strt_dttm,
                                                                                                  /* COALESCE(SRC.Busn_dt,CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP FORMAT ''MM/DD/YYYYBHH:MI:SS.S(6)''))as in_EV_STS_RSN_STRT_DTTM,    -- invalid date format */
                                                                                                  to_timestamp_ntz(''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYYBHH:MI:SS.S(6)'') AS in_ev_sts_rsn_end_dttm,
                                                                                                  cast(current_date AS timestamp)                                                   AS edw_strt_dttm,
                                                                                                  to_timestamp_ntz(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYYBHH:MI:SS.S(6)'') AS edw_end_dttm,
                                                                                                  src.ev_sts_rsn_ind                                                                AS ev_sts_rsn_ind,
                                                                                                  coalesce(src.ev_strt_dt,to_date(''01-01-1900'' ,''MM-DD-YYYY''))           AS updatetime,
                                                                                                  src.rk                                                                            AS o_rank,
                                                                                                  CASE
                                                                                                                  WHEN xlat_ev_acty_cd.tgt_idntftn_val=''CANCLTN'' THEN ''Y''
                                                                                                                  WHEN xlat_ev_acty_cd.tgt_idntftn_val<>''CANCLTN''
                                                                                                                  AND             xlat_ev_acty_cd.tgt_idntftn_val IS NOT NULL THEN ''N''
                                                                                                  END AS out_cancel_flag
                                                                                  FROM            (
                                                                                                           /* src */
                                                                                                           SELECT   x.ev_act_type_code,
                                                                                                                    x.key1,
                                                                                                                    x.SUBTYPE,
                                                                                                                    x.status,
                                                                                                                    cast(x.reason AS VARCHAR(255)) AS reason,
                                                                                                                    x.ev_strt_dt,
                                                                                                                    x.busn_dt,
                                                                                                                    x.ev_sts_rsn_ind,
                                                                                                                    rank() over (PARTITION BY key1, ev_act_type_code, SUBTYPE, status ORDER BY busn_dt, ev_strt_dt, reason COLLATE ''en-ci'') rk
                                                                                                           FROM     (
                                                                                                                                    SELECT DISTINCT cast (pctl_job.typecode_stg AS VARCHAR(50))       AS ev_act_type_code ,
                                                                                                                                                    pc_job.jobnumber                                  AS key1 ,
                                                                                                                                                    ''PLCYTRNS''                                        AS SUBTYPE ,
                                                                                                                                                    cast (typecode_policyperiodstatus AS VARCHAR(50)) AS status ,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN pctl_job.typecode_stg = ''Cancellation'' THEN typecode_cancelreason
                                                                                                                                                                    ELSE cast(coalesce( typecode_rejectreason,typecode_reinstatecode,typecode_nonrenewalcode,typecode_renewalcode) AS VARCHAR(50))
                                                                                                                                                    END AS reason,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN pctl_job.typecode_stg=''Cancellation'' THEN pc_updatetime
                                                                                                                                                                    WHEN (
                                                                                                                                                                                                    typecode_rejectreason IS NOT NULL
                                                                                                                                                                                    OR              typecode_reinstatecode IS NOT NULL) THEN pc_job.updatetime
                                                                                                                                                                    WHEN typecode_rejectreason IS NULL
                                                                                                                                                                    AND             typecode_reinstatecode IS NULL
                                                                                                                                                                    AND             typecode_nonrenewalcode IS NOT NULL THEN pc_policyterm_updatetime
                                                                                                                                                                    ELSE pc_job.updatetime
                                                                                                                                                    END AS ev_strt_dt,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN pctl_job.typecode_stg = ''Cancellation'' THEN updatetime_policyperiod
                                                                                                                                                                    ELSE pc_job.createtime
                                                                                                                                                    END                    AS busn_dt,
                                                                                                                                                    cast(''0'' AS VARCHAR(3))AS ev_sts_rsn_ind
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_job.updatetime_stg AS updatetime,
                                                                                                                                                                                    pc_job.subtype_stg    AS subtype_stg,
                                                                                                                                                                                    pc_job.jobnumber_stg  AS jobnumber,
                                                                                                                                                                                    pc_job.createtime_stg AS createtime,
                                                                                                                                                                                    CASE
                                                                                                                                                                                                    WHEN pctl_job.typecode_stg IN ( ''Submission'',
                                                                                                                                                                                                        ''Renewal'',
                                                                                                                                                                                                        ''Rewrite'',
                                                                                                                                                                                                        ''Issuance'' ) THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    WHEN pctl_job.typecode_stg = ''Cancellation'' THEN pc_policyperiod.cancellationdate_stg
                                                                                                                                                                                                    WHEN pctl_job.typecode_stg IN (''PolicyChange'',
                                                                                                                                                                                                        ''Reinstatement'') THEN pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                    END                                  AS updatetime_policyperiod ,
                                                                                                                                                                                    pctl_policyperiodstatus.typecode_stg AS typecode_policyperiodstatus ,
                                                                                                                                                                                    cancelreason.typecode_stg            AS typecode_cancelreason,
                                                                                                                                                                                    rejectreason.typecode_stg            AS typecode_rejectreason,
                                                                                                                                                                                    pctl_reinstatecode.typecode_stg      AS typecode_reinstatecode ,
                                                                                                                                                                                    pctl_renewalcode.typecode_stg        AS typecode_renewalcode,
                                                                                                                                                                                    pctl_nonrenewalcode.typecode_stg     AS typecode_nonrenewalcode ,
                                                                                                                                                                                    pc_policyperiod.updatetime_stg       AS pc_updatetime,
                                                                                                                                                                                    pc_policyperiod.policynumber_stg ,
                                                                                                                                                                                    pc_policyterm.updatetime_stg AS pc_policyterm_updatetime
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
                                                                                                                                                                                                    WHERE           quotematuritylevel_stg IN (2,3) ) vj
                                                                                                                                                                    ON              pc_job.id_stg=vj.jobid_stg
                                                                                                                                                                    WHERE           pc_policyperiod.updatetime_stg > (:START_DTTM)
                                                                                                                                                                    AND             pc_policyperiod.updatetime_stg <= (:END_DTTM)
                                                                                                                                                                    AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                                                                                                    AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                    AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL ) pc_job
                                                                                                                                    inner join      db_t_prod_stag.pctl_job 
                                                                                                                                    ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                    WHERE           pc_job.policynumber_stg IS NOT NULL
                                                                                                                                    AND             updatetime_policyperiod IS NOT NULL
                                                                                                                                    AND             status<>''TEMPORARY''
                                                                                                                                    AND             ((
                                                                                                                                                                                    pctl_job.typecode_stg = ''Cancellation'' )
                                                                                                                                                    OR              pctl_job.typecode_stg <> ''Cancellation'')
                                                                                                                                    UNION
                                                                                                                                    SELECT DISTINCT cast (jobtype AS VARCHAR(50))                     AS ev_act_type_code ,
                                                                                                                                                    jobnumber                                         AS key1 ,
                                                                                                                                                    ''PLCYTRNS''                                        AS SUBTYPE ,
                                                                                                                                                    cast (typecode_policyperiodstatus AS           VARCHAR(50)) AS status ,
                                                                                                                                                    cast(coalesce (typecode_cancelreason,''UNK'') AS VARCHAR(50)) AS reason ,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN jobtype = ''Cancellation'' THEN cancel_updatetime
                                                                                                                                                                    ELSE updatetime
                                                                                                                                                    END AS ev_strt_dt,
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN jobtype = ''Cancellation'' THEN ev_sts_rsn_strt_dttm
                                                                                                                                                                    ELSE createtime
                                                                                                                                                    END                     AS busn_dt,
                                                                                                                                                    cast(''1'' AS VARCHAR(3)) AS ev_sts_rsn_ind
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_job.jobnumber_stg                     AS jobnumber ,
                                                                                                                                                                                    pc_policyperiod.branchnumber_stg         AS branchnumber,
                                                                                                                                                                                    pc_policyperiod.cancellationdate_stg     AS ev_sts_rsn_strt_dttm,
                                                                                                                                                                                    pctl_job.typecode_stg                    AS jobtype,
                                                                                                                                                                                    pctl_policyperiodstatus.typecode_stg     AS typecode_policyperiodstatus,
                                                                                                                                                                                    pctl_reasoncode.typecode_stg             AS typecode_cancelreason,
                                                                                                                                                                                    pc_policyperiod.retired_stg              AS policy_retired,
                                                                                                                                                                                    pc_job.createtime_stg                    AS createtime,
                                                                                                                                                                                    pc_job.updatetime_stg                    AS updatetime,
                                                                                                                                                                                    pcx_cancelreasondesc_alfa.updatetime_stg AS cancel_updatetime
                                                                                                                                                                    FROM            db_t_prod_stag.pc_job
                                                                                                                                                                    left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                    ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                                                                                                    left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                    ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                                                                                                    inner join      db_t_prod_stag.pcx_cancelreasondesc_alfa
                                                                                                                                                                    ON              pc_job.id_stg=pcx_cancelreasondesc_alfa.cancellation_stg
                                                                                                                                                                    inner join      db_t_prod_stag.pctl_reasoncode
                                                                                                                                                                    ON              pcx_cancelreasondesc_alfa.cancellationreason_stg=pctl_reasoncode.id_stg
                                                                                                                                                                    inner join      db_t_prod_stag.pctl_job
                                                                                                                                                                    ON              pctl_job.id_stg = pc_job.subtype_stg
                                                                                                                                                                    WHERE           pc_job.updatetime_stg > (:START_DTTM)
                                                                                                                                                                    AND             pc_job.updatetime_stg <= (:END_DTTM)
                                                                                                                                                                    AND             pctl_policyperiodstatus.typecode_stg<>''Temporary'' 
                                                                                                                                                    ) pc_ev_sts_rsn_x
                                                                                                                                    WHERE           pc_ev_sts_rsn_x.jobnumber IS NOT NULL 
                                                                                                                    ) x 
                                                                                                                    qualify row_number() over(PARTITION BY key1, ev_act_type_code, SUBTYPE, status, reason, ev_sts_rsn_ind ORDER BY ev_strt_dt DESC, busn_dt DESC) = 1 
                                                                                        ) src /* end of src  */
                                                                                  join            db_t_prod_core.teradata_etl_ref_xlat AS xlat_ev_sts
                                                                                  ON              xlat_ev_sts.src_idntftn_val=src.status
                                                                                  AND             xlat_ev_sts.tgt_idntftn_nm= ''EV_STS_TYPE''
                                                                                  AND             xlat_ev_sts.src_idntftn_nm=''pctl_policyperiodstatus.TYPECODE''
                                                                                  AND             xlat_ev_sts.src_idntftn_sys=''GW''
                                                                                  AND             xlat_ev_sts.expn_dt=''9999-12-31''
                                                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_ev_sts_rsn
                                                                                  ON              xlat_ev_sts_rsn.src_idntftn_val=src.reason
                                                                                  AND             xlat_ev_sts_rsn.tgt_idntftn_nm= ''EV_STS_RSN_TYPE''
                                                                                  AND             xlat_ev_sts_rsn.src_idntftn_sys=''GW''
                                                                                  AND             xlat_ev_sts_rsn.expn_dt=''9999-12-31''
                                                                                  join            db_t_prod_core.teradata_etl_ref_xlat AS xlat_ev_acty_cd
                                                                                  ON              xlat_ev_acty_cd.src_idntftn_val=src.ev_act_type_code
                                                                                  AND             xlat_ev_acty_cd.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                                                  AND             xlat_ev_acty_cd.src_idntftn_nm  IN (''pctl_job.Typecode'' )
                                                                                  AND             xlat_ev_acty_cd.src_idntftn_sys IN (''GW'',
                                                                                                                                      ''DS'' )
                                                                                  AND             xlat_ev_acty_cd.expn_dt=''9999-12-31''
                                                                                  join            db_t_prod_core.ev AS ev_lkp
                                                                                  ON              ev_lkp.src_trans_id=src.key1
                                                                                  AND             ev_lkp.ev_sbtype_cd=SUBTYPE
                                                                                  AND             ev_lkp.ev_actvy_type_cd=xlat_ev_acty_cd.tgt_idntftn_val
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   ev_sts_rsn.ev_sts_rsn_strt_dttm         AS ev_sts_rsn_strt_dttm,
                                                                                                                    ev_sts_rsn.ev_sts_rsn_end_dttm          AS ev_sts_rsn_end_dttm,
                                                                                                                    rtrim(ltrim(ev_sts_rsn.ev_sts_rsn_ind)) AS ev_sts_rsn_ind,
                                                                                                                    ev_sts_rsn.edw_strt_dttm                AS edw_strt_dttm,
                                                                                                                    ev_sts_rsn.edw_end_dttm                 AS edw_end_dttm,
                                                                                                                    ev_sts_rsn.ev_id                        AS ev_id,
                                                                                                                    ev_sts_rsn.ev_sts_type_cd               AS ev_sts_type_cd,
                                                                                                                    ev_sts_rsn.ev_sts_rsn_cd                AS ev_sts_rsn_cd
                                                                                                           FROM     db_t_prod_core.ev_sts_rsn 
                                                                                                           join     db_t_prod_core.ev ev_new
                                                                                                           ON       ev_sts_rsn.ev_id=ev_new.ev_id
                                                                                                           AND      ev_new.edw_end_dttm =''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY ev_sts_rsn.ev_id, ev_sts_type_cd ,
                                                                                                                    CASE
                                                                                                                             WHEN (
                                                                                                                                               ev_actvy_type_cd=''CANCLTN'') THEN ev_sts_rsn_cd
                                                                                                                             ELSE 1
                                                                                                                    END ORDER BY ev_sts_rsn.edw_end_dttm DESC) = 1 )AS tgt_ev_sts_rsn
                                                                                  ON              ev_lkp.ev_id=tgt_ev_sts_rsn.ev_id
                                                                                  AND             xlat_ev_sts.tgt_idntftn_val=tgt_ev_sts_rsn.ev_sts_type_cd
                                                                                  AND             coalesce(ev_sts_rsn_cd,''UNK'')=
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  ev_lkp.ev_actvy_type_cd=''CANCLTN'' ) THEN coalesce(xlat_ev_sts_rsn. tgt_idntftn_val,''UNK'')
                                                                                                                  ELSE coalesce(ev_sts_rsn_cd,''UNK'')
                                                                                                  END )k ) src ) );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
         SELECT sq_pc_ev_sts_rsn_x.ev_id                    AS in_ev_id,
                sq_pc_ev_sts_rsn_x.ev_sts_type_cd           AS in_ev_sts_type_cd,
                sq_pc_ev_sts_rsn_x.ev_sts_rsn_cd            AS in_ev_sts_rsn_cd,
                :prcs_id                                    AS in_prcs_id,
                sq_pc_ev_sts_rsn_x.ev_sts_rsn_strt_dttm     AS in_ev_sts_rsn_strt_dttm,
                sq_pc_ev_sts_rsn_x.ev_sts_rsn_end_dttm      AS in_ev_sts_rsn_end_dttm,
                sq_pc_ev_sts_rsn_x.edw_strt_dttm            AS in_edw_strt_dttm,
                sq_pc_ev_sts_rsn_x.edw_end_dttm             AS in_edw_end_dttm,
                sq_pc_ev_sts_rsn_x.tgt_ev_id                AS lkp_ev_id,
                sq_pc_ev_sts_rsn_x.tgt_ev_sts_type_cd       AS lkp_ev_sts_type_cd,
                sq_pc_ev_sts_rsn_x.tgt_ev_sts_rsn_cd        AS lkp_ev_sts_rsn_cd,
                sq_pc_ev_sts_rsn_x.tgt_ev_sts_rsn_strt_dttm AS lkp_ev_sts_rsn_strt_dttm,
                sq_pc_ev_sts_rsn_x.tgt_edw_strt_dttm        AS lkp_edw_strt_dttm,
                NULL                                        AS newlookuprow,
                sq_pc_ev_sts_rsn_x.ev_sts_rsn_ind           AS in_ev_sts_rsn_ind,
                sq_pc_ev_sts_rsn_x.updatetime               AS updatetime,
                sq_pc_ev_sts_rsn_x.o_rank                   AS rank,
                sq_pc_ev_sts_rsn_x.ins_upd_flag             AS ins_upd_flag,
                sq_pc_ev_sts_rsn_x.source_record_id
         FROM   sq_pc_ev_sts_rsn_x );
  -- Component rtr_ev_sts_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_ev_sts_insert as
  SELECT exp_cdc_check.in_ev_id                 AS in_ev_id,
         exp_cdc_check.in_ev_sts_type_cd        AS in_ev_sts_type_cd,
         exp_cdc_check.in_ev_sts_rsn_cd         AS in_ev_sts_rsn_cd,
         exp_cdc_check.in_prcs_id               AS in_prcs_id,
         exp_cdc_check.in_ev_sts_rsn_strt_dttm  AS in_ev_sts_rsn_strt_dttm,
         exp_cdc_check.in_ev_sts_rsn_end_dttm   AS in_ev_sts_rsn_end_dttm,
         exp_cdc_check.in_edw_strt_dttm         AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm          AS in_edw_end_dttm,
         exp_cdc_check.lkp_ev_id                AS lkp_ev_id,
         exp_cdc_check.lkp_ev_sts_type_cd       AS lkp_ev_sts_type_cd,
         exp_cdc_check.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_cdc_check.ins_upd_flag             AS o_cdc_check,
         exp_cdc_check.newlookuprow             AS newlookuprow,
         exp_cdc_check.lkp_ev_sts_rsn_strt_dttm AS lkp_ev_sts_rsn_strt_dttm,
         exp_cdc_check.in_ev_sts_rsn_ind        AS ev_sts_rsn_ind,
         exp_cdc_check.updatetime               AS updatetime,
         exp_cdc_check.lkp_ev_sts_rsn_cd        AS lkp_ev_sts_rsn_cd,
         exp_cdc_check.rank                     AS rank,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  (
                exp_cdc_check.ins_upd_flag = ''I''
         OR     exp_cdc_check.ins_upd_flag = ''U'' )
  AND    exp_cdc_check.in_ev_id IS NOT NULL /*- - exp_cdc_check.newlookuprow = 1*/
  ;
  
  -- Component rtr_ev_sts_Update, Type ROUTER Output Group Update
  create or replace temporary table rtr_ev_sts_update as
  SELECT exp_cdc_check.in_ev_id                 AS in_ev_id,
         exp_cdc_check.in_ev_sts_type_cd        AS in_ev_sts_type_cd,
         exp_cdc_check.in_ev_sts_rsn_cd         AS in_ev_sts_rsn_cd,
         exp_cdc_check.in_prcs_id               AS in_prcs_id,
         exp_cdc_check.in_ev_sts_rsn_strt_dttm  AS in_ev_sts_rsn_strt_dttm,
         exp_cdc_check.in_ev_sts_rsn_end_dttm   AS in_ev_sts_rsn_end_dttm,
         exp_cdc_check.in_edw_strt_dttm         AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm          AS in_edw_end_dttm,
         exp_cdc_check.lkp_ev_id                AS lkp_ev_id,
         exp_cdc_check.lkp_ev_sts_type_cd       AS lkp_ev_sts_type_cd,
         exp_cdc_check.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_cdc_check.ins_upd_flag             AS o_cdc_check,
         exp_cdc_check.newlookuprow             AS newlookuprow,
         exp_cdc_check.lkp_ev_sts_rsn_strt_dttm AS lkp_ev_sts_rsn_strt_dttm,
         exp_cdc_check.in_ev_sts_rsn_ind        AS ev_sts_rsn_ind,
         exp_cdc_check.updatetime               AS updatetime,
         exp_cdc_check.lkp_ev_sts_rsn_cd        AS lkp_ev_sts_rsn_cd,
         exp_cdc_check.rank                     AS rank,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  1 = 2 /*- - exp_cdc_check.newlookuprow = 2 - - exp_cdc_check.ins_upd_flag = ''U'' */
  ;
  
  -- Component exp_ev_sts_rsn_insupd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ev_sts_rsn_insupd AS
  (
         SELECT rtr_ev_sts_update.in_ev_id                                                                 AS in_ev_id3,
                rtr_ev_sts_update.in_ev_sts_type_cd                                                        AS in_ev_sts_type_cd3,
                rtr_ev_sts_update.in_ev_sts_rsn_cd                                                         AS in_ev_sts_rsn_cd3,
                rtr_ev_sts_update.in_prcs_id                                                               AS in_prcs_id3,
                rtr_ev_sts_update.in_ev_sts_rsn_strt_dttm                                                  AS in_ev_sts_rsn_strt_dttm3,
                rtr_ev_sts_update.in_ev_sts_rsn_end_dttm                                                   AS in_ev_sts_rsn_end_dttm3,
                dateadd(''second'', ( 2 * ( rtr_ev_sts_update.rank - 1 ) ), rtr_ev_sts_update.lkp_edw_strt_dttm) AS out_edw_strt_dttm3,
                rtr_ev_sts_update.in_edw_end_dttm                                                          AS in_edw_end_dttm3,
                rtr_ev_sts_update.ev_sts_rsn_ind                                                           AS ev_sts_rsn_ind3,
                rtr_ev_sts_update.updatetime                                                               AS updatetime3,
                rtr_ev_sts_update.source_record_id
         FROM   rtr_ev_sts_update );
  -- Component upd_ev_sts_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_sts_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_sts_update.lkp_ev_id                AS lkp_ev_id3,
                rtr_ev_sts_update.lkp_ev_sts_type_cd       AS lkp_ev_sts_type_cd3,
                rtr_ev_sts_update.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm3,
                rtr_ev_sts_update.lkp_ev_sts_rsn_strt_dttm AS lkp_ev_sts_rsn_strt_dttm3,
                rtr_ev_sts_update.ev_sts_rsn_ind           AS ev_sts_rsn_ind3,
                rtr_ev_sts_update.updatetime               AS updatetime3,
                rtr_ev_sts_update.lkp_ev_sts_rsn_cd        AS lkp_ev_sts_rsn_cd3,
                rtr_ev_sts_update.in_edw_strt_dttm         AS in_edw_strt_dttm3,
                rtr_ev_sts_update.rank                     AS rank3,
                1                                          AS update_strategy_action,
                rtr_ev_sts_update.source_record_id
         FROM   rtr_ev_sts_update );
  -- Component exp_ev_sts_rsn_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ev_sts_rsn_insert AS
  (
         SELECT rtr_ev_sts_insert.in_ev_id                                                                AS in_ev_id1,
                rtr_ev_sts_insert.in_ev_sts_type_cd                                                       AS in_ev_sts_type_cd1,
                rtr_ev_sts_insert.in_ev_sts_rsn_cd                                                        AS in_ev_sts_rsn_cd1,
                rtr_ev_sts_insert.in_prcs_id                                                              AS in_prcs_id1,
                rtr_ev_sts_insert.in_ev_sts_rsn_strt_dttm                                                 AS in_ev_sts_rsn_strt_dttm1,
                rtr_ev_sts_insert.in_ev_sts_rsn_end_dttm                                                  AS in_ev_sts_rsn_end_dttm1,
                dateadd(''second'', ( 2 * ( rtr_ev_sts_insert.rank - 1 ) ), rtr_ev_sts_insert.in_edw_strt_dttm) AS out_edw_strt_dttm1,
                rtr_ev_sts_insert.in_edw_end_dttm                                                         AS in_edw_end_dttm1,
                rtr_ev_sts_insert.ev_sts_rsn_ind                                                          AS ev_sts_rsn_ind1,
                rtr_ev_sts_insert.updatetime                                                              AS updatetime1,
                rtr_ev_sts_insert.source_record_id
         FROM   rtr_ev_sts_insert );
  -- Component tgt_EV_STS_RSN_ins, Type TARGET
  INSERT INTO db_t_prod_core.ev_sts_rsn
              (
                          ev_id,
                          ev_sts_type_cd,
                          ev_sts_rsn_cd,
                          ev_sts_rsn_ind,
                          prcs_id,
                          ev_sts_rsn_strt_dttm,
                          ev_sts_rsn_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_ev_sts_rsn_insert.in_ev_id1                AS ev_id,
         exp_ev_sts_rsn_insert.in_ev_sts_type_cd1       AS ev_sts_type_cd,
         exp_ev_sts_rsn_insert.in_ev_sts_rsn_cd1        AS ev_sts_rsn_cd,
         exp_ev_sts_rsn_insert.ev_sts_rsn_ind1          AS ev_sts_rsn_ind,
         exp_ev_sts_rsn_insert.in_prcs_id1              AS prcs_id,
         exp_ev_sts_rsn_insert.in_ev_sts_rsn_strt_dttm1 AS ev_sts_rsn_strt_dttm,
         exp_ev_sts_rsn_insert.in_ev_sts_rsn_end_dttm1  AS ev_sts_rsn_end_dttm,
         exp_ev_sts_rsn_insert.out_edw_strt_dttm1       AS edw_strt_dttm,
         exp_ev_sts_rsn_insert.in_edw_end_dttm1         AS edw_end_dttm,
         exp_ev_sts_rsn_insert.updatetime1              AS trans_strt_dttm
  FROM   exp_ev_sts_rsn_insert;
  
  -- Component tgt_EV_STS_RSN_ins, Type Post SQL
  UPDATE db_t_prod_core.ev_sts_rsn
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT ev_id,
                                         ev_sts_type_cd,
                                         ev_sts_rsn_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ev_id, ev_sts_type_cd, ev_sts_rsn_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY ev_id, ev_sts_type_cd, ev_sts_rsn_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.ev_sts_rsn ) a

  WHERE  ev_sts_rsn.edw_strt_dttm = a.edw_strt_dttm
  AND    ev_sts_rsn.ev_id IN
                              (
                              SELECT DISTINCT ev_id
                              FROM            db_t_prod_core.ev
                              WHERE           ev_actvy_type_cd=''CANCLTN'')
  AND    ev_sts_rsn.ev_id=a.ev_id
  AND    ev_sts_rsn.ev_sts_type_cd=a.ev_sts_type_cd
  AND    ev_sts_rsn.ev_sts_rsn_cd=a.ev_sts_rsn_cd
  AND    ev_sts_rsn.trans_strt_dttm <>ev_sts_rsn.trans_end_dttm
  AND    lead IS NOT NULL;
  
  UPDATE db_t_prod_core.ev_sts_rsn
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT ev_id,
                                         ev_sts_type_cd,
                                         ev_sts_rsn_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ev_id, ev_sts_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY ev_id, ev_sts_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.ev_sts_rsn ) a

  WHERE  ev_sts_rsn.edw_strt_dttm = a.edw_strt_dttm
  AND    ev_sts_rsn.ev_id IN
                              (
                              SELECT DISTINCT ev_id
                              FROM            db_t_prod_core.ev
                              WHERE           ev_actvy_type_cd<>''CANCLTN'')
  AND    ev_sts_rsn.ev_id=a.ev_id
  AND    ev_sts_rsn.ev_sts_type_cd=a.ev_sts_type_cd
  AND    ev_sts_rsn.ev_sts_rsn_cd=a.ev_sts_rsn_cd
  AND    ev_sts_rsn.trans_strt_dttm <>ev_sts_rsn.trans_end_dttm
  AND    lead IS NOT NULL;
  
  -- Component exp_DateExpiry, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_dateexpiry AS
  (
         SELECT upd_ev_sts_upd.lkp_ev_id3                                                                  AS lkp_ev_id3,
                upd_ev_sts_upd.lkp_ev_sts_type_cd3                                                         AS lkp_ev_sts_type_cd3,
                upd_ev_sts_upd.lkp_edw_strt_dttm3                                                          AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, upd_ev_sts_upd.updatetime3)                                             AS o_trans_end_dttm,
                upd_ev_sts_upd.lkp_ev_sts_rsn_cd3                                                          AS lkp_ev_sts_rsn_cd3,
                dateadd(''second'', ( 2 * ( upd_ev_sts_upd.rank3 - 1 ) ) - 1, upd_ev_sts_upd.lkp_edw_strt_dttm3) AS out_edw_strt_dttm3,
                upd_ev_sts_upd.source_record_id
         FROM   upd_ev_sts_upd );
  -- Component tgt_EV_STS_RSN_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.ev_sts_rsn
              (
                          ev_id,
                          ev_sts_type_cd,
                          ev_sts_rsn_cd,
                          ev_sts_rsn_ind,
                          prcs_id,
                          ev_sts_rsn_strt_dttm,
                          ev_sts_rsn_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_ev_sts_rsn_insupd.in_ev_id3                AS ev_id,
         exp_ev_sts_rsn_insupd.in_ev_sts_type_cd3       AS ev_sts_type_cd,
         exp_ev_sts_rsn_insupd.in_ev_sts_rsn_cd3        AS ev_sts_rsn_cd,
         exp_ev_sts_rsn_insupd.ev_sts_rsn_ind3          AS ev_sts_rsn_ind,
         exp_ev_sts_rsn_insupd.in_prcs_id3              AS prcs_id,
         exp_ev_sts_rsn_insupd.in_ev_sts_rsn_strt_dttm3 AS ev_sts_rsn_strt_dttm,
         exp_ev_sts_rsn_insupd.in_ev_sts_rsn_end_dttm3  AS ev_sts_rsn_end_dttm,
         exp_ev_sts_rsn_insupd.out_edw_strt_dttm3       AS edw_strt_dttm,
         exp_ev_sts_rsn_insupd.in_edw_end_dttm3         AS edw_end_dttm,
         exp_ev_sts_rsn_insupd.updatetime3              AS trans_strt_dttm
  FROM   exp_ev_sts_rsn_insupd;
  
  -- Component tgt_EV_STS_RSN_upd, Type TARGET
  merge
  INTO         db_t_prod_core.ev_sts_rsn
  USING        exp_dateexpiry
  ON (
                            ev_sts_rsn.ev_id = exp_dateexpiry.lkp_ev_id3
               AND          ev_sts_rsn.ev_sts_type_cd = exp_dateexpiry.lkp_ev_sts_type_cd3
               AND          ev_sts_rsn.ev_sts_rsn_cd = exp_dateexpiry.lkp_ev_sts_rsn_cd3)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_dateexpiry.lkp_ev_id3,
         ev_sts_type_cd = exp_dateexpiry.lkp_ev_sts_type_cd3,
         ev_sts_rsn_cd = exp_dateexpiry.lkp_ev_sts_rsn_cd3,
         edw_strt_dttm = exp_dateexpiry.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_dateexpiry.out_edw_strt_dttm3,
         trans_end_dttm = exp_dateexpiry.o_trans_end_dttm;

END;
';