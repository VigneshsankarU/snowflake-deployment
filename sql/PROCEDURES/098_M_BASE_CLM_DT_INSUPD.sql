-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_DT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component sq_cc_claim, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_claim AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS clm_id,
                $2  AS createdate,
                $3  AS createdatetype,
                $4  AS retired,
                $5  AS trans_strt_dttm,
                $6  AS rnk,
                $7  AS ins_upd_flag,
                $8  AS lkp_edw_strt_dttm,
                $9  AS lkp_edw_end_dttm,
                $10 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT clm_id,
                                                createdate,
                                                createdatetype,
                                                retired,
                                                trans_strt_dttm,
                                                rnk,
                                                ins_upd_flag,
                                                lkp_edw_strt_dttm,
                                                lkp_edw_end_dttm
                                         FROM   (
                                                                SELECT          clm_dt_lkp.clm_id         AS lkp_clm_id,
                                                                                clm_dt_lkp.clm_dt_type_cd AS lkp_clm_dt_type_cd,
                                                                                clm_dt_lkp.clm_dttm       AS lkp_clm_dttm,
                                                                                xlat_src.clm_id           AS clm_id,
                                                                                xlat_src.createdate       AS createdate,
                                                                                xlat_src.createdatetype   AS createdatetype,
                                                                                xlat_src.retired          AS retired,
                                                                                xlat_src.trans_strt_dttm  AS trans_strt_dttm,
                                                                                xlat_src.rnk              AS rnk,
                                                                                /* SourceData */
                                                                                cast(trim(to_char(cast(xlat_src.createdate AS timestamp))) AS VARCHAR(1500)) AS sourcedata,
                                                                                /* TargetData */
                                                                                cast(trim(to_char(cast(clm_dt_lkp.clm_dttm AS timestamp))) AS VARCHAR(1500)) AS targetdata,
                                                                                /* Flag */
                                                                                CASE
                                                                                                WHEN targetdata IS NULL THEN ''I''
                                                                                                WHEN targetdata IS NOT NULL
                                                                                                AND             sourcedata <> targetdata THEN ''U''
                                                                                                WHEN targetdata IS NOT NULL
                                                                                                AND             sourcedata = targetdata THEN ''R''
                                                                                END                      AS ins_upd_flag,
                                                                                clm_dt_lkp.edw_strt_dttm AS lkp_edw_strt_dttm,
                                                                                clm_dt_lkp.edw_end_dttm  AS lkp_edw_end_dttm
                                                                FROM            (
                                                                                                SELECT          clm_lkp.clm_id,
                                                                                                                src.lossdate                   AS createdate,
                                                                                                                xlat_clm_dt_cd.tgt_idntftn_val AS createdatetype,
                                                                                                                src.retired,
                                                                                                                src.rnk,
                                                                                                                coalesce(src.trans_strt_dttm, to_timestamp_ntz(''1900-01-01 00:00:00.000000'', ''MM/DD/YYYY HH:MI:SS.S(6)'')) AS trans_strt_dttm
                                                                                                FROM            (
                                                                                                                         SELECT   claimnumber_stg AS claimnumber,
                                                                                                                                  lossdate_stg    AS lossdate,
                                                                                                                                  typecode,
                                                                                                                                  clm_src_cd,
                                                                                                                                  retired,
                                                                                                                                  trans_strt_dttm ,
                                                                                                                                  rank() over(PARTITION BY claimnumber_stg,typecode ORDER BY lossdate_stg ) AS rnk
                                                                                                                         FROM     (
                                                                                                                                         SELECT cc_claim.claimnumber_stg,
                                                                                                                                                cc_claim.lossdate_stg,
                                                                                                                                                ''CLM_DT_TYPE11'' AS typecode,
                                                                                                                                                ''SRC_SYS6''      AS clm_src_cd,
                                                                                                                                                retired_stg     AS retired,
                                                                                                                                                updatetime_stg  AS trans_strt_dttm
                                                                                                                                         FROM   (
                                                                                                                                                           SELECT     cc_claim.claimnumber_stg,
                                                                                                                                                                      cc_claim.lossdate_stg,
                                                                                                                                                                      cc_claim.retired_stg,
                                                                                                                                                                      cc_claim.updatetime_stg
                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                         WHERE  cc_claim.lossdate_stg IS NOT NULL
                                                                                                                                         AND    cc_claim.updatetime_stg>($start_dttm)
                                                                                                                                         AND    cc_claim.updatetime_stg <= ($end_dttm)
                                                                                                                                         UNION
                                                                                                                                         SELECT cc_claim.claimnumber_stg,
                                                                                                                                                cc_claim.reporteddate_stg,
                                                                                                                                                ''CLM_DT_TYPE16'' AS typecode,
                                                                                                                                                ''SRC_SYS6''      AS clm_src_cd,
                                                                                                                                                retired_stg     AS retired,
                                                                                                                                                updatetime_stg  AS trans_strt_dttm
                                                                                                                                         FROM   (
                                                                                                                                                           SELECT     cc_claim.claimnumber_stg,
                                                                                                                                                                      cc_claim.reporteddate_stg,
                                                                                                                                                                      cc_claim.retired_stg,
                                                                                                                                                                      cc_claim.updatetime_stg
                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                         WHERE  cc_claim.reporteddate_stg IS NOT NULL
                                                                                                                                         AND    cc_claim.updatetime_stg>($start_dttm)
                                                                                                                                         AND    cc_claim.updatetime_stg <= ($end_dttm)
                                                                                                                                         UNION
                                                                                                                                         SELECT cc_claim.claimnumber_stg,
                                                                                                                                                cc_claim.closedate_stg,
                                                                                                                                                ''CLM_DT_TYPE3'' AS typecode,
                                                                                                                                                ''SRC_SYS6''     AS clm_src_cd,
                                                                                                                                                retired_stg    AS retired,
                                                                                                                                                updatetime_stg AS trans_strt_dttm
                                                                                                                                         FROM   (
                                                                                                                                                           SELECT     cc_claim.claimnumber_stg,
                                                                                                                                                                      cc_claim.closedate_stg,
                                                                                                                                                                      cc_claim.retired_stg,
                                                                                                                                                                      cc_claim.updatetime_stg
                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                         WHERE  cc_claim.closedate_stg IS NOT NULL
                                                                                                                                         AND    cc_claim.updatetime_stg>($start_dttm)
                                                                                                                                         AND    cc_claim.updatetime_stg <= ($end_dttm)
                                                                                                                                         UNION
                                                                                                                                         SELECT   cc_activity.claimnumber,
                                                                                                                                                  min(cc_activity.closedate),
                                                                                                                                                  ''CLM_DT_TYPE22''     AS typecode,
                                                                                                                                                  ''SRC_SYS6''          AS clm_src_cd,
                                                                                                                                                  cc_activity.retired AS retired,
                                                                                                                                                  updatetime          AS trans_strt_dttm
                                                                                                                                         FROM     (
                                                                                                                                                         SELECT cc_claim.claimnumber_stg   AS claimnumber,
                                                                                                                                                                cc_activity.closedate_stg  AS closedate,
                                                                                                                                                                cc_activity.retired_stg    AS retired,
                                                                                                                                                                cc_activity.updatetime_stg AS updatetime,
                                                                                                                                                                cc_activity.subject_stg    AS subject
                                                                                                                                                         FROM   db_t_prod_stag.cc_activity
                                                                                                                                                         join   db_t_prod_stag.cc_claim
                                                                                                                                                         ON     cc_claim.id_stg = cc_activity.claimid_stg
                                                                                                                                                         WHERE  cc_activity.updatetime_stg>($start_dttm)
                                                                                                                                                         AND    cc_activity.updatetime_stg <= ($end_dttm) ) cc_activity
                                                                                                                                         WHERE    cc_activity.subject = ''Make initial contact with Claimant''
                                                                                                                                         AND      cc_activity.closedate IS NOT NULL
                                                                                                                                         GROUP BY cc_activity.claimnumber ,
                                                                                                                                                  cc_activity.retired,
                                                                                                                                                  updatetime
                                                                                                                                         UNION
                                                                                                                                         SELECT cc_claim.claimnumber_stg,
                                                                                                                                                cc_claim.reopendate_stg,
                                                                                                                                                ''CLM_DT_TYPE15'' AS typecode,
                                                                                                                                                ''SRC_SYS6''      AS clm_src_cd,
                                                                                                                                                retired_stg     AS retired,
                                                                                                                                                updatetime_stg  AS trans_strt_dttm
                                                                                                                                         FROM   (
                                                                                                                                                           SELECT     cc_claim.claimnumber_stg,
                                                                                                                                                                      cc_claim.reopendate_stg,
                                                                                                                                                                      cc_claim.retired_stg,
                                                                                                                                                                      cc_claim.updatetime_stg
                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                         WHERE  cc_claim.reopendate_stg IS NOT NULL
                                                                                                                                         AND    cc_claim.updatetime_stg>($start_dttm)
                                                                                                                                         AND    cc_claim.updatetime_stg <= ($end_dttm)
                                                                                                                                         UNION
                                                                                                                                         SELECT cc_claim.claimnumber_stg,
                                                                                                                                                cc_claim.createtime_stg,
                                                                                                                                                ''CLM_DT_TYPE12'' AS typecode,
                                                                                                                                                ''SRC_SYS6''      AS clm_src_cd ,
                                                                                                                                                retired_stg     AS retired,
                                                                                                                                                updatetime_stg  AS trans_strt_dttm
                                                                                                                                         FROM   (
                                                                                                                                                           SELECT     cc_claim.claimnumber_stg,
                                                                                                                                                                      cc_claim.createtime_stg,
                                                                                                                                                                      cc_claim.retired_stg,
                                                                                                                                                                      cc_claim.updatetime_stg
                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                         WHERE  cc_claim.createtime_stg IS NOT NULL
                                                                                                                                         AND    cc_claim.updatetime_stg>($start_dttm)
                                                                                                                                         AND    cc_claim.updatetime_stg <= ($end_dttm)
                                                                                                                                         UNION
                                                                                                                                         SELECT   cc_incident.claimnumber,
                                                                                                                                                  min(cc_incident.assessmentclosedate),
                                                                                                                                                  ''CLM_DT_TYPE10''     AS typecode,
                                                                                                                                                  ''SRC_SYS6''          AS clm_src_cd,
                                                                                                                                                  cc_incident.retired AS retired,
                                                                                                                                                  updatetime          AS trans_strt_dttm
                                                                                                                                         FROM     (
                                                                                                                                                             SELECT     cc_claim.claimnumber_stg            AS claimnumber,
                                                                                                                                                                        cc_incident.assessmentclosedate_stg AS assessmentclosedate,
                                                                                                                                                                        cc_incident.retired_stg             AS retired,
                                                                                                                                                                        cc_incident.updatetime_stg          AS updatetime
                                                                                                                                                             FROM       db_t_prod_stag.cc_incident
                                                                                                                                                             inner join
                                                                                                                                                                        (
                                                                                                                                                                                   SELECT     cc_claim.*
                                                                                                                                                                                   FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                                   inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                                   ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                                   WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                                             ON         cc_claim.id_stg=cc_incident.claimid_stg
                                                                                                                                                             WHERE      cc_incident.updatetime_stg <= ($end_dttm)
                                                                                                                                                             AND        cc_incident.updatetime_stg > ($start_dttm) ) cc_incident
                                                                                                                                         WHERE    cc_incident.assessmentclosedate IS NOT NULL
                                                                                                                                         GROUP BY cc_incident.claimnumber ,
                                                                                                                                                  cc_incident.retired,
                                                                                                                                                  updatetime
                                                                                                                                         UNION
                                                                                                                                         SELECT   cc_statuelimitationsline.claimnumber,
                                                                                                                                                  min(cc_statuelimitationsline.statutedate),
                                                                                                                                                  ''CLM_DT_TYPE18''                  AS typecode,
                                                                                                                                                  ''SRC_SYS6''                       AS clm_src_cd,
                                                                                                                                                  cc_statuelimitationsline.retired AS retired,
                                                                                                                                                  updatetime                       AS trans_strt_dttm
                                                                                                                                         FROM     (
                                                                                                                                                                  SELECT          cc_claim.claimnumber_stg                 AS claimnumber,
                                                                                                                                                                                  cc_statuelimitationsline.statutedate_stg AS statutedate,
                                                                                                                                                                                  cc_statuelimitationsline.retired_stg     AS retired,
                                                                                                                                                                                  cc_statuelimitationsline.updatetime_stg  AS updatetime
                                                                                                                                                                  FROM            db_t_prod_stag.cc_statuelimitationsline
                                                                                                                                                                  left outer join db_t_prod_stag.cc_subrogationsummary
                                                                                                                                                                  ON              cc_statuelimitationsline.subrogationsummaryid_stg=cc_subrogationsummary.id_stg
                                                                                                                                                                  inner join
                                                                                                                                                                                  (
                                                                                                                                                                                             SELECT     cc_claim.*
                                                                                                                                                                                             FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                                             inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                                             ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                                                  ON              cc_subrogationsummary.claimid_stg=cc_claim.id_stg
                                                                                                                                                                  WHERE           cc_statuelimitationsline.updatetime_stg> ($start_dttm)
                                                                                                                                                                  AND             cc_statuelimitationsline.updatetime_stg <= ($end_dttm) ) cc_statuelimitationsline
                                                                                                                                         WHERE    cc_statuelimitationsline.statutedate IS NOT NULL
                                                                                                                                         GROUP BY cc_statuelimitationsline.claimnumber ,
                                                                                                                                                  cc_statuelimitationsline.retired,
                                                                                                                                                  updatetime
                                                                                                                                                  /*UNION
Select distinct cc_history_x.ClaimNumber,  cc_history_x.EventTimestamp ,''CLM_DT_TYPE24'' as Typecode,
''SRC_SYS6'' as CLM_SRC_CD,
1 as retired,EventTimestamp as Trans_strt_dttm
from
(select cc_claim.claimnumber_stg as claimnumber,cc_history.EventTimestamp_stg as EventTimestamp,cctl_historytype.typecode_stg as HistoryType_Typecode
from DB_T_PROD_STAG.cc_history
inner join (
select cc_claim.* from DB_T_PROD_STAG.cc_claim
inner join DB_T_PROD_STAG.cctl_claimstate
on cc_claim.State_stg= cctl_claimstate.id_stg
where cctl_claimstate.name_stg <> ''Draft'') DB_T_PROD_STAG.cc_claim
on cc_history.ClaimID_stg=cc_claim.id_stg
inner join DB_T_PROD_STAG.cctl_historytype
on cctl_historytype.id_stg = cc_history.type_stg
) cc_history_x
where cc_history_x.HistoryType_Typecode=''viewing''*/
                                                                                                                                  ) AS tmp ) AS src
                                                                                                left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_src_cd
                                                                                                ON              xlat_src_cd.src_idntftn_val = src.clm_src_cd
                                                                                                AND             xlat_src_cd.tgt_idntftn_nm= ''SRC_SYS''
                                                                                                AND             xlat_src_cd.src_idntftn_nm= ''derived''
                                                                                                AND             xlat_src_cd.src_idntftn_sys=''DS''
                                                                                                AND             xlat_src_cd.expn_dt=''9999-12-31''
                                                                                                left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_clm_dt_cd
                                                                                                ON              xlat_clm_dt_cd.src_idntftn_val = src.typecode
                                                                                                AND             xlat_clm_dt_cd.tgt_idntftn_nm= ''CLM_DT_TYPE''
                                                                                                AND             xlat_clm_dt_cd.src_idntftn_nm= ''derived''
                                                                                                AND             xlat_clm_dt_cd.src_idntftn_sys=''DS''
                                                                                                AND             xlat_clm_dt_cd.expn_dt=''9999-12-31''
                                                                                                left outer join
                                                                                                                (
                                                                                                                         SELECT   clm_id ,
                                                                                                                                  clm_type_cd ,
                                                                                                                                  clm_mdia_type_cd ,
                                                                                                                                  clm_submtl_type_cd ,
                                                                                                                                  acdnt_type_cd ,
                                                                                                                                  clm_ctgy_type_cd ,
                                                                                                                                  addl_insrnc_pln_ind ,
                                                                                                                                  emplmt_rltd_ind ,
                                                                                                                                  attny_invlvmt_ind ,
                                                                                                                                  clm_prir_ind ,
                                                                                                                                  pmt_mode_cd ,
                                                                                                                                  clm_oblgtn_type_cd ,
                                                                                                                                  subrgtn_elgbl_cd ,
                                                                                                                                  subrgtn_elgbly_rsn_cd ,
                                                                                                                                  cury_cd,
                                                                                                                                  incdt_ev_id ,
                                                                                                                                  insrd_at_fault_ind ,
                                                                                                                                  cvge_in_ques_ind ,
                                                                                                                                  extnt_of_fire_dmg_type_cd ,
                                                                                                                                  vfyd_clm_ind ,
                                                                                                                                  prcs_id ,
                                                                                                                                  clm_strt_dttm ,
                                                                                                                                  clm_end_dttm ,
                                                                                                                                  edw_strt_dttm ,
                                                                                                                                  edw_end_dttm ,
                                                                                                                                  trans_strt_dttm ,
                                                                                                                                  lgcy_clm_num ,
                                                                                                                                  clm_num ,
                                                                                                                                  src_sys_cd
                                                                                                                         FROM     db_t_prod_core.clm qualify row_number() over(PARTITION BY clm_num,src_sys_cd ORDER BY edw_end_dttm DESC) = 1 ) AS clm_lkp
                                                                                                ON              clm_lkp.clm_num = src.claimnumber
                                                                                                AND             clm_lkp.src_sys_cd = xlat_src_cd.tgt_idntftn_val) AS xlat_src
                                                                left outer join
                                                                                (
                                                                                         SELECT   clm_dt.clm_dttm       AS clm_dttm,
                                                                                                  clm_dt.edw_strt_dttm  AS edw_strt_dttm,
                                                                                                  clm_dt.edw_end_dttm   AS edw_end_dttm,
                                                                                                  clm_dt.clm_id         AS clm_id,
                                                                                                  clm_dt.clm_dt_type_cd AS clm_dt_type_cd
                                                                                         FROM     db_t_prod_core.clm_dt qualify row_number() over(PARTITION BY clm_dt.clm_id,clm_dt.clm_dt_type_cd ORDER BY clm_dt.edw_end_dttm DESC) = 1 ) AS clm_dt_lkp
                                                                ON              clm_dt_lkp.clm_id = xlat_src.clm_id
                                                                AND             clm_dt_lkp.clm_dt_type_cd = xlat_src.createdatetype ) a ) src ) );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
         SELECT sq_cc_claim.clm_id                                                     AS clm_id,
                sq_cc_claim.createdate                                                 AS createdate,
                sq_cc_claim.createdatetype                                             AS createdatetype,
                $prcs_id                                                               AS prcs_id,
                sq_cc_claim.ins_upd_flag                                               AS ins_upd_flag,
                sq_cc_claim.lkp_edw_strt_dttm                                          AS lkp_edw_strt_dttm,
                sq_cc_claim.lkp_edw_end_dttm                                           AS lkp_edw_end_dttm,
                current_timestamp                                                      AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                dateadd(''second'', - 1, current_timestamp)                                  AS edw_end_dttm_expiry,
                sq_cc_claim.retired                                                    AS retired,
                sq_cc_claim.rnk                                                        AS rnk,
                sq_cc_claim.trans_strt_dttm                                            AS trans_strt_dttm,
                sq_cc_claim.source_record_id
         FROM   sq_cc_claim );
  -- Component rtr_ins_upd_condition_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_ins_upd_condition_insert as
  SELECT exp_check_flag.clm_id              AS clm_id,
         exp_check_flag.createdate          AS createdate,
         exp_check_flag.createdatetype      AS createdatetype,
         exp_check_flag.prcs_id             AS prcs_id,
         exp_check_flag.ins_upd_flag        AS o_flag,
         exp_check_flag.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_check_flag.edw_strt_dttm       AS edw_strt_dttm,
         exp_check_flag.edw_end_dttm        AS edw_end_dttm,
         exp_check_flag.edw_end_dttm_expiry AS edw_end_dttm_expiry,
         exp_check_flag.retired             AS retired,
         exp_check_flag.rnk                 AS rnk,
         exp_check_flag.trans_strt_dttm     AS trans_strt_dttm,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.ins_upd_flag = ''I''
  AND    exp_check_flag.clm_id IS NOT NULL
  OR     exp_check_flag.ins_upd_flag = ''U''
  OR     (
                exp_check_flag.retired = 0
         AND    exp_check_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_ins_upd_condition_retired, Type ROUTER Output Group retired
  create or replace temporary table rtr_ins_upd_condition_retired as
  SELECT exp_check_flag.clm_id              AS clm_id,
         exp_check_flag.createdate          AS createdate,
         exp_check_flag.createdatetype      AS createdatetype,
         exp_check_flag.prcs_id             AS prcs_id,
         exp_check_flag.ins_upd_flag        AS o_flag,
         exp_check_flag.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_check_flag.edw_strt_dttm       AS edw_strt_dttm,
         exp_check_flag.edw_end_dttm        AS edw_end_dttm,
         exp_check_flag.edw_end_dttm_expiry AS edw_end_dttm_expiry,
         exp_check_flag.retired             AS retired,
         exp_check_flag.rnk                 AS rnk,
         exp_check_flag.trans_strt_dttm     AS trans_strt_dttm,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.ins_upd_flag = ''R''
  AND    exp_check_flag.retired != 0
  AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) /*-- > NOT
  INSERT
  OR
  UPDATE ,
         no CHANGE IN VALUES - - > but data IS retired - - >
  UPDATE these records WITH current_timestamp*/
  ;
  
  -- Component clm_dt_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE clm_dt_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_condition_retired.clm_id            AS clm_id1,
                rtr_ins_upd_condition_retired.lkp_edw_strt_dttm AS lkp_edw_strt_dttm1,
                rtr_ins_upd_condition_retired.createdatetype    AS createdatetype3,
                rtr_ins_upd_condition_retired.trans_strt_dttm   AS trans_strt_dttm3,
                1                                               AS update_strategy_action,
				rtr_ins_upd_condition_retired.source_record_id
         FROM   rtr_ins_upd_condition_retired );
  -- Component exp_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired AS
  (
         SELECT clm_dt_retired.clm_id1            AS clm_id1,
                clm_dt_retired.lkp_edw_strt_dttm1 AS lkp_edw_strt_dttm1,
                clm_dt_retired.createdatetype3    AS createdatetype3,
                current_timestamp                 AS o_edw_end_dttm,
                clm_dt_retired.trans_strt_dttm3   AS trans_strt_dttm3,
                clm_dt_retired.source_record_id
         FROM   clm_dt_retired );
  -- Component insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE
  ins_INSERT AS
         (
                /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
                SELECT rtr_ins_upd_condition_insert.clm_id              AS clm_id1,
                       rtr_ins_upd_condition_insert.createdate          AS createdate1,
                       rtr_ins_upd_condition_insert.createdatetype      AS createdatetype1,
                       rtr_ins_upd_condition_insert.prcs_id             AS prcs_id1,
                       rtr_ins_upd_condition_insert.o_flag              AS o_flag1,
                       rtr_ins_upd_condition_insert.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm1,
                       rtr_ins_upd_condition_insert.lkp_edw_end_dttm    AS lkp_edw_end_dttm1,
                       rtr_ins_upd_condition_insert.edw_strt_dttm       AS edw_strt_dttm1,
                       rtr_ins_upd_condition_insert.edw_end_dttm        AS edw_end_dttm1,
                       rtr_ins_upd_condition_insert.edw_end_dttm_expiry AS edw_end_dttm_expiry1,
                       rtr_ins_upd_condition_insert.retired             AS retired1,
                       rtr_ins_upd_condition_insert.rnk                 AS rnk1,
                       rtr_ins_upd_condition_insert.trans_strt_dttm     AS trans_strt_dttm1,
                       0                                                AS update_strategy_action,
					   rtr_ins_upd_condition_insert.source_record_id
                FROM   rtr_ins_upd_condition_insert
         );
  
  -- Component tgt_clm_dt_retired, Type TARGET
  merge
  INTO         db_t_prod_core.clm_dt
  USING        exp_retired
  ON (
                            clm_dt.clm_id = exp_retired.clm_id1
               AND          clm_dt.clm_dt_type_cd = exp_retired.createdatetype3
               AND          clm_dt.edw_strt_dttm = exp_retired.lkp_edw_strt_dttm1)
  WHEN matched THEN
  UPDATE
  SET    clm_id = exp_retired.clm_id1,
         clm_dt_type_cd = exp_retired.createdatetype3,
         edw_strt_dttm = exp_retired.lkp_edw_strt_dttm1,
         edw_end_dttm = exp_retired.o_edw_end_dttm,
         trans_end_dttm = exp_retired.trans_strt_dttm3;
  
  -- Component exp_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_insert AS
  (
         SELECT ins_INSERT.clm_id1         AS clm_id1,
                ins_INSERT.createdate1     AS createdate1,
                ins_INSERT.createdatetype1 AS createdatetype1,
                ins_INSERT.prcs_id1        AS prcs_id1,
                CASE
                       WHEN ins_INSERT.retired1 = 0 THEN ins_INSERT.edw_end_dttm1
                       ELSE current_timestamp
                END AS o_edw_end_dttm,
                CASE
                       WHEN ins_INSERT.retired1 = 0 THEN dateadd(''second'', ( 2 * ( ins_INSERT.rnk1 - 1 ) ), current_timestamp)
                       ELSE current_timestamp
                END                     AS edw_date,
                ins_INSERT.trans_strt_dttm1 AS trans_strt_dttm1,
                CASE
                       WHEN ins_INSERT.retired1 != 0 THEN ins_INSERT.trans_strt_dttm1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm,
                ins_INSERT.source_record_id
         FROM
         ins_INSERT );
  -- Component tgt_clm_dt_ins, Type TARGET
  INSERT INTO db_t_prod_core.clm_dt
              (
                          clm_id,
                          clm_dt_type_cd,
                          clm_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_insert.clm_id1          AS clm_id,
         exp_insert.createdatetype1  AS clm_dt_type_cd,
         exp_insert.createdate1      AS clm_dttm,
         exp_insert.prcs_id1         AS prcs_id,
         exp_insert.edw_date         AS edw_strt_dttm,
         exp_insert.o_edw_end_dttm   AS edw_end_dttm,
         exp_insert.trans_strt_dttm1 AS trans_strt_dttm,
         exp_insert.trans_end_dttm   AS trans_end_dttm
  FROM   exp_insert;
  
  -- Component tgt_clm_dt_ins, Type Post SQL
  /*UPDATE  CLM_DT  FROM
(
SELECT distinct CLM_ID,CLM_DT_TYPE_CD,EDW_STRT_DTTM, CLM_DTTM,TRANS_STRT_DTTM
FROM CLM_DT
WHERE EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_ID,CLM_DT_TYPE_CD  ORDER BY CLM_DTTM DESC) >1
)  A
SET EDW_END_DTTM= A.EDW_STRT_DTTM+ INTERVAL ''1'' SECOND
WHERE  CLM_DT.CLM_ID=A.CLM_ID AND CLM_DT.CLM_DT_TYPE_CD=A.CLM_DT_TYPE_CD
AND  CLM_DT.EDW_STRT_DTTM=A.EDW_STRT_DTTM
AND  CLM_DT.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'');*/
  UPDATE db_t_prod_core.clm_dt
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT clm_id,
                                         clm_dt_type_cd,
                                         edw_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY clm_id,clm_dt_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead,
                                         max(edw_strt_dttm) over (PARTITION BY clm_id,clm_dt_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.clm_dt ) a

  WHERE  clm_dt.edw_strt_dttm = a.edw_strt_dttm
  AND    clm_dt.clm_id=a.clm_id
  AND    clm_dt.clm_dt_type_cd=a.clm_dt_type_cd
  AND    clm_dt.trans_strt_dttm <>clm_dt.trans_end_dttm
  AND    lead IS NOT NULL;

END;
';