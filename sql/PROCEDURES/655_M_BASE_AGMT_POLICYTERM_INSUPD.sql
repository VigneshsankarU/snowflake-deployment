-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_POLICYTERM_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  prcs_id int;
BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'');


  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_DATA_SRC, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_data_src AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DATA_SRC_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_XREF_AGMNT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_xref_agmnt AS
  (
         SELECT dir_agmt.agmt_id                    AS agmt_id,
                ltrim(rtrim(dir_agmt.nk_src_key))   AS nk_src_key,
                dir_agmt.term_num                   AS term_num,
                ltrim(rtrim(dir_agmt.agmt_type_cd)) AS agmt_type_cd
         FROM   db_t_prod_core.dir_agmt
         WHERE  agmt_type_cd=''POLTRM'' );
  -- Component SQ_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS policynumber,
                $2  AS termnumber,
                $3  AS originaleffectivedate,
                $4  AS issuedate,
                $5  AS periodend,
                $6  AS agmt_type_cd,
                $7  AS periodstart,
                $8  AS retired,
                $9  AS updatetime,
                $10 AS paymenttype,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT pp.policynumber_stg               AS policynumber ,
                                                                  pp.termnumber_stg                 AS termnumber ,
                                                                  max(pp.originaleffectivedate_stg) AS originaleffectivedate ,
                                                                  max(pp.issuedate_stg)             AS issuedate ,
                                                                  max(pp.periodend_stg)             AS periodend ,
                                                                  ''AGMT_TYPE6'' ,
                                                                  max(periodstart_stg) AS periodstart ,
                                                                  max(pp.retired_stg)  AS retired ,
                                                                  CASE
                                                                                  WHEN max(pp.updatetime_stg)<max(pp.updatetime_bcpolicyperiod_stg) THEN max(pp.updatetime_bcpolicyperiod_stg)
                                                                                  ELSE max(pp.updatetime_stg)
                                                                  END                     AS updatetime ,
                                                                  max(pp.paymenttype_stg) AS paymenttype
                                                  FROM            (
                                                                                  SELECT DISTINCT pc_policyperiod.updatetime_stg,
                                                                                                  pc_policyperiod.retired_stg,
                                                                                                  pc_policyperiod.termnumber_stg,
                                                                                                  pc_policyperiod.periodstart_stg,
                                                                                                  pc_policyperiod.periodend_stg,
                                                                                                  pc_policyperiod.policynumber_stg,
                                                                                                  pc_policy.originaleffectivedate_stg,
                                                                                                  pc_policy.issuedate_stg,
                                                                                                  pc_policyperiod.status_stg,
                                                                                                  pc_policyperiod.mostrecentmodel_stg,
                                                                                                  aa.updatetime_bcpolicyperiod_stg,
                                                                                                  aa.paymenttype_stg
                                                                                  FROM            db_t_prod_stag.pc_policyperiod
                                                                                  left outer join db_t_prod_stag.pc_policy
                                                                                  ON              pc_policy.id_stg=pc_policyperiod.policyid_stg
                                                                                  left outer join
                                                                                                  (
                                                                                                         SELECT bc_plan.name_stg                 AS paymenttype_stg,
                                                                                                                bc_policyperiod.policynumber_stg AS policynumber_stg,
                                                                                                                bc_policyperiod.termnumber_stg   AS termnumber_stg,
                                                                                                                bc_policyperiod.updatetime_stg   AS updatetime_bcpolicyperiod_stg
                                                                                                         FROM   db_t_prod_stag.bc_policyperiod
                                                                                                         join   db_t_prod_stag.bc_plan
                                                                                                         ON     bc_policyperiod.paymentplanid_stg=bc_plan.id_stg ) aa
                                                                                  ON              aa.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                  AND             aa.termnumber_stg=pc_policyperiod.termnumber_stg
                                                                                  WHERE           pc_policyperiod.updatetime_stg > (:END_DTTM)
                                                                                  AND             pc_policyperiod.updatetime_stg <= (:END_DTTM) )pp
                                                  inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                  ON              pctl_policyperiodstatus.id_stg=pp.status_stg
                                                  WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                  AND             pp.mostrecentmodel_stg=1
                                                                  /* AND MostRecentModel_stg=''T'' */
                                                  AND             pp.policynumber_stg IS NOT NULL
                                                  GROUP BY        pp.policynumber_stg,
                                                                  pp.termnumber_stg,
                                                                  pp.paymenttype_stg ) src ) );
  -- Component exp_pass_frm_src, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_frm_src AS
  (
            SELECT    sq_pc_policyperiod.policynumber          AS policynumber,
                      sq_pc_policyperiod.termnumber            AS termnumber,
                      sq_pc_policyperiod.originaleffectivedate AS originaleffectivedate,
                      CASE
                                WHEN sq_pc_policyperiod.issuedate IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                ELSE sq_pc_policyperiod.issuedate
                      END                                           AS o_issuedate,
                      date_trunc(day, sq_pc_policyperiod.periodend) AS o_periodend,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE_CD */
                      END                            AS o_agmt_type_cd,
                      sq_pc_policyperiod.periodstart AS periodstart,
                      sq_pc_policyperiod.retired     AS retired,
                      sq_pc_policyperiod.updatetime  AS updatetime,
                      sq_pc_policyperiod.paymenttype AS paymenttype,
                      sq_pc_policyperiod.source_record_id,
                      row_number() over (PARTITION BY sq_pc_policyperiod.source_record_id ORDER BY sq_pc_policyperiod.source_record_id) AS rnk
            FROM      sq_pc_policyperiod
            left join lkp_teradata_etl_ref_xlat_agmt_type_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_pc_policyperiod.agmt_type_cd
            left join lkp_teradata_etl_ref_xlat_agmt_type_cd lkp_2
            ON        lkp_2.src_idntftn_val = sq_pc_policyperiod.agmt_type_cd qualify rnk = 1 );
  -- Component LKP_AGMT_POLTRM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_poltrm AS
  (
            SELECT    lkp.agmt_id,
                      lkp.agmt_opn_dttm,
                      lkp.agmt_plnd_expn_dttm,
                      lkp.agmt_signd_dttm,
                      lkp.pmt_pln_type_cd,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_pass_frm_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_busn_type_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.mortgagee_prem_pmt_ind ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_num ASC,lkp.modl_crtn_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.agmt_eff_dttm ASC,lkp.term_num ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.prior_insrnc_ind ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.nk_src_key ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_pass_frm_src
            left join
                      (
                               SELECT   agmt.agmt_id                     AS agmt_id,
                                        agmt.agmt_name                   AS agmt_name,
                                        agmt.agmt_opn_dttm               AS agmt_opn_dttm,
                                        agmt.agmt_cls_dttm               AS agmt_cls_dttm,
                                        agmt.agmt_plnd_expn_dttm         AS agmt_plnd_expn_dttm,
                                        agmt.agmt_signd_dttm             AS agmt_signd_dttm,
                                        agmt.agmt_legly_bindg_ind        AS agmt_legly_bindg_ind,
                                        agmt.agmt_src_cd                 AS agmt_src_cd,
                                        agmt.agmt_cur_sts_cd             AS agmt_cur_sts_cd,
                                        agmt.agmt_cur_sts_rsn_cd         AS agmt_cur_sts_rsn_cd,
                                        agmt.agmt_obtnd_cd               AS agmt_obtnd_cd,
                                        agmt.agmt_sbtype_cd              AS agmt_sbtype_cd,
                                        agmt.agmt_prcsg_dttm             AS agmt_prcsg_dttm,
                                        agmt.alt_agmt_name               AS alt_agmt_name,
                                        agmt.asset_liabty_cd             AS asset_liabty_cd,
                                        agmt.bal_shet_cd                 AS bal_shet_cd,
                                        agmt.stmt_cycl_cd                AS stmt_cycl_cd,
                                        agmt.stmt_ml_type_cd             AS stmt_ml_type_cd,
                                        agmt.prposl_id                   AS prposl_id,
                                        agmt.agmt_objtv_type_cd          AS agmt_objtv_type_cd,
                                        agmt.fincl_agmt_sbtype_cd        AS fincl_agmt_sbtype_cd,
                                        agmt.mkt_risk_type_cd            AS mkt_risk_type_cd,
                                        agmt.orignl_maturty_dt           AS orignl_maturty_dt,
                                        agmt.risk_expsr_mtgnt_sbtype_cd  AS risk_expsr_mtgnt_sbtype_cd,
                                        agmt.bnk_trd_bk_cd               AS bnk_trd_bk_cd,
                                        agmt.prcg_meth_sbtype_cd         AS prcg_meth_sbtype_cd,
                                        agmt.fincl_agmt_type_cd          AS fincl_agmt_type_cd,
                                        agmt.dy_cnt_bss_cd               AS dy_cnt_bss_cd,
                                        agmt.frst_prem_due_dt            AS frst_prem_due_dt,
                                        agmt.insrnc_agmt_sbtype_cd       AS insrnc_agmt_sbtype_cd,
                                        agmt.insrnc_busn_type_cd         AS insrnc_busn_type_cd,
                                        agmt.insrnc_agmt_type_cd         AS insrnc_agmt_type_cd,
                                        agmt.ntwk_srvc_agmt_type_cd      AS ntwk_srvc_agmt_type_cd,
                                        agmt.frmlty_type_cd              AS frmlty_type_cd,
                                        agmt.cntrct_term_num             AS cntrct_term_num,
                                        agmt.rate_rprcg_cycl_mth_num     AS rate_rprcg_cycl_mth_num,
                                        agmt.cmpnd_int_cycl_mth_num      AS cmpnd_int_cycl_mth_num,
                                        agmt.mdterm_int_pmt_cycl_mth_num AS mdterm_int_pmt_cycl_mth_num,
                                        agmt.prev_mdterm_int_pmt_dt      AS prev_mdterm_int_pmt_dt,
                                        agmt.nxt_mdterm_int_pmt_dt       AS nxt_mdterm_int_pmt_dt,
                                        agmt.prev_int_rate_rvsd_dt       AS prev_int_rate_rvsd_dt,
                                        agmt.nxt_int_rate_rvsd_dt        AS nxt_int_rate_rvsd_dt,
                                        agmt.prev_ref_dt_int_rate        AS prev_ref_dt_int_rate,
                                        agmt.nxt_ref_dt_for_int_rate     AS nxt_ref_dt_for_int_rate,
                                        agmt.mdterm_cncltn_dt            AS mdterm_cncltn_dt,
                                        agmt.stk_flow_clas_in_mth_ind    AS stk_flow_clas_in_mth_ind,
                                        agmt.stk_flow_clas_in_term_ind   AS stk_flow_clas_in_term_ind,
                                        agmt.agmt_idntftn_cd             AS agmt_idntftn_cd,
                                        agmt.trmtn_type_cd               AS trmtn_type_cd,
                                        agmt.int_pmt_meth_cd             AS int_pmt_meth_cd,
                                        agmt.lbr_agmt_desc               AS lbr_agmt_desc,
                                        agmt.guartd_imprsns_cnt          AS guartd_imprsns_cnt,
                                        agmt.cost_per_imprsn_amt         AS cost_per_imprsn_amt,
                                        agmt.guartd_clkthru_cnt          AS guartd_clkthru_cnt,
                                        agmt.cost_per_clkthru_amt        AS cost_per_clkthru_amt,
                                        agmt.busn_prty_id                AS busn_prty_id,
                                        agmt.mortgagee_prem_pmt_ind      AS mortgagee_prem_pmt_ind,
                                        agmt.pmt_pln_type_cd             AS pmt_pln_type_cd,
                                        agmt.invc_strem_type_cd          AS invc_strem_type_cd,
                                        agmt.modl_num                    AS modl_num,
                                        agmt.modl_crtn_dttm              AS modl_crtn_dttm,
                                        agmt.bilg_meth_type_cd           AS bilg_meth_type_cd,
                                        agmt.agmt_eff_dttm               AS agmt_eff_dttm,
                                        agmt.modl_eff_dttm               AS modl_eff_dttm,
                                        agmt.prcs_id                     AS prcs_id,
                                        agmt.modl_actl_end_dttm          AS modl_actl_end_dttm,
                                        agmt.cntnus_srvc_dttm            AS cntnus_srvc_dttm,
                                        agmt.prior_insrnc_ind            AS prior_insrnc_ind,
                                        agmt.edw_strt_dttm               AS edw_strt_dttm,
                                        agmt.edw_end_dttm                AS edw_end_dttm,
                                        agmt.nk_src_key                  AS nk_src_key,
                                        agmt.src_sys_cd                  AS src_sys_cd,
                                        agmt.host_agmt_num               AS host_agmt_num,
                                        agmt.term_num                    AS term_num,
                                        agmt.agmt_type_cd                AS agmt_type_cd
                               FROM     db_t_prod_core.agmt
                               WHERE    agmt_type_cd=''POLTRM'' qualify row_number() over(PARTITION BY agmt.host_agmt_num, agmt.term_num, agmt.agmt_type_cd ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.host_agmt_num = exp_pass_frm_src.policynumber
            AND       lkp.term_num = exp_pass_frm_src.termnumber
            AND       lkp.agmt_type_cd = exp_pass_frm_src.o_agmt_type_cd qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_PMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_pmt AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_frm_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_frm_src
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PMT_PLN_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bc_plan.name''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_frm_src.paymenttype qualify rnk = 1 );
  -- Component exp_data_ins_upd_chk, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_ins_upd_chk AS
  (
             SELECT     lkp_agmt_poltrm.agmt_id                                                AS lkp_agmt_id,
                        lkp_agmt_poltrm.edw_strt_dttm                                          AS lkp_edw_strt_dttm,
                        exp_pass_frm_src.policynumber                                          AS in_host_agmt_num,
                        exp_pass_frm_src.originaleffectivedate                                 AS in_agmt_opn_dttm,
                        exp_pass_frm_src.o_periodend                                           AS in_agmt_plnd_expn_dt,
                        exp_pass_frm_src.o_issuedate                                           AS in_agmt_signd_dt,
                        exp_pass_frm_src.termnumber                                            AS in_term_num,
                        exp_pass_frm_src.o_agmt_type_cd                                        AS in_agmt_type_cd,
                        current_timestamp                                                      AS in_edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        exp_pass_frm_src.periodstart                                           AS in_agmt_eff_dttm,
                        lkp_1.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                                               AS src_cd,
                        ''UNK''                                                                  AS default_txt,
                        lkp_teradata_etl_ref_pmt.tgt_idntftn_val                               AS paymenttype,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS default_dt,
                        to_date ( ''01/01/1900 00:00:00.000000'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS default_eff_dt,
                        md5 ( to_char ( lkp_agmt_poltrm.agmt_opn_dttm )
                                   || to_char ( lkp_agmt_poltrm.agmt_plnd_expn_dttm )
                                   || to_char ( lkp_agmt_poltrm.agmt_signd_dttm )
                                   || lkp_agmt_poltrm.pmt_pln_type_cd ) AS lkp_chksum,
                        md5 ( to_char ( exp_pass_frm_src.originaleffectivedate )
                                   || to_char ( exp_pass_frm_src.o_periodend )
                                   || to_char ( exp_pass_frm_src.o_issuedate )
                                   || lkp_teradata_etl_ref_pmt.tgt_idntftn_val ) AS in_chksum,
                        CASE
                                   WHEN lkp_agmt_poltrm.agmt_id IS NULL THEN ''I''
                                   ELSE (
                                              CASE
                                                         WHEN (
                                                                               lkp_chksum <> in_chksum ) THEN ''U''
                                                         ELSE ''R''
                                              END )
                        END                          AS inser_update_flag,
                        :prcs_id                     AS prcs_id,
                        exp_pass_frm_src.retired     AS retired,
                        lkp_agmt_poltrm.edw_end_dttm AS lkp_edw_end_dttm,
                        lkp_2.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DATA_SRC */
                                                    AS agmt_src_cd,
                        exp_pass_frm_src.updatetime AS updatetime,
                        exp_pass_frm_src.source_record_id,
                        row_number() over (PARTITION BY exp_pass_frm_src.source_record_id ORDER BY exp_pass_frm_src.source_record_id) AS rnk
             FROM       exp_pass_frm_src
             inner join lkp_agmt_poltrm
             ON         exp_pass_frm_src.source_record_id = lkp_agmt_poltrm.source_record_id
             inner join lkp_teradata_etl_ref_pmt
             ON         lkp_agmt_poltrm.source_record_id = lkp_teradata_etl_ref_pmt.source_record_id
             left join  lkp_teradata_etl_ref_xlat_src_cd lkp_1
             ON         lkp_1.src_idntftn_val = ''SRC_SYS4''
             left join  lkp_teradata_etl_ref_xlat_data_src lkp_2
             ON         lkp_2.src_idntftn_val = ''DATA_SRC_TYPE2'' 
			 qualify row_number() over (PARTITION BY exp_pass_frm_src.source_record_id ORDER BY exp_pass_frm_src.source_record_id) 
			 = 1 );
  -- Component rtr_ins_upd_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_ins_upd_Insert as
  SELECT NULL                                      AS affinitytype,
         exp_data_ins_upd_chk.lkp_agmt_id          AS lkp_agmt_id,
         exp_data_ins_upd_chk.in_host_agmt_num     AS in_host_agmt_num,
         exp_data_ins_upd_chk.in_agmt_opn_dttm     AS in_agmt_opn_dttm,
         exp_data_ins_upd_chk.in_agmt_plnd_expn_dt AS in_agmt_plnd_expn_dt,
         exp_data_ins_upd_chk.in_agmt_signd_dt     AS in_agmt_signd_dt,
         exp_data_ins_upd_chk.in_term_num          AS in_term_num,
         exp_data_ins_upd_chk.in_agmt_type_cd      AS in_agmt_type_cd,
         exp_data_ins_upd_chk.src_cd               AS src_cd,
         exp_data_ins_upd_chk.default_txt          AS default_txt,
         exp_data_ins_upd_chk.default_dt           AS default_dt,
         exp_data_ins_upd_chk.inser_update_flag    AS insert_update_flag,
         exp_data_ins_upd_chk.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_data_ins_upd_chk.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_data_ins_upd_chk.in_edw_end_dttm      AS in_edw_end_dttm,
         exp_data_ins_upd_chk.in_agmt_eff_dttm     AS in_agmt_eff_dttm,
         exp_data_ins_upd_chk.prcs_id              AS prcs_id,
         exp_data_ins_upd_chk.retired              AS retired,
         exp_data_ins_upd_chk.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_data_ins_upd_chk.agmt_src_cd          AS agmt_src_cd,
         exp_data_ins_upd_chk.paymenttype          AS paymenttype,
         exp_data_ins_upd_chk.updatetime           AS updatetime,
         exp_data_ins_upd_chk.default_eff_dt       AS default_eff_dt,
         exp_data_ins_upd_chk.source_record_id
  FROM   exp_data_ins_upd_chk
  WHERE  exp_data_ins_upd_chk.inser_update_flag = ''I''
  OR     (
                exp_data_ins_upd_chk.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_data_ins_upd_chk.retired = 0 );
  
  -- Component rtr_ins_upd_Retired, Type ROUTER Output Group Retired
  create or replace temporary table rtr_ins_upd_Retired as
  SELECT NULL                                      AS affinitytype,
         exp_data_ins_upd_chk.lkp_agmt_id          AS lkp_agmt_id,
         exp_data_ins_upd_chk.in_host_agmt_num     AS in_host_agmt_num,
         exp_data_ins_upd_chk.in_agmt_opn_dttm     AS in_agmt_opn_dttm,
         exp_data_ins_upd_chk.in_agmt_plnd_expn_dt AS in_agmt_plnd_expn_dt,
         exp_data_ins_upd_chk.in_agmt_signd_dt     AS in_agmt_signd_dt,
         exp_data_ins_upd_chk.in_term_num          AS in_term_num,
         exp_data_ins_upd_chk.in_agmt_type_cd      AS in_agmt_type_cd,
         exp_data_ins_upd_chk.src_cd               AS src_cd,
         exp_data_ins_upd_chk.default_txt          AS default_txt,
         exp_data_ins_upd_chk.default_dt           AS default_dt,
         exp_data_ins_upd_chk.inser_update_flag    AS insert_update_flag,
         exp_data_ins_upd_chk.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_data_ins_upd_chk.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_data_ins_upd_chk.in_edw_end_dttm      AS in_edw_end_dttm,
         exp_data_ins_upd_chk.in_agmt_eff_dttm     AS in_agmt_eff_dttm,
         exp_data_ins_upd_chk.prcs_id              AS prcs_id,
         exp_data_ins_upd_chk.retired              AS retired,
         exp_data_ins_upd_chk.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_data_ins_upd_chk.agmt_src_cd          AS agmt_src_cd,
         exp_data_ins_upd_chk.paymenttype          AS paymenttype,
         exp_data_ins_upd_chk.updatetime           AS updatetime,
         exp_data_ins_upd_chk.default_eff_dt       AS default_eff_dt,
         exp_data_ins_upd_chk.source_record_id
  FROM   exp_data_ins_upd_chk
  WHERE  exp_data_ins_upd_chk.inser_update_flag = ''R''
  AND    exp_data_ins_upd_chk.retired != 0
  AND    exp_data_ins_upd_chk.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_ins_upd_Update, Type ROUTER Output Group Update
  create or replace temporary table rtr_ins_upd_Update as
  SELECT NULL                                      AS affinitytype,
         exp_data_ins_upd_chk.lkp_agmt_id          AS lkp_agmt_id,
         exp_data_ins_upd_chk.in_host_agmt_num     AS in_host_agmt_num,
         exp_data_ins_upd_chk.in_agmt_opn_dttm     AS in_agmt_opn_dttm,
         exp_data_ins_upd_chk.in_agmt_plnd_expn_dt AS in_agmt_plnd_expn_dt,
         exp_data_ins_upd_chk.in_agmt_signd_dt     AS in_agmt_signd_dt,
         exp_data_ins_upd_chk.in_term_num          AS in_term_num,
         exp_data_ins_upd_chk.in_agmt_type_cd      AS in_agmt_type_cd,
         exp_data_ins_upd_chk.src_cd               AS src_cd,
         exp_data_ins_upd_chk.default_txt          AS default_txt,
         exp_data_ins_upd_chk.default_dt           AS default_dt,
         exp_data_ins_upd_chk.inser_update_flag    AS insert_update_flag,
         exp_data_ins_upd_chk.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_data_ins_upd_chk.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_data_ins_upd_chk.in_edw_end_dttm      AS in_edw_end_dttm,
         exp_data_ins_upd_chk.in_agmt_eff_dttm     AS in_agmt_eff_dttm,
         exp_data_ins_upd_chk.prcs_id              AS prcs_id,
         exp_data_ins_upd_chk.retired              AS retired,
         exp_data_ins_upd_chk.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
         exp_data_ins_upd_chk.agmt_src_cd          AS agmt_src_cd,
         exp_data_ins_upd_chk.paymenttype          AS paymenttype,
         exp_data_ins_upd_chk.updatetime           AS updatetime,
         exp_data_ins_upd_chk.default_eff_dt       AS default_eff_dt,
         exp_data_ins_upd_chk.source_record_id
  FROM   exp_data_ins_upd_chk
  WHERE  exp_data_ins_upd_chk.inser_update_flag = ''U''
  AND    exp_data_ins_upd_chk.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component exp_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired AS
  (
         SELECT rtr_ins_upd_retired.lkp_agmt_id       AS lkp_agmt_id4,
                rtr_ins_upd_retired.lkp_edw_strt_dttm AS lkp_edw_strt_dttm4,
                current_timestamp                     AS edw_end_dttm,
                rtr_ins_upd_retired.prcs_id           AS prcs_id4,
                rtr_ins_upd_retired.updatetime        AS updatetime4,
                rtr_ins_upd_retired.source_record_id
         FROM   rtr_ins_upd_retired );
  -- Component upd_agmt_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_retired.lkp_agmt_id4       AS lkp_agmt_id3,
                exp_retired.lkp_edw_strt_dttm4 AS edw_strt_dttm,
                exp_retired.edw_end_dttm       AS edw_end_dttm,
                exp_retired.prcs_id4           AS prcs_id,
                exp_retired.updatetime4        AS updatetime4,
                1                              AS update_strategy_action,
				source_record_id
         FROM   exp_retired );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
            SELECT    rtr_ins_upd_insert.in_host_agmt_num     AS in_host_agmt_num1,
                      rtr_ins_upd_insert.in_agmt_opn_dttm     AS in_agmt_opn_dttm1,
                      rtr_ins_upd_insert.in_agmt_plnd_expn_dt AS agmt_plnd_expn_dt,
                      rtr_ins_upd_insert.in_agmt_signd_dt     AS agmt_signd_dt,
                      rtr_ins_upd_insert.in_agmt_type_cd      AS agmt_type_cd,
                      rtr_ins_upd_insert.in_term_num          AS term_num,
                      rtr_ins_upd_insert.prcs_id              AS prcs_id,
                      rtr_ins_upd_insert.in_edw_strt_dttm     AS edw_strt_dttm,
                      rtr_ins_upd_insert.in_agmt_eff_dttm     AS in_agmt_eff_dttm1,
                      rtr_ins_upd_insert.src_cd               AS src_sys_cd,
                      rtr_ins_upd_insert.default_txt          AS default_txt1,
                      rtr_ins_upd_insert.default_dt           AS default_dt1,
                      CASE
                                WHEN rtr_ins_upd_insert.retired <> 0 THEN current_timestamp
                                ELSE rtr_ins_upd_insert.in_edw_end_dttm
                      END                                     AS edw_end_dttm1,
                      to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) AS agmt_prcsg_dttm,
                      to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) AS cntnus_srvc_dttm,
                      rtr_ins_upd_insert.agmt_src_cd          AS agmt_src_cd1,
                      rtr_ins_upd_insert.updatetime           AS updatetime1,
                      rtr_ins_upd_insert.paymenttype          AS paymenttype1,
                      CASE
                                WHEN rtr_ins_upd_insert.retired <> 0 THEN rtr_ins_upd_insert.updatetime
                                ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                      END AS trans_end_dttm,
                      lkp_1.agmt_id
                      /* replaced lookup LKP_XREF_AGMNT */
                                                        AS o_agmt_id,
                      rtr_ins_upd_insert.default_eff_dt AS default_eff_dt1,
                      rtr_ins_upd_insert.affinitytype   AS affinitytype,
                      rtr_ins_upd_insert.source_record_id,
                      row_number() over (PARTITION BY rtr_ins_upd_insert.source_record_id ORDER BY rtr_ins_upd_insert.source_record_id) AS rnk
            FROM      rtr_ins_upd_insert
            left join lkp_xref_agmnt lkp_1
            ON        lkp_1.nk_src_key = ltrim ( rtrim ( rtr_ins_upd_insert.in_host_agmt_num ) )
            AND       lkp_1.term_num = rtr_ins_upd_insert.in_term_num
            AND       lkp_1.agmt_type_cd = ltrim ( rtrim ( rtr_ins_upd_insert.in_agmt_type_cd ) ) qualify rnk = 1 );
  -- Component tgt_AGMT_upd_retired, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.agmt
  USING        upd_agmt_retired
  ON (
                            update_strategy_action = 1
               AND          agmt.agmt_id = upd_agmt_retired.lkp_agmt_id3
               AND          agmt.edw_strt_dttm = upd_agmt_retired.edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    edw_end_dttm = upd_agmt_retired.edw_end_dttm,
         trans_end_dttm = upd_agmt_retired.updatetime4 ;
  
  -- Component FILTRANS, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE filtrans AS
  (
         SELECT rtr_ins_upd_update.in_host_agmt_num     AS in_host_agmt_num3,
                rtr_ins_upd_update.in_agmt_opn_dttm     AS in_agmt_opn_dttm3,
                rtr_ins_upd_update.in_agmt_plnd_expn_dt AS in_agmt_plnd_expn_dt3,
                rtr_ins_upd_update.in_agmt_signd_dt     AS agmt_signd_dt,
                rtr_ins_upd_update.in_agmt_type_cd      AS agmt_type_cd,
                rtr_ins_upd_update.in_term_num          AS term_num,
                rtr_ins_upd_update.prcs_id              AS prcs_id,
                rtr_ins_upd_update.in_edw_strt_dttm     AS edw_strt_dttm,
                rtr_ins_upd_update.in_edw_end_dttm      AS edw_end_dttm,
                rtr_ins_upd_update.in_agmt_eff_dttm     AS in_agmt_eff_dttm3,
                rtr_ins_upd_update.src_cd               AS src_sys_cd,
                rtr_ins_upd_update.default_txt          AS default_txt1,
                rtr_ins_upd_update.default_dt           AS default_dt1,
                rtr_ins_upd_update.retired              AS retired3,
                rtr_ins_upd_update.agmt_src_cd          AS agmt_src_cd3,
                rtr_ins_upd_update.lkp_agmt_id          AS lkp_agmt_id3,
                rtr_ins_upd_update.updatetime           AS updatetime1,
                rtr_ins_upd_update.paymenttype          AS paymenttype3,
                rtr_ins_upd_update.default_eff_dt       AS default_eff_dt3,
                rtr_ins_upd_update.affinitytype         AS affinitytype1,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update
         WHERE  rtr_ins_upd_update.retired = 0 );
  -- Component exp_pass_to_tgt_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd AS
  (
         SELECT rtr_ins_upd_update.lkp_agmt_id                                   AS lkp_agmt_id3,
                rtr_ins_upd_update.lkp_edw_strt_dttm                             AS edw_strt_dttm,
                dateadd (second,-1,  rtr_ins_upd_update.in_edw_strt_dttm  ) AS in_edw_strt_dttm,
                rtr_ins_upd_update.prcs_id                                       AS prcs_id,
                rtr_ins_upd_update.updatetime                                    AS updatetime3,
                dateadd ( second, -1, rtr_ins_upd_update.updatetime )       AS trans_end_dttm,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update );
  -- Component upd_agmt_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_upd.lkp_agmt_id3     AS lkp_agmt_id3,
                exp_pass_to_tgt_upd.edw_strt_dttm    AS edw_strt_dttm,
                exp_pass_to_tgt_upd.in_edw_strt_dttm AS in_edw_strt_dttm3,
                exp_pass_to_tgt_upd.prcs_id          AS prcs_id,
                exp_pass_to_tgt_upd.updatetime3      AS updatetime3,
                exp_pass_to_tgt_upd.trans_end_dttm   AS trans_end_dttm,
                1                                    AS update_strategy_action
				
         FROM   exp_pass_to_tgt_upd );
  -- Component upd_agmt_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_ins.in_host_agmt_num1 AS host_agmt_num,
                exp_pass_to_tgt_ins.in_agmt_opn_dttm1 AS agmt_opn_dttm,
                exp_pass_to_tgt_ins.agmt_plnd_expn_dt AS agmt_plnd_expn_dt,
                exp_pass_to_tgt_ins.agmt_signd_dt     AS agmt_signd_dt,
                exp_pass_to_tgt_ins.agmt_type_cd      AS agmt_type_cd,
                exp_pass_to_tgt_ins.term_num          AS term_num,
                exp_pass_to_tgt_ins.prcs_id           AS prcs_id,
                exp_pass_to_tgt_ins.edw_strt_dttm     AS edw_strt_dttm,
                exp_pass_to_tgt_ins.in_agmt_eff_dttm1 AS in_agmt_eff_dttm1,
                exp_pass_to_tgt_ins.edw_end_dttm1     AS edw_end_dttm,
                exp_pass_to_tgt_ins.src_sys_cd        AS src_sys_cd,
                exp_pass_to_tgt_ins.default_txt1      AS default_txt1,
                exp_pass_to_tgt_ins.default_dt1       AS default_dt1,
                exp_pass_to_tgt_ins.agmt_prcsg_dttm   AS agmt_prcsg_dttm,
                exp_pass_to_tgt_ins.cntnus_srvc_dttm  AS cntnus_srvc_dttm,
                exp_pass_to_tgt_ins.agmt_src_cd1      AS agmt_src_cd1,
                exp_pass_to_tgt_ins.updatetime1       AS updatetime1,
                exp_pass_to_tgt_ins.paymenttype1      AS paymenttype1,
                exp_pass_to_tgt_ins.trans_end_dttm    AS trans_end_dttm,
                exp_pass_to_tgt_ins.o_agmt_id         AS agmt_id,
                exp_pass_to_tgt_ins.default_eff_dt1   AS default_eff_dt1,
                exp_pass_to_tgt_ins.affinitytype      AS affinitytype,
                0                                     AS update_strategy_action
         FROM   exp_pass_to_tgt_ins );
  -- Component exp_pass_to_tgt_ins1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins1 AS
  (
         SELECT filtrans.in_host_agmt_num3              AS in_host_agmt_num1,
                filtrans.in_agmt_opn_dttm3              AS in_agmt_opn_dttm1,
                filtrans.in_agmt_plnd_expn_dt3          AS agmt_plnd_expn_dt,
                filtrans.agmt_signd_dt                  AS agmt_signd_dt,
                filtrans.agmt_type_cd                   AS agmt_type_cd,
                filtrans.term_num                       AS term_num,
                filtrans.prcs_id                        AS prcs_id,
                filtrans.edw_strt_dttm                  AS edw_strt_dttm,
                filtrans.edw_end_dttm                   AS edw_end_dttm,
                filtrans.in_agmt_eff_dttm3              AS in_agmt_eff_dttm3,
                filtrans.src_sys_cd                     AS src_sys_cd,
                filtrans.default_txt1                   AS default_txt1,
                filtrans.default_dt1                    AS default_dt1,
                to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) AS agmt_prcsg_dttm,
                to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) AS cntnus_srvc_dttm,
                filtrans.agmt_src_cd3                   AS agmt_src_cd3,
                filtrans.lkp_agmt_id3                   AS lkp_agmt_id3,
                filtrans.updatetime1                    AS updatetime1,
                filtrans.paymenttype3                   AS paymenttype3,
                filtrans.default_eff_dt3                AS default_eff_dt3,
                filtrans.affinitytype1                  AS affinitytype1,
                filtrans.source_record_id
         FROM   filtrans );
  -- Component tgt_AGMT_upd, Type TARGET
  /* Perform Updates */
  merge
  INTO	db_t_prod_core.agmt
  USING        upd_agmt_upd
  ON (
                            agmt_id = upd_agmt_upd.lkp_AGMT_ID3
               AND          agmt.edw_strt_dttm = upd_agmt_upd.edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    agmt.edw_end_dttm = upd_agmt_upd.in_edw_strt_dttm3,
         agmt.trans_end_dttm = upd_agmt_upd.trans_end_dttm;
  
  -- Component upd_agmt_ins1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_ins1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_pass_to_tgt_ins1.in_host_agmt_num1 AS host_agmt_num,
                exp_pass_to_tgt_ins1.in_agmt_opn_dttm1 AS agmt_opn_dttm,
                exp_pass_to_tgt_ins1.agmt_plnd_expn_dt AS agmt_plnd_expn_dt,
                exp_pass_to_tgt_ins1.agmt_signd_dt     AS agmt_signd_dt,
                exp_pass_to_tgt_ins1.agmt_type_cd      AS agmt_type_cd,
                exp_pass_to_tgt_ins1.term_num          AS term_num,
                exp_pass_to_tgt_ins1.prcs_id           AS prcs_id,
                exp_pass_to_tgt_ins1.edw_strt_dttm     AS edw_strt_dttm,
                exp_pass_to_tgt_ins1.in_agmt_eff_dttm3 AS in_agmt_eff_dttm3,
                exp_pass_to_tgt_ins1.edw_end_dttm      AS edw_end_dttm,
                exp_pass_to_tgt_ins1.src_sys_cd        AS src_sys_cd,
                exp_pass_to_tgt_ins1.default_txt1      AS default_txt1,
                exp_pass_to_tgt_ins1.default_dt1       AS default_dt1,
                exp_pass_to_tgt_ins1.agmt_prcsg_dttm   AS agmt_prcsg_dttm,
                exp_pass_to_tgt_ins1.cntnus_srvc_dttm  AS cntnus_srvc_dttm,
                exp_pass_to_tgt_ins1.agmt_src_cd3      AS agmt_src_cd3,
                exp_pass_to_tgt_ins1.lkp_agmt_id3      AS lkp_agmt_id3,
                exp_pass_to_tgt_ins1.affinitytype1     AS affinitytype1,
                exp_pass_to_tgt_ins1.updatetime1       AS updatetime1,
                exp_pass_to_tgt_ins1.paymenttype3      AS paymenttype3,
                exp_pass_to_tgt_ins1.default_eff_dt3   AS default_eff_dt3,
                0                                      AS update_strategy_action
         FROM   exp_pass_to_tgt_ins1 );
  -- Component tgt_AGMT_ins, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_opn_dttm,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          agmt_objtv_type_cd,
                          mkt_risk_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          pmt_pln_type_cd,
                          agmt_eff_dttm,
                          term_num,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT upd_agmt_ins.agmt_id           AS agmt_id,
         upd_agmt_ins.host_agmt_num     AS host_agmt_num,
         upd_agmt_ins.agmt_opn_dttm     AS agmt_opn_dttm,
         upd_agmt_ins.agmt_plnd_expn_dt AS agmt_plnd_expn_dttm,
         upd_agmt_ins.agmt_signd_dt     AS agmt_signd_dttm,
         upd_agmt_ins.agmt_type_cd      AS agmt_type_cd,
         upd_agmt_ins.agmt_src_cd1      AS agmt_src_cd,
         upd_agmt_ins.default_txt1      AS agmt_cur_sts_cd,
         upd_agmt_ins.default_txt1      AS agmt_obtnd_cd,
         upd_agmt_ins.default_txt1      AS agmt_sbtype_cd,
         upd_agmt_ins.agmt_prcsg_dttm   AS agmt_prcsg_dttm,
         upd_agmt_ins.default_txt1      AS agmt_objtv_type_cd,
         upd_agmt_ins.default_txt1      AS mkt_risk_type_cd,
         upd_agmt_ins.default_txt1      AS ntwk_srvc_agmt_type_cd,
         upd_agmt_ins.default_txt1      AS frmlty_type_cd,
         upd_agmt_ins.default_txt1      AS agmt_idntftn_cd,
         upd_agmt_ins.default_txt1      AS trmtn_type_cd,
         upd_agmt_ins.default_txt1      AS int_pmt_meth_cd,
         upd_agmt_ins.paymenttype1      AS pmt_pln_type_cd,
         upd_agmt_ins.in_agmt_eff_dttm1 AS agmt_eff_dttm,
         upd_agmt_ins.term_num          AS term_num,
         upd_agmt_ins.default_eff_dt1   AS modl_eff_dttm,
         upd_agmt_ins.prcs_id           AS prcs_id,
         upd_agmt_ins.default_dt1       AS modl_actl_end_dttm,
         upd_agmt_ins.cntnus_srvc_dttm  AS cntnus_srvc_dttm,
         upd_agmt_ins.src_sys_cd        AS src_sys_cd,
         upd_agmt_ins.edw_strt_dttm     AS edw_strt_dttm,
         upd_agmt_ins.edw_end_dttm      AS edw_end_dttm,
         upd_agmt_ins.updatetime1       AS trans_strt_dttm,
         upd_agmt_ins.trans_end_dttm    AS trans_end_dttm
  FROM   upd_agmt_ins;
  
  -- Component tgt_AGMT_ins1, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_opn_dttm,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          agmt_objtv_type_cd,
                          mkt_risk_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          pmt_pln_type_cd,
                          agmt_eff_dttm,
                          term_num,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT upd_agmt_ins1.lkp_agmt_id3      AS agmt_id,
         upd_agmt_ins1.host_agmt_num     AS host_agmt_num,
         upd_agmt_ins1.agmt_opn_dttm     AS agmt_opn_dttm,
         upd_agmt_ins1.agmt_plnd_expn_dt AS agmt_plnd_expn_dttm,
         upd_agmt_ins1.agmt_signd_dt     AS agmt_signd_dttm,
         upd_agmt_ins1.agmt_type_cd      AS agmt_type_cd,
         upd_agmt_ins1.agmt_src_cd3      AS agmt_src_cd,
         upd_agmt_ins1.default_txt1      AS agmt_cur_sts_cd,
         upd_agmt_ins1.default_txt1      AS agmt_obtnd_cd,
         upd_agmt_ins1.default_txt1      AS agmt_sbtype_cd,
         upd_agmt_ins1.agmt_prcsg_dttm   AS agmt_prcsg_dttm,
         upd_agmt_ins1.default_txt1      AS agmt_objtv_type_cd,
         upd_agmt_ins1.default_txt1      AS mkt_risk_type_cd,
         upd_agmt_ins1.default_txt1      AS ntwk_srvc_agmt_type_cd,
         upd_agmt_ins1.default_txt1      AS frmlty_type_cd,
         upd_agmt_ins1.default_txt1      AS agmt_idntftn_cd,
         upd_agmt_ins1.default_txt1      AS trmtn_type_cd,
         upd_agmt_ins1.default_txt1      AS int_pmt_meth_cd,
         upd_agmt_ins1.paymenttype3      AS pmt_pln_type_cd,
         upd_agmt_ins1.in_agmt_eff_dttm3 AS agmt_eff_dttm,
         upd_agmt_ins1.term_num          AS term_num,
         upd_agmt_ins1.default_eff_dt3   AS modl_eff_dttm,
         upd_agmt_ins1.prcs_id           AS prcs_id,
         upd_agmt_ins1.default_dt1       AS modl_actl_end_dttm,
         upd_agmt_ins1.cntnus_srvc_dttm  AS cntnus_srvc_dttm,
         upd_agmt_ins1.src_sys_cd        AS src_sys_cd,
         upd_agmt_ins1.edw_strt_dttm     AS edw_strt_dttm,
         upd_agmt_ins1.edw_end_dttm      AS edw_end_dttm,
         upd_agmt_ins1.updatetime1       AS trans_strt_dttm
  FROM   upd_agmt_ins1;

END;
';