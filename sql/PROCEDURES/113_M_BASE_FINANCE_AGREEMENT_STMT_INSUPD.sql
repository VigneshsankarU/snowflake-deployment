-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_AGREEMENT_STMT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
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
  -- Component src_sq_bc_account, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE src_sq_bc_account AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS accountnumber,
                $2 AS src_cd,
                $3 AS invoicenumber,
                $4 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT ba.accountnumber_stg,
                                                                  bc_invoice.invoicenumber_stg ,
                                                                  ''SRC_SYS5'' AS src_cd
                                                  FROM            (
                                                                                  SELECT          bc_account.id_stg,
                                                                                                  bc_account.accountnumber_stg
                                                                                  FROM            db_t_prod_stag.bc_account
                                                                                  left outer join db_t_prod_stag.bctl_accounttype
                                                                                  ON              bc_account.accounttype_stg=bctl_accounttype.id_stg
                                                                                  WHERE           bc_account.updatetime_stg > cast($start_dttm AS timestamp)
                                                                                  AND             bc_account.updatetime_stg <= cast($end_dttm AS timestamp) )ba
                                                  left outer join db_t_prod_stag.pc_account
                                                  ON              ba.accountnumber_stg=pc_account.accountnumber_stg
                                                  inner join      db_t_prod_stag.bc_invoice
                                                  ON              bc_invoice.accountid_stg=ba.id_stg
                                                  AND             bc_invoice.updatetime_stg > cast($start_dttm AS timestamp)
                                                  AND             bc_invoice.updatetime_stg <= cast($end_dttm AS timestamp) ) src ) );
  -- Component exp_src_pass, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_pass AS
  (
            SELECT    src_sq_bc_account.accountnumber AS accountnumber,
                      src_sq_bc_account.invoicenumber AS invoicenumber,
                      ''INVOICE''                       AS o_doc_type,
                      ''BILL''                          AS o_doc_cat_type,
                      ''ACT''                           AS o_agmt_type_new,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                      AS out_src_cd,
                      src_sq_bc_account.source_record_id,
                      row_number() over (PARTITION BY src_sq_bc_account.source_record_id ORDER BY src_sq_bc_account.source_record_id) AS rnk
            FROM      src_sq_bc_account
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_1
            ON        lkp_1.src_idntftn_val = src_sq_bc_account.src_cd 
			qualify row_number() over (PARTITION BY src_sq_bc_account.source_record_id ORDER BY src_sq_bc_account.source_record_id) 
			= 1 );
  -- Component LKP_AGMT_POL, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_pol AS
  (
            SELECT    lkp.agmt_id,
                      exp_src_pass.source_record_id,
                      row_number() over(PARTITION BY exp_src_pass.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC) rnk
            FROM      exp_src_pass
            left join
                      (
                               SELECT   agmt.agmt_id                     AS agmt_id,
                                        agmt.host_agmt_num               AS host_agmt_num,
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
                                        agmt.lgcy_dscnt_ind              AS lgcy_dscnt_ind,
                                        agmt.agmt_idntftn_cd             AS agmt_idntftn_cd,
                                        agmt.trmtn_type_cd               AS trmtn_type_cd,
                                        agmt.int_pmt_meth_cd             AS int_pmt_meth_cd,
                                        agmt.lbr_agmt_desc               AS lbr_agmt_desc,
                                        agmt.guartd_imprsns_cnt          AS guartd_imprsns_cnt,
                                        agmt.cost_per_imprsn_amt         AS cost_per_imprsn_amt,
                                        agmt.guartd_clkthru_cnt          AS guartd_clkthru_cnt,
                                        agmt.cost_per_clkthru_amt        AS cost_per_clkthru_amt,
                                        agmt.busn_prty_id                AS busn_prty_id,
                                        agmt.pmt_pln_type_cd             AS pmt_pln_type_cd,
                                        agmt.invc_strem_type_cd          AS invc_strem_type_cd,
                                        agmt.modl_crtn_dttm              AS modl_crtn_dttm,
                                        agmt.cntnus_srvc_dttm            AS cntnus_srvc_dttm,
                                        agmt.bilg_meth_type_cd           AS bilg_meth_type_cd,
                                        agmt.src_sys_cd                  AS src_sys_cd,
                                        agmt.agmt_eff_dttm               AS agmt_eff_dttm,
                                        agmt.modl_eff_dttm               AS modl_eff_dttm,
                                        agmt.prcs_id                     AS prcs_id,
                                        agmt.modl_actl_end_dttm          AS modl_actl_end_dttm,
                                        agmt.tier_type_cd                AS tier_type_cd,
                                        agmt.edw_strt_dttm               AS edw_strt_dttm,
                                        agmt.edw_end_dttm                AS edw_end_dttm,
                                        agmt.vfyd_plcy_ind               AS vfyd_plcy_ind,
                                        agmt.src_of_busn_cd              AS src_of_busn_cd,
                                        agmt.ovrd_coms_type_cd           AS ovrd_coms_type_cd,
                                        agmt.lgcy_plcy_ind               AS lgcy_plcy_ind,
                                        agmt.trans_strt_dttm             AS trans_strt_dttm,
                                        agmt.nk_src_key                  AS nk_src_key,
                                        agmt.agmt_type_cd                AS agmt_type_cd
                               FROM     db_t_prod_core.agmt qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_src_key = exp_src_pass.accountnumber
            AND       lkp.agmt_type_cd = exp_src_pass.o_agmt_type_new 
			qualify row_number() over(PARTITION BY exp_src_pass.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC) 
            		= 1 );
  -- Component LKP_DOC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_doc AS
  (
            SELECT    lkp.doc_id,
                      exp_src_pass.source_record_id,
                      row_number() over(PARTITION BY exp_src_pass.source_record_id ORDER BY lkp.doc_id ASC,lkp.tm_prd_cd ASC,lkp.doc_crtn_dttm ASC,lkp.doc_recpt_dt ASC,lkp.doc_prd_strt_dttm ASC,lkp.doc_prd_end_dttm ASC,lkp.doc_issur_num ASC,lkp.data_src_type_cd ASC,lkp.doc_desc_txt ASC,lkp.doc_name ASC,lkp.doc_host_num ASC,lkp.doc_host_vers_num ASC,lkp.doc_cycl_cd ASC,lkp.doc_type_cd ASC,lkp.mm_objt_id ASC,lkp.doc_ctgy_type_cd ASC,lkp.lang_type_cd ASC,lkp.prcs_id ASC,lkp.doc_sts_cd ASC) rnk
            FROM      exp_src_pass
            left join
                      (
                             SELECT doc_id,
                                    tm_prd_cd,
                                    doc_crtn_dttm,
                                    doc_recpt_dt,
                                    doc_prd_strt_dttm,
                                    doc_prd_end_dttm,
                                    doc_issur_num,
                                    data_src_type_cd,
                                    doc_desc_txt,
                                    doc_name,
                                    doc_host_num,
                                    doc_host_vers_num,
                                    doc_cycl_cd,
                                    doc_type_cd,
                                    mm_objt_id,
                                    doc_ctgy_type_cd,
                                    lang_type_cd,
                                    prcs_id,
                                    doc_sts_cd
                             FROM   db_t_prod_core.doc ) lkp
            ON        lkp.doc_issur_num = exp_src_pass.invoicenumber
            AND       lkp.doc_type_cd = exp_src_pass.o_doc_type
            AND       lkp.doc_ctgy_type_cd = exp_src_pass.o_doc_cat_type 
			qualify row_number() over(PARTITION BY exp_src_pass.source_record_id ORDER BY lkp.doc_id ASC,lkp.tm_prd_cd ASC,lkp.doc_crtn_dttm ASC,lkp.doc_recpt_dt ASC,lkp.doc_prd_strt_dttm ASC,lkp.doc_prd_end_dttm ASC,lkp.doc_issur_num ASC,lkp.data_src_type_cd ASC,lkp.doc_desc_txt ASC,lkp.doc_name ASC,lkp.doc_host_num ASC,lkp.doc_host_vers_num ASC,lkp.doc_cycl_cd ASC,lkp.doc_type_cd ASC,lkp.mm_objt_id ASC,lkp.doc_ctgy_type_cd ASC,lkp.lang_type_cd ASC,lkp.prcs_id ASC,lkp.doc_sts_cd ASC) 
            			= 1 );
  -- Component exp_agmt_stmt_consolidation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_agmt_stmt_consolidation AS
  (
             SELECT     lkp_doc.doc_id       AS doc_id,
                        lkp_agmt_pol.agmt_id AS agmt_id,
                        lkp_agmt_pol.source_record_id
             FROM       lkp_agmt_pol
             inner join lkp_doc
             ON         lkp_agmt_pol.source_record_id = lkp_doc.source_record_id );
  -- Component LKP_AGMT_STMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_stmt AS
  (
            SELECT    lkp.agmt_id,
                      lkp.stmt_doc_id,
                      exp_agmt_stmt_consolidation.doc_id AS doc_id,
                      exp_agmt_stmt_consolidation.source_record_id,
                      row_number() over(PARTITION BY exp_agmt_stmt_consolidation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.stmt_doc_id ASC) rnk
            FROM      exp_agmt_stmt_consolidation
            left join
                      (
                             SELECT agmt_id,
                                    stmt_doc_id
                             FROM   db_t_prod_core.agmt_stmt ) lkp
            ON        lkp.agmt_id = exp_agmt_stmt_consolidation.agmt_id
            AND       lkp.stmt_doc_id = exp_agmt_stmt_consolidation.doc_id 
			qualify row_number() over(PARTITION BY exp_agmt_stmt_consolidation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.stmt_doc_id ASC) 
			= 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_agmt_stmt.agmt_id               AS lkp_agmt_id,
                        lkp_agmt_stmt.stmt_doc_id           AS lkp_stmt_doc_id,
                        exp_agmt_stmt_consolidation.doc_id  AS doc_id,
                        exp_agmt_stmt_consolidation.agmt_id AS agmt_id,
                        exp_agmt_stmt_consolidation.source_record_id
             FROM       exp_agmt_stmt_consolidation
             inner join lkp_agmt_stmt
             ON         exp_agmt_stmt_consolidation.source_record_id = lkp_agmt_stmt.source_record_id );
  -- Component flt_stmt_amt, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE flt_stmt_amt AS
  (
         SELECT exp_data_transformation.lkp_agmt_id     AS lkp_agmt_id,
                exp_data_transformation.lkp_stmt_doc_id AS lkp_stmt_doc_id,
                exp_data_transformation.doc_id          AS doc_id,
                exp_data_transformation.agmt_id         AS agmt_id,
                exp_data_transformation.source_record_id
         FROM   exp_data_transformation
         WHERE  exp_data_transformation.doc_id IS NOT NULL
         AND    exp_data_transformation.agmt_id IS NOT NULL
         AND    exp_data_transformation.lkp_agmt_id IS NULL );
  -- Component upd_agmt_stmt_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_stmt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT NULL                 AS o_doc_type,
                NULL                 AS o_doc_cat_type,
                NULL                 AS invoicenumber,
                flt_stmt_amt.doc_id  AS doc_id1,
                flt_stmt_amt.agmt_id AS agmt_id1,
                0                    AS update_strategy_action,
				flt_stmt_amt.source_record_id
         FROM   flt_stmt_amt );
  -- Component exp_ins_pass_to_target, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_pass_to_target AS
  (
         SELECT upd_agmt_stmt_ins.doc_id1  AS doc_id1,
                upd_agmt_stmt_ins.agmt_id1 AS agmt_id1,
                $prcs_id                   AS ou_prcs_id,
                upd_agmt_stmt_ins.source_record_id
         FROM   upd_agmt_stmt_ins );
  -- Component tgt_agmt_stmt_ins, Type TARGET
  INSERT INTO db_t_prod_core.agmt_stmt
              (
                          agmt_id,
                          stmt_doc_id,
                          prcs_id
              )
  SELECT exp_ins_pass_to_target.agmt_id1   AS agmt_id,
         exp_ins_pass_to_target.doc_id1    AS stmt_doc_id,
         exp_ins_pass_to_target.ou_prcs_id AS prcs_id
  FROM   exp_ins_pass_to_target;

END;
';