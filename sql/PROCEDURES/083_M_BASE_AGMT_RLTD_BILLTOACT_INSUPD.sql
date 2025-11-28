-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_RLTD_BILLTOACT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_RLTD_RSN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_rltd_rsn AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_RLTD_RSN''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
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
  -- Component sq_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS policynumber,
                $2  AS billingreferencenumber_alfa,
                $3  AS agmt_rltd_rsn_cd,
                $4  AS pc_src_cd,
                $5  AS bc_src_cd,
                $6  AS termnumber,
                $7  AS periodstart,
                $8  AS cancellationdate,
                $9  AS retired,
                $10 AS updatetime_agmt_rltd,
                $11 AS edw_strt_dttm,
                $12 AS rnk,
                $13 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT x.policynumber_stg AS pol ,
                                                                  x.billingreferencenumber_alfa_stg ,
                                                                  ''AGMT_RLTD_RSN2'' AS agmt_rltd_rsn_cd ,
                                                                  ''SRC_SYS4''       AS pc_src_cd ,
                                                                  ''SRC_SYS5''       AS bc_src_cd ,
                                                                  x.termnumber_stg ,
                                                                  CASE
                                                                                  WHEN x.periodstart IS NULL THEN cast (''1900-01-01 00:00:00.000000'' AS timestamp(6))
                                                                                  ELSE x.periodstart
                                                                  END periodstart ,
                                                                  CASE
                                                                                  WHEN x.cancellationdate IS NULL THEN cast (''9999-12-31 23:59:59.999999'' AS timestamp(6))
                                                                                  ELSE x.cancellationdate
                                                                  END cancellationdate ,
                                                                  CASE
                                                                                  WHEN x.retired_invoicestream = 0 THEN 0
                                                                                  ELSE 1
                                                                  END retired ,
                                                                  x.updatetime_agmtrltd,
                                                                  cast(current_timestamp AS timestamp(6)) AS src_edw_strt_dttm,
                                                                  /* EIM-50629 */
                                                                  row_number() over(PARTITION BY x.policynumber_stg,x.termnumber_stg, agmt_rltd_rsn_cd ORDER BY x.updatetime_agmtrltd ASC,updatetime_stg ASC ,billingreferencenumber_alfa_stg ASC ) AS rnk
                                                  --ORDER BY        pol
                                                                  /* EIM-50629 */
                                                  FROM            (
                                                                           SELECT   *
                                                                           FROM     (
                                                                                                    SELECT DISTINCT inv2.billingreferencenumber_alfa_stg ,
                                                                                                                    bc_policyperiod.policynumber_stg ,
                                                                                                                    bc_policyperiod.termnumber_stg ,
                                                                                                                    inv2.retired_stg AS retired_invoicestream,
                                                                                                                    inv2.updatetime_stg ,
                                                                                                                    CASE
                                                                                                                                    WHEN bc_policyperiod.updatetime_stg>inv2.updatetime_stg THEN bc_policyperiod.updatetime_stg
                                                                                                                                    ELSE inv2.updatetime_stg
                                                                                                                    END AS updatetime_agmtrltd ,
                                                                                                                    lkup.periodstart ,
                                                                                                                    lkup.cancellationdate,
                                                                                                                    1 ind
                                                                                                    FROM            db_t_prod_stag.bc_policyperiod
                                                                                                    join            db_t_prod_stag.bc_invoicestream inv2
                                                                                                    ON              bc_policyperiod.primaryinvoicestream_alfa_stg = inv2.id_stg
                                                                                                    left join
                                                                                                                    (
                                                                                                                           SELECT cancellationdate AS cancellationdate ,
                                                                                                                                  periodstart      AS periodstart,
                                                                                                                                  publicid_stg     AS publicid,
                                                                                                                                  policynumber_stg AS policynumber ,
                                                                                                                                  termnumber_stg
                                                                                                                           FROM   (
                                                                                                                                             SELECT     pol.policynumber_stg,
                                                                                                                                                        pol.cancellationdate_stg  AS cancellationdate,
                                                                                                                                                        pol.editeffectivedate_stg AS periodstart,
                                                                                                                                                        pol.modelnumber_stg,
                                                                                                                                                        pol.termnumber_stg,
                                                                                                                                                        pol.publicid_stg ,
                                                                                                                                                        row_number() over(PARTITION BY pol.policynumber_stg,pol.termnumber_stg ORDER BY pol.policynumber_stg,pol.modelnumber_stg DESC) AS r
                                                                                                                                                        /* EIM-50629 */
                                                                                                                                             FROM       db_t_prod_stag.pc_policyperiod pol
                                                                                                                                             inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                             ON         pol.status_stg = pctl_policyperiodstatus.id_stg
                                                                                                                                             WHERE      pctl_policyperiodstatus.typecode_stg = ''Bound'' )lkup
                                                                                                                           WHERE  r=1 ) lkup
                                                                                                    ON              bc_policyperiod.policynumber_stg =lkup.policynumber
                                                                                                    AND             bc_policyperiod.termnumber_stg =lkup.termnumber_stg
                                                                                                                    /* EIM-50629 */
                                                                                                    WHERE           ((
                                                                                                                                                    inv2.updatetime_stg> ($start_dttm)
                                                                                                                                    AND             inv2.updatetime_stg <= ($end_dttm))
                                                                                                                    OR              (
                                                                                                                                                    bc_policyperiod.updatetime_stg> ($start_dttm)
                                                                                                                                    AND             bc_policyperiod.updatetime_stg <= ($end_dttm))) 
                                                                                                                                    qualify row_number() over(PARTITION BY policynumber_stg,bc_policyperiod.termnumber_stg,inv2.billingreferencenumber_alfa_stg ORDER BY updatetime_agmtrltd DESC) = 1
                                                                                                    /* EIM-50629 New union to add Billing reference number */
                                                                                                    UNION
                                                                                                    SELECT DISTINCT inv1.billingreferencenumber_alfa_stg ,
                                                                                                                    bc_policyperiod.policynumber_stg ,
                                                                                                                    bc_policyperiod.termnumber_stg ,
                                                                                                                    inv1.retired_stg AS retired_invoicestream,
                                                                                                                    inv1.updatetime_stg ,
                                                                                                                    CASE
                                                                                                                                    WHEN (
                                                                                                                                                                    bc_invoiceitem.updatetime_stg > bc_policyperiod.updatetime_stg
                                                                                                                                                    AND             bc_invoiceitem.updatetime_stg>inv1.updatetime_stg) THEN bc_invoiceitem.updatetime_stg
                                                                                                                                    WHEN bc_policyperiod.updatetime_stg>inv1.updatetime_stg THEN bc_policyperiod.updatetime_stg
                                                                                                                                    ELSE inv1.updatetime_stg
                                                                                                                    END AS updatetime_agmtrltd ,
                                                                                                                    lkup.periodstart ,
                                                                                                                    lkup.cancellationdate,
                                                                                                                    2 ind
                                                                                                    FROM            db_t_prod_stag.bc_policyperiod
                                                                                                    left join       db_t_prod_stag.bc_invoiceitem
                                                                                                    ON              bc_invoiceitem.policyperiodid_stg = bc_policyperiod.id_stg
                                                                                                    left join       db_t_prod_stag.bc_invoice
                                                                                                    ON              bc_invoiceitem.invoiceid_stg = bc_invoice.id_stg
                                                                                                    left join       db_t_prod_stag.bc_invoicestream inv1
                                                                                                    ON              bc_invoice.invoicestreamid_stg = inv1.id_stg
                                                                                                    left join       db_t_prod_stag.bc_invoicestream inv2
                                                                                                    ON              bc_policyperiod.primaryinvoicestream_alfa_stg = inv2.id_stg
                                                                                                    left join
                                                                                                                    (
                                                                                                                           SELECT cancellationdate AS cancellationdate ,
                                                                                                                                  periodstart      AS periodstart,
                                                                                                                                  publicid_stg     AS publicid,
                                                                                                                                  policynumber_stg AS policynumber ,
                                                                                                                                  termnumber_stg
                                                                                                                           FROM   (
                                                                                                                                             SELECT     pol.policynumber_stg,
                                                                                                                                                        pol.cancellationdate_stg  AS cancellationdate,
                                                                                                                                                        pol.editeffectivedate_stg AS periodstart,
                                                                                                                                                        pol.modelnumber_stg,
                                                                                                                                                        pol.termnumber_stg,
                                                                                                                                                        pol.publicid_stg ,
                                                                                                                                                        row_number() over(PARTITION BY pol.policynumber_stg,pol.termnumber_stg ORDER BY pol.policynumber_stg,pol.modelnumber_stg DESC) AS r
                                                                                                                                             FROM       db_t_prod_stag.pc_policyperiod pol
                                                                                                                                             inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                             ON         pol.status_stg = pctl_policyperiodstatus.id_stg
                                                                                                                                             WHERE      pctl_policyperiodstatus.typecode_stg = ''Bound'' )lkup
                                                                                                                           WHERE  r=1 ) lkup
                                                                                                    ON              bc_policyperiod.policynumber_stg =lkup.policynumber
                                                                                                    AND             bc_policyperiod.termnumber_stg =lkup.termnumber_stg
                                                                                                    WHERE           (
                                                                                                                                    inv2.billingreferencenumber_alfa_stg<> inv1.billingreferencenumber_alfa_stg)
                                                                                                    AND             ((
                                                                                                                                                    inv1.updatetime_stg> ($start_dttm)
                                                                                                                                    AND             inv1.updatetime_stg <= ($end_dttm))
                                                                                                                    OR              (
                                                                                                                                                    bc_policyperiod.updatetime_stg> ($start_dttm)
                                                                                                                                    AND             bc_policyperiod.updatetime_stg <= ($end_dttm))
                                                                                                                    OR              (
                                                                                                                                                    bc_invoiceitem.updatetime_stg> ($start_dttm)
                                                                                                                                    AND             bc_invoiceitem.updatetime_stg <= ($end_dttm))) 
                                                                                                                                    qualify row_number() over(PARTITION BY policynumber_stg,bc_policyperiod.termnumber_stg,inv1.billingreferencenumber_alfa_stg ORDER BY updatetime_agmtrltd DESC) = 1 ) x 
                                                                                                                                    qualify row_number() over(PARTITION BY x.policynumber_stg,x.termnumber_stg ORDER BY ind DESC,x.updatetime_agmtrltd DESC,updatetime_stg DESC) = 1 )x ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_pc_policyperiod.billingreferencenumber_alfa AS billingreferencenumber_alfa,
                sq_pc_policyperiod.policynumber                AS policynumber,
                sq_pc_policyperiod.agmt_rltd_rsn_cd            AS agmt_rltd_rsn_cd,
                sq_pc_policyperiod.pc_src_cd                   AS pc_src_cd,
                sq_pc_policyperiod.bc_src_cd                   AS bc_src_cd,
                sq_pc_policyperiod.termnumber                  AS termnumber,
                sq_pc_policyperiod.periodstart                 AS periodstart,
                sq_pc_policyperiod.cancellationdate            AS cancellationdate,
                sq_pc_policyperiod.retired                     AS retired,
                sq_pc_policyperiod.updatetime_agmt_rltd        AS updatetime_agmt_rltd,
                sq_pc_policyperiod.edw_strt_dttm               AS edw_strt_dttm,
                sq_pc_policyperiod.rnk                         AS rnk,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.billingreferencenumber_alfa AS billingreferencenumber_alfa,
                      exp_pass_from_source.policynumber                AS policynumber,
                      exp_pass_from_source.periodstart                 AS agmt_rltd_strt_dt,
                      ''INV''                                            AS out_agmt_type_cd_inv,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE_CD */
                      END AS out_agmt_type_cd_poltrm,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_RLTD_RSN */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_RLTD_RSN */
                      END                                                                   AS out_agmt_rltd_rsn_cd,
                      exp_pass_from_source.cancellationdate                                 AS out_agmt_rltd_end_dt,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                      exp_pass_from_source.edw_strt_dttm                                    AS out_edw_strt_dttm,
                      $prcs_id                                                              AS out_prcs_id,
                      lkp_5.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                                            AS out_bc_src_cd,
                      exp_pass_from_source.updatetime_agmt_rltd                             AS trans_strt_dttm,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                      exp_pass_from_source.termnumber                                       AS termnumber,
                      exp_pass_from_source.retired                                          AS retired,
                      exp_pass_from_source.updatetime_agmt_rltd                             AS updatetime_agmt_rltd,
                      --exp_pass_from_source.rnk                                              AS rnk,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_agmt_type_cd lkp_1
            ON        lkp_1.src_idntftn_val = ''AGMT_TYPE6''
            left join lkp_teradata_etl_ref_xlat_agmt_type_cd lkp_2
            ON        lkp_2.src_idntftn_val = ''AGMT_TYPE6''
            left join lkp_teradata_etl_ref_xlat_agmt_rltd_rsn lkp_3
            ON        lkp_3.src_idntftn_val = exp_pass_from_source.agmt_rltd_rsn_cd
            left join lkp_teradata_etl_ref_xlat_agmt_rltd_rsn lkp_4
            ON        lkp_4.src_idntftn_val = exp_pass_from_source.agmt_rltd_rsn_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_5
            ON        lkp_5.src_idntftn_val = exp_pass_from_source.bc_src_cd 
            qualify row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) 
             = 1 );
  -- Component LKP_AGMT_POLTRM, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_poltrm AS
  (
            SELECT    lkp.agmt_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_busn_type_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.mortgagee_prem_pmt_ind ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_num ASC,lkp.modl_crtn_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.agmt_eff_dttm ASC,lkp.term_num ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.prior_insrnc_ind ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.nk_src_key ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_data_transformation
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
                               qualify row_number() over(PARTITION BY agmt.host_agmt_num, agmt.term_num, agmt.agmt_type_cd ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.host_agmt_num = exp_data_transformation.policynumber
            AND       lkp.term_num = exp_data_transformation.termnumber
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_cd_poltrm 
            qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_busn_type_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.mortgagee_prem_pmt_ind ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_num ASC,lkp.modl_crtn_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.agmt_eff_dttm ASC,lkp.term_num ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.prior_insrnc_ind ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.nk_src_key ASC,lkp.src_sys_cd ASC) 
                         = 1 );
  -- Component LKP_AGMT_INV, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_inv AS
  (
            SELECT    lkp.agmt_id,
                      lkp.trans_strt_dttm,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC) rnk
            FROM      exp_data_transformation
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
                               FROM     db_t_prod_core.agmt 
                               qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_src_key = exp_data_transformation.billingreferencenumber_alfa
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_cd_inv 
            qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC) 
                         = 1 );
  -- Component LKP_AGMT_RLTD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_rltd AS
  (
             SELECT     lkp.agmt_id,
                        lkp.rltd_agmt_id,
                        lkp.agmt_rltd_rsn_cd,
                        lkp.agmt_rltd_strt_dttm,
                        lkp.agmt_rltd_end_dttm,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        exp_data_transformation.source_record_id,
                        row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.rltd_agmt_id ASC,lkp.agmt_rltd_rsn_cd ASC,lkp.agmt_rltd_strt_dttm ASC,lkp.agmt_rltd_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
             FROM       exp_data_transformation
             inner join lkp_agmt_poltrm
             ON         exp_data_transformation.source_record_id = lkp_agmt_poltrm.source_record_id
             left join
                        (
                                 SELECT   agmt_rltd.agmt_rltd_strt_dttm AS agmt_rltd_strt_dttm,
                                          agmt_rltd.agmt_rltd_end_dttm  AS agmt_rltd_end_dttm,
                                          agmt_rltd.edw_strt_dttm       AS edw_strt_dttm,
                                          agmt_rltd.edw_end_dttm        AS edw_end_dttm,
                                          agmt_rltd.agmt_id             AS agmt_id,
                                          agmt_rltd.rltd_agmt_id        AS rltd_agmt_id,
                                          agmt_rltd.agmt_rltd_rsn_cd    AS agmt_rltd_rsn_cd
                                 FROM     db_t_prod_core.agmt_rltd                               
                                 WHERE    agmt_rltd_rsn_cd =''BILLTOPLCYTRM'' 
                                 qualify row_number() over(PARTITION BY rltd_agmt_id,agmt_rltd_rsn_cd ORDER BY edw_end_dttm DESC) = 1
                                 ) lkp
             ON         lkp.rltd_agmt_id = lkp_agmt_poltrm.agmt_id
             AND        lkp.agmt_rltd_rsn_cd = exp_data_transformation.out_agmt_rltd_rsn_cd 
             qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.rltd_agmt_id ASC,lkp.agmt_rltd_rsn_cd ASC,lkp.agmt_rltd_strt_dttm ASC,lkp.agmt_rltd_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) 
                           = 1 );
  -- Component ex_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE ex_ins_upd AS
  (
             SELECT     lkp_agmt_rltd.agmt_id             AS lkp_agmt_id,
                        lkp_agmt_rltd.rltd_agmt_id        AS lkp_rltd_agmt_id,
                        lkp_agmt_rltd.agmt_rltd_rsn_cd    AS lkp_agmt_rltd_rsn_cd,
                        lkp_agmt_rltd.agmt_rltd_strt_dttm AS lkp_agmt_rltd_strt_dt,
                        lkp_agmt_rltd.edw_strt_dttm       AS lkp_edw_strt_dttm,
                        md5 ( to_char ( lkp_agmt_rltd.agmt_rltd_strt_dttm )
                                   || to_char ( lkp_agmt_rltd.agmt_rltd_end_dttm )
                                   || to_char ( lkp_agmt_rltd.agmt_id ) ) AS orig_chksm,
                        lkp_agmt_inv.agmt_id                              AS agmt_id,
                        lkp_agmt_poltrm.agmt_id                           AS rltd_agmt_id,
                        exp_data_transformation.out_agmt_rltd_rsn_cd      AS out_agmt_rltd_rsn_cd,
                        exp_data_transformation.out_agmt_rltd_end_dt      AS out_agmt_rltd_end_dt,
                        exp_data_transformation.agmt_rltd_strt_dt         AS agmt_rltd_strt_dt,
                        exp_data_transformation.out_edw_end_dttm          AS out_edw_end_dttm,
                        exp_data_transformation.out_edw_strt_dttm         AS out_edw_strt_dttm,
                        exp_data_transformation.out_prcs_id               AS out_prcs_id,
                        md5 ( to_char ( exp_data_transformation.agmt_rltd_strt_dt )
                                   || to_char ( exp_data_transformation.out_agmt_rltd_end_dt )
                                   || to_char ( lkp_agmt_inv.agmt_id ) ) AS calc_chksm,
                        CASE
                                   WHEN orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN calc_chksm != orig_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                          AS out_ins_upd,
                        exp_data_transformation.trans_strt_dttm      AS trans_strt_dttm,
                        exp_data_transformation.trans_end_dttm       AS trans_end_dttm,
                        exp_data_transformation.retired              AS retired,
                        lkp_agmt_rltd.edw_end_dttm                   AS edw_end_dttm,
                        exp_data_transformation.updatetime_agmt_rltd AS updatetime_agmt_rltd,
                        exp_data_transformation.rnk                  AS rnk,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_agmt_poltrm
             ON         exp_data_transformation.source_record_id = lkp_agmt_poltrm.source_record_id
             inner join lkp_agmt_inv
             ON         lkp_agmt_poltrm.source_record_id = lkp_agmt_inv.source_record_id
             inner join lkp_agmt_rltd
             ON         lkp_agmt_inv.source_record_id = lkp_agmt_rltd.source_record_id );
  
  -- Component rt_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rt_ins_upd_insert as
    SELECT ex_ins_upd.agmt_id               AS agmt_id,
         ex_ins_upd.rltd_agmt_id          AS rltd_agmt_id,
         ex_ins_upd.out_agmt_rltd_rsn_cd  AS out_agmt_rltd_rsn_cd,
         ex_ins_upd.agmt_rltd_strt_dt     AS agmt_rltd_strt_dt,
         ex_ins_upd.out_agmt_rltd_end_dt  AS out_agmt_rltd_end_dt,
         ex_ins_upd.out_prcs_id           AS out_prcs_id,
         ex_ins_upd.lkp_agmt_rltd_strt_dt AS lkp_agmt_rltd_strt_dt,
         ex_ins_upd.lkp_agmt_id           AS lkp_agmt_id,
         ex_ins_upd.lkp_rltd_agmt_id      AS lkp_rltd_agmt_id,
         ex_ins_upd.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm_upd,
         ex_ins_upd.out_edw_end_dttm      AS out_edw_end_dttm,
         ex_ins_upd.out_edw_strt_dttm     AS out_edw_strt_dttm,
         ex_ins_upd.out_ins_upd           AS out_ins_upd,
         ex_ins_upd.lkp_agmt_rltd_rsn_cd  AS lkp_agmt_rltd_rsn_cd,
         ex_ins_upd.trans_strt_dttm       AS trans_strt_dttm,
         ex_ins_upd.trans_end_dttm        AS trans_end_dttm,
         ex_ins_upd.retired               AS retired,
         ex_ins_upd.edw_end_dttm          AS lkp_edw_end_dttm,
         ex_ins_upd.updatetime_agmt_rltd  AS updatetime_agmt_rltd,
         ex_ins_upd.rnk                   AS rnk,
         ex_ins_upd.source_record_id
  FROM   ex_ins_upd
  WHERE  ( (
                       ex_ins_upd.agmt_id IS NOT NULL
                AND    ex_ins_upd.rltd_agmt_id IS NOT NULL )
         AND    ex_ins_upd.out_ins_upd = ''I''
         OR     (
                       ex_ins_upd.edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    ex_ins_upd.retired = 0 ) )
  OR     (
                ex_ins_upd.out_ins_upd = ''U''
         AND    ex_ins_upd.edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    ex_ins_upd.agmt_id IS NOT NULL
         AND    ex_ins_upd.rltd_agmt_id IS NOT NULL );
  
  -- Component rt_ins_upd_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rt_ins_upd_retired as
  SELECT ex_ins_upd.agmt_id               AS agmt_id,
         ex_ins_upd.rltd_agmt_id          AS rltd_agmt_id,
         ex_ins_upd.out_agmt_rltd_rsn_cd  AS out_agmt_rltd_rsn_cd,
         ex_ins_upd.agmt_rltd_strt_dt     AS agmt_rltd_strt_dt,
         ex_ins_upd.out_agmt_rltd_end_dt  AS out_agmt_rltd_end_dt,
         ex_ins_upd.out_prcs_id           AS out_prcs_id,
         ex_ins_upd.lkp_agmt_rltd_strt_dt AS lkp_agmt_rltd_strt_dt,
         ex_ins_upd.lkp_agmt_id           AS lkp_agmt_id,
         ex_ins_upd.lkp_rltd_agmt_id      AS lkp_rltd_agmt_id,
         ex_ins_upd.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm_upd,
         ex_ins_upd.out_edw_end_dttm      AS out_edw_end_dttm,
         ex_ins_upd.out_edw_strt_dttm     AS out_edw_strt_dttm,
         ex_ins_upd.out_ins_upd           AS out_ins_upd,
         ex_ins_upd.lkp_agmt_rltd_rsn_cd  AS lkp_agmt_rltd_rsn_cd,
         ex_ins_upd.trans_strt_dttm       AS trans_strt_dttm,
         ex_ins_upd.trans_end_dttm        AS trans_end_dttm,
         ex_ins_upd.retired               AS retired,
         ex_ins_upd.edw_end_dttm          AS lkp_edw_end_dttm,
         ex_ins_upd.updatetime_agmt_rltd  AS updatetime_agmt_rltd,
         ex_ins_upd.rnk                   AS rnk,
         ex_ins_upd.source_record_id
  FROM   ex_ins_upd
  WHERE  ex_ins_upd.out_ins_upd = ''R''
  AND    ex_ins_upd.retired != 0
  AND    ex_ins_upd.edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    ex_ins_upd.agmt_id IS NOT NULL
  AND    ex_ins_upd.rltd_agmt_id IS NOT NULL;
  
  -- Component upd_insert_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rt_ins_upd_insert.agmt_id              AS agmt_id1,
                rt_ins_upd_insert.rltd_agmt_id         AS rltd_agmt_id1,
                rt_ins_upd_insert.out_agmt_rltd_rsn_cd AS out_agmt_rltd_rsn_cd1,
                rt_ins_upd_insert.agmt_rltd_strt_dt    AS agmt_rltd_strt_dt1,
                rt_ins_upd_insert.out_agmt_rltd_end_dt AS out_agmt_rltd_end_dt1,
                rt_ins_upd_insert.out_prcs_id          AS out_prcs_id1,
                rt_ins_upd_insert.out_edw_end_dttm     AS out_edw_end_dttm1,
                rt_ins_upd_insert.out_edw_strt_dttm    AS out_edw_strt_dttm1,
                rt_ins_upd_insert.trans_strt_dttm      AS trans_strt_dttm1,
                rt_ins_upd_insert.trans_end_dttm       AS trans_end_dttm1,
                rt_ins_upd_insert.retired              AS retired1,
                rt_ins_upd_insert.rnk                  AS rnk1,
                0                                      AS update_strategy_action,
                rt_ins_upd_insert.source_record_id
         FROM   rt_ins_upd_insert );
  -- Component upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rt_ins_upd_retired.lkp_agmt_id           AS lkp_agmt_id3,
                rt_ins_upd_retired.lkp_rltd_agmt_id      AS lkp_rltd_agmt_id3,
                rt_ins_upd_retired.lkp_edw_strt_dttm_upd AS lkp_edw_strt_dttm_upd3,
                rt_ins_upd_retired.lkp_agmt_rltd_rsn_cd  AS lkp_agmt_rltd_rsn_cd3,
                rt_ins_upd_retired.trans_strt_dttm       AS trans_strt_dttm4,
                1                                        AS update_strategy_action,
                rt_ins_upd_retired.source_record_id,
         FROM   rt_ins_upd_retired );
  -- Component exp_pass_to_tgt_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_retired AS
  (
         SELECT upd_retired.lkp_agmt_id3           AS lkp_agmt_id3,
                upd_retired.lkp_rltd_agmt_id3      AS lkp_rltd_agmt_id3,
                upd_retired.lkp_edw_strt_dttm_upd3 AS lkp_edw_strt_dttm_upd3,
                upd_retired.lkp_agmt_rltd_rsn_cd3  AS lkp_agmt_rltd_rsn_cd3,
                upd_retired.trans_strt_dttm4       AS trans_strt_dttm4,
                current_timestamp                  AS edw_end_dttm,
                upd_retired.source_record_id
         FROM   upd_retired );
  -- Component exp_ins_pass_to_tgt, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_pass_to_tgt AS
  (
         SELECT upd_insert_new.agmt_id1              AS agmt_id1,
                upd_insert_new.rltd_agmt_id1         AS rltd_agmt_id1,
                upd_insert_new.out_agmt_rltd_rsn_cd1 AS out_agmt_rltd_rsn_cd1,
                upd_insert_new.agmt_rltd_strt_dt1    AS agmt_rltd_strt_dt1,
                upd_insert_new.out_agmt_rltd_end_dt1 AS out_agmt_rltd_end_dt1,
                upd_insert_new.out_prcs_id1          AS out_prcs_id1,
                CASE
                       WHEN upd_insert_new.retired1 = 0 THEN dateadd(''second'', ( 2 * ( upd_insert_new.rnk1 - 1 ) ), upd_insert_new.out_edw_strt_dttm1)
                       ELSE upd_insert_new.out_edw_strt_dttm1
                END                             AS out_edw_strt_dttm1,
                upd_insert_new.trans_strt_dttm1 AS trans_strt_dttm1,
                CASE
                       WHEN upd_insert_new.retired1 != 0 THEN upd_insert_new.trans_strt_dttm1
                       ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm1,
                CASE
                       WHEN upd_insert_new.retired1 = 0 THEN upd_insert_new.out_edw_end_dttm1
                       ELSE current_timestamp
                END AS out_edw_end_dttm,
                upd_insert_new.source_record_id
         FROM   upd_insert_new );
  -- Component AGMT_RLTD_INS_NEW, Type TARGET
  INSERT INTO db_t_prod_core.agmt_rltd
              (
                          agmt_id,
                          rltd_agmt_id,
                          agmt_rltd_rsn_cd,
                          agmt_rltd_strt_dttm,
                          agmt_rltd_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_ins_pass_to_tgt.agmt_id1              AS agmt_id,
         exp_ins_pass_to_tgt.rltd_agmt_id1         AS rltd_agmt_id,
         exp_ins_pass_to_tgt.out_agmt_rltd_rsn_cd1 AS agmt_rltd_rsn_cd,
         exp_ins_pass_to_tgt.agmt_rltd_strt_dt1    AS agmt_rltd_strt_dttm,
         exp_ins_pass_to_tgt.out_agmt_rltd_end_dt1 AS agmt_rltd_end_dttm,
         exp_ins_pass_to_tgt.out_prcs_id1          AS prcs_id,
         exp_ins_pass_to_tgt.out_edw_strt_dttm1    AS edw_strt_dttm,
         exp_ins_pass_to_tgt.out_edw_end_dttm      AS edw_end_dttm,
         exp_ins_pass_to_tgt.trans_strt_dttm1      AS trans_strt_dttm,
         exp_ins_pass_to_tgt.trans_end_dttm1       AS trans_end_dttm
  FROM   exp_ins_pass_to_tgt;
  
  -- Component AGMT_RLTD_UPD_retired, Type TARGET
  merge
  INTO         db_t_prod_core.agmt_rltd
  USING        exp_pass_to_tgt_retired
  ON (
                            agmt_rltd.agmt_id = exp_pass_to_tgt_retired.lkp_agmt_id3
               AND          agmt_rltd.rltd_agmt_id = exp_pass_to_tgt_retired.lkp_rltd_agmt_id3
               AND          agmt_rltd.agmt_rltd_rsn_cd = exp_pass_to_tgt_retired.lkp_agmt_rltd_rsn_cd3
               AND          agmt_rltd.edw_strt_dttm = exp_pass_to_tgt_retired.lkp_edw_strt_dttm_upd3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_pass_to_tgt_retired.lkp_agmt_id3,
         rltd_agmt_id = exp_pass_to_tgt_retired.lkp_rltd_agmt_id3,
         agmt_rltd_rsn_cd = exp_pass_to_tgt_retired.lkp_agmt_rltd_rsn_cd3,
         edw_strt_dttm = exp_pass_to_tgt_retired.lkp_edw_strt_dttm_upd3,
         edw_end_dttm = exp_pass_to_tgt_retired.edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_retired.trans_strt_dttm4;
  
  -- Component AGMT_RLTD_UPD_retired, Type Post SQL
  UPDATE db_t_prod_core.agmt_rltd
  SET    edw_end_dttm=a.lead1 ,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         rltd_agmt_id,
                                         agmt_rltd_rsn_cd,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY rltd_agmt_id, agmt_rltd_rsn_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY rltd_agmt_id, agmt_rltd_rsn_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.agmt_rltd
                         WHERE           agmt_rltd_rsn_cd =''BILLTOPLCYTRM'' ) a

  WHERE  agmt_rltd.edw_strt_dttm = a.edw_strt_dttm
  AND    agmt_rltd.rltd_agmt_id=a.rltd_agmt_id
  AND    agmt_rltd.agmt_rltd_rsn_cd=a.agmt_rltd_rsn_cd
  AND    agmt_rltd.agmt_rltd_rsn_cd =''BILLTOPLCYTRM''
  AND    cast(agmt_rltd.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(agmt_rltd.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';