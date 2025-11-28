-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_RLTD_ACCTTOBILL_INSUPD("PARAM_JSON" VARCHAR)
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
  -- Component sq_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS pol_acc,
                $2  AS billingreferencenumber_alfa,
                $3  AS agmt_rltd_rsn_cd,
                $4  AS pc_src_cd,
                $5  AS bc_src_cd,
                $6  AS periodstart,
                $7  AS cancellationdate,
                $8  AS retired,
                $9  AS updatetime_agmt_rltd,
                $10 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT accountnumber_x               AS pol_acc,
                                                                  billingreferencenumber_alfa_x AS billingreferencenumber_alfa,
                                                                  agmt_rltd_rsn_cd,
                                                                  ''SRC_SYS4''                                                                                         AS pc_src_cd,
                                                                  ''SRC_SYS5''                                                                                         AS bc_src_cd,
                                                                  coalesce(agmt_invoicestream_x.periodstart_y, cast(''1900-01-01 00:00:00.000000'' AS timestamp))      AS periodstart,
                                                                  coalesce(agmt_invoicestream_x.cancellationdate_y, cast(''9999-12-31 23:59:59.999999'' AS timestamp)) AS cancellationdate,
                                                                  CASE
                                                                                  WHEN retired_invoicestream_x = 0
                                                                                  AND             retired_account_x = 0 THEN 0
                                                                                  ELSE 1
                                                                  END                                               retired,
                                                                  agmt_invoicestream_x.updatetime_agmtrltd_max_x AS updatetime_agmt_rltd
                                                  FROM            (
                                                                            SELECT    accountnumber_x,
                                                                                      billingreferencenumber_alfa_x,
                                                                                      agmt_rltd_rsn_cd,
                                                                                      lkp_y.cancellationdate AS cancellationdate_y ,
                                                                                      lkp_y.periodstart      AS periodstart_y,
                                                                                      updatetime_agmtrltd_max_x ,
                                                                                      retired_invoicestream_x,
                                                                                      retired_account_x
                                                                            FROM     (
                                                                                                      SELECT DISTINCT x.billingreferencenumber_alfa AS billingreferencenumber_alfa_x,
                                                                                                                      /*BIL_DA_0257-invoice stream type cd*/
                                                                                                                      x.invoice_stream_type_cd AS invoice_stream_type_cd_x,
                                                                                                                      /*BIL_DA_0252-Statement Mail Type Cd*/
                                                                                                                      x.statement_mail_type_cd AS statement_mail_type_cd_x,
                                                                                                                      /*BIL_DA_0326-transmittal mode type cd*/
                                                                                                                      CASE
                                                                                                                                      WHEN x.transmittal_mode_type_cd IS NULL THEN ''responsive''
                                                                                                                                      ELSE x.transmittal_mode_type_cd
                                                                                                                      END AS transmittal_mode_type_cd_x
                                                                                                                      /*Defect 6914- Quinn Default payment is Responsive as alfa*/
                                                                                                                      ,
                                                                                                                      x.policynumber             AS policynumber_x,
                                                                                                                      x.accountnumber            AS accountnumber_x,
                                                                                                                      x.billing_meth_type_cd     AS billing_meth_type_cd_x,
                                                                                                                      max(x.updatetime)          AS updatetime_max_x,
                                                                                                                      x.agmt_rltd_rsn_cd         AS agmt_rltd_rsn_cd,
                                                                                                                      x.typecode                 AS stmt_cycl_cd_x ,
                                                                                                                      x.termnumber               AS termnumber_x,
                                                                                                                      min(x.eventdate)           AS eventdate_min_x,
                                                                                                                      max(x.paymentduedate)      AS paymentduedate_max_x,
                                                                                                                      x.retired_invoicestream    AS retired_invoicestream_x,
                                                                                                                      x.retired_account          AS retired_account_x,
                                                                                                                      x.retired_policyperiod     AS retired_policyperiod_x,
                                                                                                                      ( $start_dttm )            AS start_dttm_x,
                                                                                                                      ( $end_dttm )              AS end_dttm_x,
                                                                                                                      max(x.updatetime_agmtrltd) AS updatetime_agmtrltd_max_x,
                                                                                                                      max(x.inv_trans_strt_dttm) AS inv_trans_strt_dttm_max_x
                                                                                                                      /* , */
                                                                                                                      /* lkp_y.cancellationdate as cancellationdate_y ,lkp_y.periodstart as periodstart_y */
                                                                                                      FROM            (
                                                                                                                                      SELECT DISTINCT bc_invoicestream.billingreferencenumber_alfa_stg AS billingreferencenumber_alfa,
                                                                                                                                                      /*BIL_DA_0257-invoice stream type cd*/
                                                                                                                                                      bctl_billinglevel.typecode_stg AS invoice_stream_type_cd,
                                                                                                                                                      /* ,/*BIL_DA_0252-transmittal mode type cd
                                                                                                                                                      bctl_invoicedeliverymethod.typecode_stg AS transmittal_mode_type_cd  ,*/
                                                                                                                                                      /*BIL_DA_0252-Statement Mail Type Cd*/
                                                                                                                                                      bctl_invoicedeliverymethod.typecode_stg AS statement_mail_type_cd,
                                                                                                                                                      /*BIL_DA_0326-transmittal mode type cd*/
                                                                                                                                                      bctl_paymentmethod.typecode_stg             AS transmittal_mode_type_cd,
                                                                                                                                                      bc_policyperiod.policynumber_stg            AS policynumber,
                                                                                                                                                      bc_account.accountnumber_stg                AS accountnumber,
                                                                                                                                                      bctl_policyperiodbillingmethod.typecode_stg AS billing_meth_type_cd,
                                                                                                                                                      bc_invoicestream.updatetime_stg             AS updatetime,
                                                                                                                                                      ''ACCTTOBILL''                                   agmt_rltd_rsn_cd,
                                                                                                                                                      bctl_periodicity.typecode_stg               AS typecode,
                                                                                                                                                      NULL                                        AS termnumber,
                                                                                                                                                      bc_invoice.eventdate_stg                    AS eventdate,
                                                                                                                                                      bc_invoice.paymentduedate_stg               AS paymentduedate,
                                                                                                                                                      bc_invoicestream.retired_stg                   retired_invoicestream,
                                                                                                                                                      0                                           AS retired_account,
                                                                                                                                                      bc_policyperiod.retired_stg                    retired_policyperiod,
                                                                                                                                                      bc_invoicestream.updatetime_stg                updatetime_agmtrltd,
                                                                                                                                                      ( $start_dttm )                             AS start_dttm,
                                                                                                                                                      ( $end_dttm )                               AS end_dttm,
                                                                                                                                                      CASE
                                                                                                                                                                      WHEN bc_policyperiod.updatetime_stg > bc_invoicestream.updatetime_stg THEN bc_policyperiod.updatetime_stg
                                                                                                                                                                      ELSE bc_invoicestream.updatetime_stg
                                                                                                                                                      END AS inv_trans_strt_dttm
                                                                                                                                      FROM            db_t_prod_stag.bc_invoicestream
                                                                                                                                      inner join      db_t_prod_stag.bc_account
                                                                                                                                      ON              bc_account.id_stg = bc_invoicestream.accountid_stg
                                                                                                                                                      /****************************************************************New Join*****************************************************************/
                                                                                                                                      left outer join db_t_prod_stag.bc_invoice
                                                                                                                                      ON              bc_invoicestream.id_stg = bc_invoice.invoicestreamid_stg
                                                                                                                                                      /*****************************************************************************************************************************************/
                                                                                                                                      left outer join db_t_prod_stag.bc_acctpmntinst
                                                                                                                                      ON              bc_account.id_stg = bc_acctpmntinst.ownerid_stg
                                                                                                                                      left outer join db_t_prod_stag.bctl_billinglevel
                                                                                                                                      ON              bc_account.billinglevel_stg = bctl_billinglevel.id_stg
                                                                                                                                      left outer join db_t_prod_stag.bctl_invoicedeliverymethod
                                                                                                                                      ON              bctl_invoicedeliverymethod.id_stg = bc_account.invoicedeliverytype_stg
                                                                                                                                      left outer join db_t_prod_stag.bc_policy
                                                                                                                                      ON              bc_invoicestream.policyid_stg = bc_policy.id_stg
                                                                                                                                      left outer join db_t_prod_stag.bc_policyperiod
                                                                                                                                      ON              bc_policyperiod.policyid_stg = bc_policy.id_stg
                                                                                                                                      left outer join db_t_prod_stag.bctl_policyperiodbillingmethod
                                                                                                                                      ON              bctl_policyperiodbillingmethod.id_stg = bc_policyperiod.billingmethod_stg
                                                                                                                                      left outer join db_t_prod_stag.bctl_periodicity
                                                                                                                                      ON              bc_invoicestream.periodicity_stg = bctl_periodicity.id_stg
                                                                                                                                      left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                                                                      ON              bc_paymentinstrument.id_stg = coalesce(bc_invoicestream.overridingpaymentinstrumentid_stg, bc_acctpmntinst.foreignentityid_stg)
                                                                                                                                      left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                                                                      ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg
                                                                                                                                      WHERE           (
                                                                                                                                                                      bc_invoicestream.updatetime_stg > ( $start_dttm)
                                                                                                                                                      AND             bc_invoicestream.updatetime_stg <= ( $end_dttm ) )
                                                                                                                                      OR              (
                                                                                                                                                                      bc_policyperiod.updatetime_stg > ( $start_dttm)
                                                                                                                                                      AND             bc_policyperiod.updatetime_stg <= ( $end_dttm ) ) ) x
                                                                                                      GROUP BY        billingreferencenumber_alfa_x ,
                                                                                                                      invoice_stream_type_cd_x ,
                                                                                                                      statement_mail_type_cd_x ,
                                                                                                                      transmittal_mode_type_cd_x ,
                                                                                                                      policynumber_x,
                                                                                                                      accountnumber_x,
                                                                                                                      billing_meth_type_cd_x,
                                                                                                                      agmt_rltd_rsn_cd,
                                                                                                                      stmt_cycl_cd_x,
                                                                                                                      termnumber_x,
                                                                                                                      retired_invoicestream_x,
                                                                                                                      retired_account_x,
                                                                                                                      retired_policyperiod_x,
                                                                                                                      start_dttm_x,
                                                                                                                      end_dttm_x,
                                                                                                                      x.updatetime_agmtrltd
                                                                                                                      /* , */
                                                                                                                      /* cancellationdate_y, periodstart_y */
                                                                                      ) z
                                                                            left join
                                                                                      (
                                                                                             SELECT cancellationdate_stg AS cancellationdate ,
                                                                                                    periodstart_stg      AS periodstart,
                                                                                                    publicid_stg         AS publicid,
                                                                                                    policynumber_stg     AS policynumber
                                                                                             FROM   (
                                                                                                               SELECT     pol.policynumber_stg,
                                                                                                                          pol.cancellationdate_stg  AS cancellationdate_stg,
                                                                                                                          pol.editeffectivedate_stg AS periodstart_stg,
                                                                                                                          pol.modelnumber_stg,
                                                                                                                          pol.termnumber_stg,
                                                                                                                          pol.publicid_stg ,
                                                                                                                          row_number() over(PARTITION BY pol.policynumber_stg,pol.termnumber_stg ORDER BY pol.policynumber_stg,pol.modelnumber_stg DESC) AS r
                                                                                                               FROM       db_t_prod_stag.pc_policyperiod pol
                                                                                                               inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                               ON         pol.status_stg=pctl_policyperiodstatus.id_stg
                                                                                                               WHERE      pctl_policyperiodstatus.typecode_stg=''Bound'' ) a
                                                                                             WHERE  r=1
                                                                                                    /* order by 1-- */
                                                                                      ) lkp_y
                                                                            ON        z.policynumber_x = lkp_y.policynumber ) agmt_invoicestream_x 
                                                                            
                                                  WHERE           agmt_rltd_rsn_cd = ''ACCTTOBILL'' 
                                                  qualify row_number() over( PARTITION BY accountnumber_x, billingreferencenumber_alfa_x, agmt_rltd_rsn_cd ORDER BY updatetime_agmtrltd_max_x DESC) = 1
                                                  ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_pc_policyperiod.billingreferencenumber_alfa AS billingreferencenumber_alfa,
                sq_pc_policyperiod.pol_acc                     AS pol_acc,
                sq_pc_policyperiod.agmt_rltd_rsn_cd            AS agmt_rltd_rsn_cd,
                sq_pc_policyperiod.pc_src_cd                   AS pc_src_cd,
                sq_pc_policyperiod.bc_src_cd                   AS bc_src_cd,
                sq_pc_policyperiod.periodstart                 AS periodstart,
                sq_pc_policyperiod.cancellationdate            AS cancellationdate,
                sq_pc_policyperiod.retired                     AS retired,
                sq_pc_policyperiod.updatetime_agmt_rltd        AS updatetime_agmt_rltd,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.billingreferencenumber_alfa                      AS billingreferencenumber_alfa,
                      exp_pass_from_source.pol_acc                                          AS pol_acc,
                      exp_pass_from_source.periodstart                                      AS agmt_rltd_strt_dt,
                      ''INV''                                                                 AS out_agmt_type_cd_inv,
                      ''ACT''                                                                 AS out_agmt_type_cd_account,
                      exp_pass_from_source.agmt_rltd_rsn_cd                                 AS out_agmt_rltd_rsn_cd,
                      exp_pass_from_source.cancellationdate                                 AS out_agmt_rltd_end_dt,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                      current_timestamp                                                     AS out_edw_strt_dttm,
                      $prcs_id                                                              AS out_prcs_id,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                      AS out_pc_src_cd,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                                            AS out_bc_src_dd,
                      exp_pass_from_source.updatetime_agmt_rltd                             AS trans_strt_dttm,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                      exp_pass_from_source.retired                                          AS retired,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.pc_src_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_2
            ON        lkp_2.src_idntftn_val = exp_pass_from_source.bc_src_cd 
            qualify rnk = 1 );
  -- Component LKP_AGMT_ACT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_act AS
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
            ON        lkp.nk_src_key = exp_data_transformation.pol_acc
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_cd_account 
            qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC) 
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
                               FROM     db_t_prod_core.agmt qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
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
             inner join lkp_agmt_act
             ON         exp_data_transformation.source_record_id = lkp_agmt_act.source_record_id
             inner join lkp_agmt_inv
             ON         lkp_agmt_act.source_record_id = lkp_agmt_inv.source_record_id
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
                                 WHERE    agmt_rltd_rsn_cd =''ACCTTOBILL'' qualify row_number() over(PARTITION BY agmt_id,rltd_agmt_id,agmt_rltd_rsn_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.agmt_id = lkp_agmt_act.agmt_id
             AND        lkp.rltd_agmt_id = lkp_agmt_inv.agmt_id
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
                        md5 ( ltrim ( rtrim ( to_char ( lkp_agmt_rltd.agmt_rltd_strt_dttm , ''yyyy-mm-dd'' ) ) )
                                   || ltrim ( rtrim ( to_char ( lkp_agmt_rltd.agmt_rltd_end_dttm , ''yyyy-mm-dd'' ) ) ) ) AS orig_chksm,
                        lkp_agmt_act.agmt_id                                                                            AS agmt_id,
                        lkp_agmt_inv.agmt_id                                                                            AS rltd_agmt_id,
                        exp_data_transformation.out_agmt_rltd_rsn_cd                                                    AS out_agmt_rltd_rsn_cd,
                        exp_data_transformation.out_agmt_rltd_end_dt                                                    AS out_agmt_rltd_end_dt,
                        exp_data_transformation.agmt_rltd_strt_dt                                                       AS agmt_rltd_strt_dt,
                        exp_data_transformation.out_edw_end_dttm                                                        AS out_edw_end_dttm,
                        exp_data_transformation.out_edw_strt_dttm                                                       AS out_edw_strt_dttm,
                        exp_data_transformation.out_prcs_id                                                             AS out_prcs_id,
                        md5 ( ltrim ( rtrim ( to_char ( exp_data_transformation.agmt_rltd_strt_dt , ''yyyy-mm-dd'' ) ) )
                                   || ltrim ( rtrim ( to_char ( exp_data_transformation.out_agmt_rltd_end_dt , ''yyyy-mm-dd'' ) ) ) ) AS calc_chksm,
                        CASE
                                   WHEN orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN calc_chksm != orig_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                     AS out_ins_upd,
                        exp_data_transformation.trans_strt_dttm AS trans_strt_dttm,
                        exp_data_transformation.trans_end_dttm  AS trans_end_dttm,
                        exp_data_transformation.retired         AS retired,
                        lkp_agmt_rltd.edw_end_dttm              AS lkp_edw_end_dttm,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_agmt_act
             ON         exp_data_transformation.source_record_id = lkp_agmt_act.source_record_id
             inner join lkp_agmt_inv
             ON         lkp_agmt_act.source_record_id = lkp_agmt_inv.source_record_id
             inner join lkp_agmt_rltd
             ON         lkp_agmt_inv.source_record_id = lkp_agmt_rltd.source_record_id );
  -- Component rt_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rt_ins_upd_INSERT AS
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
         ex_ins_upd.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         ex_ins_upd.source_record_id
  FROM   ex_ins_upd
  WHERE  (
                ex_ins_upd.out_ins_upd = ''I''
         AND    ex_ins_upd.agmt_id IS NOT NULL
         AND    ex_ins_upd.rltd_agmt_id IS NOT NULL
         OR     (
                       ex_ins_upd.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    ex_ins_upd.retired = 0 )
         AND    ex_ins_upd.agmt_id IS NOT NULL
         AND    ex_ins_upd.rltd_agmt_id IS NOT NULL )
  OR     (
                ex_ins_upd.out_ins_upd = ''U''
         AND    ex_ins_upd.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rt_ins_upd_RETIRED, Type ROUTER Output Group RETIRED
 create or replace TEMPORARY TABLE rt_ins_upd_RETIRED AS
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
         ex_ins_upd.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         ex_ins_upd.source_record_id
  FROM   ex_ins_upd
  WHERE  ex_ins_upd.out_ins_upd = ''R''
  AND    ex_ins_upd.retired != 0
  AND    ex_ins_upd.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
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
                rt_ins_upd_retired.source_record_id
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
                current_timestamp                  AS edw_end_dttm,
                upd_retired.trans_strt_dttm4       AS trans_strt_dttm4,
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
                upd_insert_new.out_edw_strt_dttm1    AS out_edw_strt_dttm1,
                upd_insert_new.trans_strt_dttm1      AS trans_strt_dttm1,
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
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id, rltd_agmt_id, agmt_rltd_rsn_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY agmt_id, rltd_agmt_id, agmt_rltd_rsn_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.agmt_rltd
                         WHERE           agmt_rltd_rsn_cd =''ACCTTOBILL'' ) a

  WHERE  agmt_rltd.edw_strt_dttm = a.edw_strt_dttm
  AND    agmt_rltd.agmt_id=a.agmt_id
  AND    agmt_rltd.rltd_agmt_id=a.rltd_agmt_id
  AND    agmt_rltd.agmt_rltd_rsn_cd=a.agmt_rltd_rsn_cd
  AND    agmt_rltd.agmt_rltd_rsn_cd =''ACCTTOBILL''
  AND    cast(agmt_rltd.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(agmt_rltd.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';