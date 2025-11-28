-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_STS_ACCOUNTNUMBER_INSUPD("WORKLET_NAME" VARCHAR)
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

  -- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_STS_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_accountstatus.TYPECODE''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
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
  -- Component sq_pc_account, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_account AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS accountnumber,
                $2 AS typecode,
                $3 AS updatetime,
                $4 AS createtime,
                $5 AS expirationdate,
                $6 AS src_cd,
                $7 AS retired,
                $8 AS rnk,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT cast (pc_account.accountnumber_stg AS    VARCHAR (60)) AS accountnumber,
                                                                  cast (pctl_accountstatus.typecode_stg AS VARCHAR (60)) AS status,
                                                                  CASE
                                                                                  WHEN(
                                                                                                                  bc_account.updatetime_stg IS NULL) THEN pc_account.updatetime_stg
                                                                                  WHEN(
                                                                                                                  pc_account.updatetime_stg > bc_account.updatetime_stg) THEN pc_account.updatetime_stg
                                                                                  WHEN(
                                                                                                                  pc_account.updatetime_stg <= bc_account.updatetime_stg) THEN bc_account.updatetime_stg
                                                                  END                                                 AS pc_bc_updatetime,
                                                                  cast (pc_account.createtime_stg AS VARCHAR (60))    AS createtime,
                                                                  cast (''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS expirationdate,
                                                                  ''SRC_SYS4''                                          AS src_cd,
                                                                  pc_account.retired_stg,
                                                                  rank() over(PARTITION BY pc_account.accountnumber_stg ORDER BY pc_bc_updatetime ) AS rnk
                                                  FROM            db_t_prod_stag.pc_account
                                                  left outer join db_t_prod_stag.bc_account
                                                  ON              bc_account.accountnumber_stg = pc_account.accountnumber_stg
                                                  left outer join db_t_prod_stag.pctl_accountstatus
                                                  ON              pc_account.accountstatus_stg=pctl_accountstatus.id_stg 
                                                  
                                                  WHERE           pc_account.updatetime_stg > (:START_DTTM)
                                                  AND             pc_account.updatetime_stg <= (:END_DTTM)
                                                  qualify row_number() over(PARTITION BY pc_account.accountnumber_stg ORDER BY pc_account.createtime_stg DESC) =1
                                                   ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_pc_account.accountnumber  AS accountnumber,
                sq_pc_account.typecode       AS typecode,
                sq_pc_account.createtime     AS createtime,
                sq_pc_account.src_cd         AS src_cd,
                sq_pc_account.expirationdate AS expirationdate,
                sq_pc_account.retired        AS retired,
                sq_pc_account.rnk            AS rnk,
                sq_pc_account.source_record_id
         FROM   sq_pc_account );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.accountnumber AS accountnumber,
                      ''ACT''                              AS out_agmt_type_cd,
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                      END                             AS out_agmt_sts_cd,
                      exp_pass_from_source.createtime AS createtime,
                      CASE
                                WHEN exp_pass_from_source.createtime IS NULL THEN to_date ( ''01-01-1900'' , ''mm-dd-yyyy'' )
                                ELSE exp_pass_from_source.createtime
                      END                                                                   AS out_agmt_sts_strt_dttm,
                      ''UNK''                                                                 AS out_agmt_sts_rsn_cd,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_agmt_sts_end_dttm,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                      :prcs_id                                                              AS out_prcs_id,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                   AS out_src_cd,
                      exp_pass_from_source.retired AS retired,
                      --exp_pass_from_source.rnk     AS rnk,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.typecode
            left join lkp_teradata_etl_ref_xlat lkp_2
            ON        lkp_2.src_idntftn_val = exp_pass_from_source.typecode
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_3
            ON        lkp_3.src_idntftn_val = exp_pass_from_source.src_cd qualify rnk = 1 );
  -- Component LKP_AGMT_POL, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_pol AS
  (
            SELECT    lkp.agmt_id,
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
            ON        lkp.nk_src_key = exp_data_transformation.accountnumber
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_cd qualify rnk = 1 );
  -- Component LKP_AGMT_STS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_sts AS
  (
             SELECT     lkp.agmt_id,
                        lkp.agmt_sts_cd,
                        lkp.agmt_sts_rsn_cd,
                        lkp.agmt_sts_strt_dttm,
                        lkp.agmt_sts_end_dttm,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        exp_data_transformation.source_record_id,
                        row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_sts_cd ASC,lkp.agmt_sts_rsn_cd ASC,lkp.agmt_sts_strt_dttm ASC,lkp.agmt_sts_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
             FROM       exp_data_transformation
             inner join lkp_agmt_pol
             ON         exp_data_transformation.source_record_id = lkp_agmt_pol.source_record_id
             left join
                        (
                                 SELECT   agmt_sts.agmt_sts_rsn_cd    AS agmt_sts_rsn_cd,
                                          agmt_sts.agmt_sts_strt_dttm AS agmt_sts_strt_dttm,
                                          agmt_sts.agmt_sts_end_dttm  AS agmt_sts_end_dttm,
                                          agmt_sts.edw_strt_dttm      AS edw_strt_dttm,
                                          agmt_sts.edw_end_dttm       AS edw_end_dttm,
                                          agmt_sts.agmt_id            AS agmt_id,
                                          agmt_sts.agmt_sts_cd        AS agmt_sts_cd
                                 FROM     db_t_prod_core.agmt_sts qualify row_number() over(PARTITION BY agmt_id,agmt_sts_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.agmt_id = lkp_agmt_pol.agmt_id
             AND        lkp.agmt_sts_cd = exp_data_transformation.out_agmt_sts_cd 
             qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_sts_cd ASC,lkp.agmt_sts_rsn_cd ASC,lkp.agmt_sts_strt_dttm ASC,lkp.agmt_sts_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) 
             = 1 );
  -- Component exp_insert_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_insert_update AS
  (
             SELECT     lkp_agmt_sts.agmt_id            AS lkp_agmt_id,
                        lkp_agmt_sts.agmt_sts_cd        AS lkp_agmt_sts_cd,
                        lkp_agmt_sts.agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
                        lkp_agmt_sts.edw_strt_dttm      AS lkp_edw_strt_dttm,
                        lkp_agmt_sts.edw_end_dttm       AS lkp_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( lkp_agmt_sts.agmt_sts_strt_dttm ) )
                                   || ltrim ( rtrim ( lkp_agmt_sts.agmt_sts_rsn_cd ) )
                                   || ltrim ( rtrim ( lkp_agmt_sts.agmt_sts_end_dttm ) )
                                   || ltrim ( rtrim ( lkp_agmt_sts.agmt_sts_cd ) ) ) AS orig_chksm,
                        lkp_agmt_pol.agmt_id                                         AS agmt_id,
                        exp_data_transformation.out_agmt_sts_cd                      AS agmt_sts_cd,
                        exp_data_transformation.out_agmt_sts_strt_dttm               AS agmt_sts_strt_dttm,
                        exp_data_transformation.out_agmt_sts_rsn_cd                  AS agmt_sts_rsn_cd,
                        exp_data_transformation.out_agmt_sts_end_dttm                AS agmt_sts_end_dttm,
                        exp_data_transformation.out_prcs_id                          AS prcs_id,
                        md5 ( ltrim ( rtrim ( exp_data_transformation.out_agmt_sts_strt_dttm ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_agmt_sts_rsn_cd ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_agmt_sts_end_dttm ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_agmt_sts_cd ) ) ) AS calc_chksm,
                        CASE
                                   WHEN orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN orig_chksm != calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                      AS out_insert_update_flag,
                        exp_data_transformation.out_edw_end_dttm AS out_edw_end_dttm,
                        current_timestamp                        AS out_edw_strt_dttm,
                        exp_data_transformation.createtime       AS createtime,
                        exp_data_transformation.retired          AS retired,
                        exp_data_transformation.rnk              AS rnk,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_agmt_pol
             ON         exp_data_transformation.source_record_id = lkp_agmt_pol.source_record_id
             inner join lkp_agmt_sts
             ON         lkp_agmt_pol.source_record_id = lkp_agmt_sts.source_record_id );
  -- Component rtr_AGMT_INSERT, Type ROUTER Output Group INSERT
  create or replace view rtr_agmt_insert as
  SELECT exp_insert_update.lkp_agmt_id            AS lkp_agmt_id,
         exp_insert_update.lkp_agmt_sts_cd        AS lkp_agmt_sts_cd,
         exp_insert_update.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
         exp_insert_update.agmt_id                AS agmt_id,
         exp_insert_update.agmt_sts_cd            AS agmt_sts_cd,
         exp_insert_update.agmt_sts_strt_dttm     AS agmt_sts_strt_dttm,
         exp_insert_update.agmt_sts_rsn_cd        AS agmt_sts_rsn_cd,
         exp_insert_update.agmt_sts_end_dttm      AS agmt_sts_end_dttm,
         exp_insert_update.prcs_id                AS prcs_id,
         exp_insert_update.out_insert_update_flag AS out_insert_update_flag,
         exp_insert_update.out_edw_end_dttm       AS out_edw_end_dttm,
         exp_insert_update.out_edw_strt_dttm      AS out_edw_strt_dttm,
         exp_insert_update.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
         exp_insert_update.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_insert_update.retired                AS retired,
         exp_insert_update.rnk                    AS rnk,
         exp_insert_update.source_record_id
  FROM   exp_insert_update
  WHERE  exp_insert_update.out_insert_update_flag = ''I''
  AND    exp_insert_update.agmt_id IS NOT NULL
  OR     exp_insert_update.out_insert_update_flag = ''U''
  OR     (
                exp_insert_update.retired = 0
         AND    exp_insert_update.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_AGMT_RETIRE, Type ROUTER Output Group RETIRE
  create or replace view rtr_agmt_retire as
  SELECT exp_insert_update.lkp_agmt_id            AS lkp_agmt_id,
         exp_insert_update.lkp_agmt_sts_cd        AS lkp_agmt_sts_cd,
         exp_insert_update.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
         exp_insert_update.agmt_id                AS agmt_id,
         exp_insert_update.agmt_sts_cd            AS agmt_sts_cd,
         exp_insert_update.agmt_sts_strt_dttm     AS agmt_sts_strt_dttm,
         exp_insert_update.agmt_sts_rsn_cd        AS agmt_sts_rsn_cd,
         exp_insert_update.agmt_sts_end_dttm      AS agmt_sts_end_dttm,
         exp_insert_update.prcs_id                AS prcs_id,
         exp_insert_update.out_insert_update_flag AS out_insert_update_flag,
         exp_insert_update.out_edw_end_dttm       AS out_edw_end_dttm,
         exp_insert_update.out_edw_strt_dttm      AS out_edw_strt_dttm,
         exp_insert_update.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
         exp_insert_update.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_insert_update.retired                AS retired,
         exp_insert_update.rnk                    AS rnk,
         exp_insert_update.source_record_id
  FROM   exp_insert_update
  WHERE  exp_insert_update.out_insert_update_flag = ''R''
  AND    exp_insert_update.retired != 0
  AND    exp_insert_update.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_AGMT_ins_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_insert.agmt_id            AS agmt_id,
                rtr_agmt_insert.agmt_sts_cd        AS agmt_sts_cd,
                rtr_agmt_insert.agmt_sts_strt_dttm AS agmt_sts_strt_dttm,
                rtr_agmt_insert.agmt_sts_rsn_cd    AS agmt_sts_rsn_cd,
                rtr_agmt_insert.prcs_id            AS prcs_id,
                rtr_agmt_insert.agmt_sts_end_dttm  AS agmt_sts_end_dttm1,
                rtr_agmt_insert.out_edw_end_dttm   AS out_edw_end_dttm1,
                rtr_agmt_insert.out_edw_strt_dttm  AS out_edw_strt_dttm,
                rtr_agmt_insert.retired            AS retired1,
                rtr_agmt_insert.rnk                AS rnk1,
                0                                  AS update_strategy_action,
                rtr_agmt_insert.source_record_id
         FROM   rtr_agmt_insert );
  -- Component upd_AGMT_upd1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_upd1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_retire.lkp_agmt_id            AS lkp_agmt_id,
                rtr_agmt_retire.lkp_agmt_sts_cd        AS lkp_agmt_sts_cd,
                rtr_agmt_retire.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
                rtr_agmt_retire.agmt_sts_end_dttm      AS agmt_sts_end_dttm,
                rtr_agmt_retire.prcs_id                AS prcs_id,
                rtr_agmt_retire.agmt_sts_rsn_cd        AS agmt_sts_rsn_cd3,
                rtr_agmt_retire.out_edw_end_dttm       AS out_edw_end_dttm3,
                rtr_agmt_retire.out_edw_strt_dttm      AS out_edw_strt_dttm,
                rtr_agmt_retire.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm3,
                1                                      AS update_strategy_action,
                rtr_agmt_retire.source_record_id
         FROM   rtr_agmt_retire );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_agmt_ins_new.agmt_id            AS agmt_id,
                upd_agmt_ins_new.agmt_sts_cd        AS agmt_sts_cd,
                upd_agmt_ins_new.agmt_sts_strt_dttm AS agmt_sts_strt_dttm,
                upd_agmt_ins_new.agmt_sts_rsn_cd    AS agmt_sts_rsn_cd,
                upd_agmt_ins_new.agmt_sts_end_dttm1 AS agmt_sts_end_dttm,
                upd_agmt_ins_new.prcs_id            AS prcs_id,
                CASE
                       WHEN upd_agmt_ins_new.retired1 = 0 THEN upd_agmt_ins_new.out_edw_end_dttm1
                       ELSE current_timestamp
                END                                                                                      AS out_edw_end_dttm11,
                dateadd(''second'', ( 2 * ( upd_agmt_ins_new.rnk1 - 1 ) ), upd_agmt_ins_new.out_edw_strt_dttm) AS out_edw_strt_dttm1,
                upd_agmt_ins_new.source_record_id
         FROM   upd_agmt_ins_new );
  -- Component exp_pass_to_target_upd_upd1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd_upd1 AS
  (
         SELECT upd_agmt_upd1.lkp_agmt_id        AS lkp_agmt_id,
                upd_agmt_upd1.lkp_agmt_sts_cd    AS lkp_agmt_sts_cd,
                upd_agmt_upd1.lkp_edw_strt_dttm3 AS lkp_agmt_sts_strt_dttm,
                current_timestamp                AS out_createtime,
                upd_agmt_upd1.source_record_id
         FROM   upd_agmt_upd1 );
  -- Component AGMT_STS_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.agmt_sts
              (
                          agmt_id,
                          agmt_sts_cd,
                          agmt_sts_strt_dttm,
                          agmt_sts_rsn_cd,
                          agmt_sts_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_ins.agmt_id            AS agmt_id,
         exp_pass_to_target_ins.agmt_sts_cd        AS agmt_sts_cd,
         exp_pass_to_target_ins.agmt_sts_strt_dttm AS agmt_sts_strt_dttm,
         exp_pass_to_target_ins.agmt_sts_rsn_cd    AS agmt_sts_rsn_cd,
         exp_pass_to_target_ins.agmt_sts_end_dttm  AS agmt_sts_end_dttm,
         exp_pass_to_target_ins.prcs_id            AS prcs_id,
         exp_pass_to_target_ins.out_edw_strt_dttm1 AS edw_strt_dttm,
         exp_pass_to_target_ins.out_edw_end_dttm11 AS edw_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component AGMT_STS_ins_new, Type Post SQL
  UPDATE db_t_prod_core.agmt_sts
  SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT agmt_sts.agmt_id,
                                         agmt_sts.edw_strt_dttm,
                                         agmt_sts_cd,
                                         max(agmt_sts.edw_strt_dttm) over (PARTITION BY agmt_sts.agmt_id ORDER BY agmt_sts.edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.agmt_sts
                         WHERE           agmt_id IN
                                         (
                                                  SELECT   agmt_id
                                                  FROM     db_t_prod_core.agmt
                                                  WHERE    agmt_type_cd = ''act''
                                                  GROUP BY agmt_id) ) a

  WHERE  agmt_sts.agmt_id=a.agmt_id
  AND    agmt_sts.edw_strt_dttm=a.edw_strt_dttm
  AND    lead1 IS NOT NULL;
  
  -- Component AGMT_STS_retire, Type TARGET
  merge
  INTO         db_t_prod_core.agmt_sts
  USING        exp_pass_to_target_upd_upd1
  ON (
                            agmt_sts.agmt_id = exp_pass_to_target_upd_upd1.lkp_agmt_id
               AND          agmt_sts.edw_strt_dttm = exp_pass_to_target_upd_upd1.lkp_agmt_sts_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_pass_to_target_upd_upd1.lkp_agmt_id,
         agmt_sts_cd = exp_pass_to_target_upd_upd1.lkp_agmt_sts_cd,
         edw_strt_dttm = exp_pass_to_target_upd_upd1.lkp_agmt_sts_strt_dttm,
         edw_end_dttm = exp_pass_to_target_upd_upd1.out_createtime;

END;
';