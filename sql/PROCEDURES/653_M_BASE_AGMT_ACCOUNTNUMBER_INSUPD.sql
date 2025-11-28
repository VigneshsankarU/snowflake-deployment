-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ACCOUNTNUMBER_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN
run_id :=   (SELECT run_id   FROM control_run_id where worklet_name=:worklet_name order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
START_DTTM:= (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'');


  -- Component LKP_TERADATA_ETL_REF_XLAT_ACCNT_SYBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_accnt_sybtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm=''INSRNC_BUSN_TYPE''
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
  -- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_AGMT_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_insrnc_agmt_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INSRNC_AGMT_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_accountorgtype.typecode'',
                                                         ''derived'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                          ''DS'')
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
         WHERE  agmt_type_cd=''ACT'' );
  -- Component sq_pc_account, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_account AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS accountnumber,
                $2  AS accountorgtype,
                $3  AS accountname,
                $4  AS src_cd,
                $5  AS accounttype,
                $6  AS originationdate,
                $7  AS closedate,
                $8  AS updatetime,
                $9  AS retired,
                $10 AS agmt_type,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT pc.accountnumber_stg             AS accountnumber,
                                                                  pctl_accountorgtype.typecode_stg AS accountorgtype,
                                                                  pc.name_stg                      AS accountname,
                                                                  ''SRC_SYS4''                       AS srccd,
                                                                  /* bc_account.AccountType_stg as AccountType, */
                                                                  bctl_accounttype.typecode_stg                   AS accounttype,
                                                                  pc.createtime_stg                               AS originationdate,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS closedate,
                                                                  CASE
                                                                                  WHEN pc.updatetime_stg > coalesce(bc_account.updatetime_stg ,cast(''1900-01-01 00:00:00.000000'' AS timestamp))THEN pc.updatetime_stg
                                                                                  ELSE bc_account.updatetime_stg
                                                                  END            AS updatetime,
                                                                  pc.retired_stg AS retired,
                                                                  ''AGMT_TYPE1''   AS agmt_type
                                                  FROM            (
                                                                                  SELECT          pc_account.updatetime_stg,
                                                                                                  a.name_stg,
                                                                                                  pc_account.id_stg,
                                                                                                  pc_account.retired_stg,
                                                                                                  pc_account.accountnumber_stg,
                                                                                                  pc_account.createtime_stg,
                                                                                                  pc_account.accountorgtype_stg
                                                                                  FROM            db_t_prod_stag.pc_account
                                                                                  left outer join db_t_prod_stag.pc_acctholderedge
                                                                                  ON              pc_acctholderedge.ownerid_stg = pc_account.id_stg
                                                                                  left outer join db_t_prod_stag.pc_contact a
                                                                                  ON              a.id_stg = pc_acctholderedge.foreignentityid_stg
                                                                                  WHERE           pc_account.updatetime_stg> (:START_DTTM)
                                                                                  AND             pc_account.updatetime_stg <= (:END_DTTM) ) pc
                                                  left outer join db_t_prod_stag.bc_account
                                                  ON              bc_account.accountnumber_stg = pc.accountnumber_stg
                                                  left outer join db_t_prod_stag.bctl_accounttype
                                                  ON              bctl_accounttype.id_stg = bc_account.accounttype_stg
                                                  left outer join db_t_prod_stag.pctl_accountorgtype
                                                  ON              pctl_accountorgtype.id_stg = pc.accountorgtype_stg
                                                  WHERE           pc.retired_stg=0
                                                  UNION
                                                  SELECT DISTINCT bc_account.accountnumber_stg AS accountnumber,
                                                                  typecode_stg                 AS accountorgtype,
                                                                  bc_account.accountname_stg   AS accountname,
                                                                  ''SRC_SYS5''                   AS srccd,
                                                                  typecode_stg                 AS accounttype,
                                                                  bc_account.createtime_stg    AS originationdate,
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  bc_account.closedate_stg IS NULL) THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp)
                                                                                  ELSE bc_account.closedate_stg
                                                                  END                       AS closedate,
                                                                  bc_account.updatetime_stg AS updatetime,
                                                                  bc_account.retired_stg    AS retired,
                                                                  ''AGMT_TYPE1''              AS agmt_type
                                                  FROM            (
                                                                                  SELECT          bc_account.createtime_stg,
                                                                                                  bc_account.accounttype_stg,
                                                                                                  bc_account.closedate_stg,
                                                                                                  bc_account.updatetime_stg,
                                                                                                  bc_account.retired_stg,
                                                                                                  bc_account.accountnumber_stg,
                                                                                                  bc_account.accountname_stg,
                                                                                                  bctl_accounttype.typecode_stg,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  pc_account.accountnumber_stg IS NULL
                                                                                                                                  OR              pc_account.retired_stg <>0) THEN ''N''
                                                                                                                  ELSE ''Y''
                                                                                                  END AS pc_account_existflag_stg
                                                                                  FROM            db_t_prod_stag.bc_account
                                                                                  left outer join db_t_prod_stag.bctl_accounttype
                                                                                  ON              bc_account.accounttype_stg=bctl_accounttype.id_stg
                                                                                  left outer join db_t_prod_stag.pc_account
                                                                                  ON              pc_account.accountnumber_stg = bc_account.accountnumber_stg
                                                                                  WHERE           bc_account.updatetime_stg > (:START_DTTM)
                                                                                  AND             bc_account.updatetime_stg <= (:END_DTTM) ) bc_account
                                                  WHERE           bc_account.pc_account_existflag_stg=''N'' ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_pc_account.accountnumber                         AS accountnumber,
                sq_pc_account.accountorgtype                        AS accountorgtype,
                sq_pc_account.accountname                           AS accountname,
                sq_pc_account.src_cd                                AS src_cd,
                sq_pc_account.accounttype                           AS accounttype,
                sq_pc_account.originationdate                       AS originationdate,
                to_char ( sq_pc_account.updatetime , ''YYYY-MM-DD'' ) AS v_updatetime,
                CASE
                       WHEN sq_pc_account.updatetime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                       ELSE sq_pc_account.updatetime
                END                     AS o_updatetime,
                sq_pc_account.retired   AS retired,
                sq_pc_account.agmt_type AS agmt_type,
                sq_pc_account.closedate AS closedate,
                sq_pc_account.source_record_id
         FROM   sq_pc_account );
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_type AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm = ''derived''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                                              ''GW'')
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.agmt_type qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_SBTYPE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_sbtype AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm = ''AGMT_SBTYPE''
                             AND    teradata_etl_ref_xlat. src_idntftn_nm IN (''pctl_accountorgtype.typecode'',
                                                                              ''bctl_accounttype.typecode'')
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.accountorgtype qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_BILG_METH_TYPE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_bilg_meth_type AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''BILG_METH_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bctl_accounttype.typecode''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.accounttype qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     exp_pass_from_source.accountnumber  AS accountnumber,
                        exp_pass_from_source.accountorgtype AS accountorgtype,
                        CASE
                                   WHEN lkp_1.tgt_idntftn_val
                                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_AGMT_SBTYPE */
                                              IS NULL THEN ''UNK''
                                   ELSE lkp_2.tgt_idntftn_val
                                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_AGMT_SBTYPE */
                        END                              AS out_accountorg_type,
                        exp_pass_from_source.accountname AS accountname,
                        exp_pass_from_source.src_cd      AS src_cd,
                        lkp_3.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                            AS o_src_cd,
                        lkp_teradata_etl_ref_xlat_agmt_type.tgt_idntftn_val AS agmt_type_cd,
                        ''UNK''                                               AS out_agmt_cur_sts_cd,
                        NULL                                                AS out_agmt_cur_sts_rsn_cd,
                        ''UNK''                                               AS out_agmt_obtnd_cd,
                        ''UNK''                                               AS out_agmt_objtv_type_cd,
                        ''UNK''                                               AS out_mkt_risk_type_cd,
                        ''UNK''                                               AS out_ntwk_srvc_agmt_type_cd,
                        ''UNK''                                               AS out_frmlty_type_cd,
                        ''UNK''                                               AS out_agmt_idntftn_cd,
                        ''UNK''                                               AS out_trmtn_type_cd,
                        ''UNK''                                               AS out_int_pmt_meth_cd,
                        1                                                   AS out_cntrl_id,
                        :PRCS_ID                                            AS out_prcs_id,
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat_bilg_meth_type.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat_bilg_meth_type.tgt_idntftn_val
                        END                                  AS o_accounttype,
                        exp_pass_from_source.originationdate AS agmt_opn_dttm,
                        exp_pass_from_source.o_updatetime    AS updatetime,
                        exp_pass_from_source.retired         AS retired,
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat_agmt_sbtype.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat_agmt_sbtype.tgt_idntftn_val
                        END                            AS o_agmt_sbtype,
                        exp_pass_from_source.closedate AS closedate,
                        exp_pass_from_source.source_record_id,
                        row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
             FROM       exp_pass_from_source
             inner join lkp_teradata_etl_ref_xlat_agmt_type
             ON         exp_pass_from_source.source_record_id = lkp_teradata_etl_ref_xlat_agmt_type.source_record_id
             inner join lkp_teradata_etl_ref_xlat_agmt_sbtype
             ON         lkp_teradata_etl_ref_xlat_agmt_type.source_record_id = lkp_teradata_etl_ref_xlat_agmt_sbtype.source_record_id
             inner join lkp_teradata_etl_ref_xlat_bilg_meth_type
             ON         lkp_teradata_etl_ref_xlat_agmt_sbtype.source_record_id = lkp_teradata_etl_ref_xlat_bilg_meth_type.source_record_id
             left join  lkp_teradata_etl_ref_xlat_insrnc_agmt_sbtype lkp_1
             ON         lkp_1.src_idntftn_val = exp_pass_from_source.accountorgtype
             left join  lkp_teradata_etl_ref_xlat_insrnc_agmt_sbtype lkp_2
             ON         lkp_2.src_idntftn_val = exp_pass_from_source.accountorgtype
             left join  lkp_teradata_etl_ref_xlat_src_cd lkp_3
             ON         lkp_3.src_idntftn_val = exp_pass_from_source.src_cd 
			 qualify row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) 
			 = 1 );
  -- Component LKP_AGMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt AS
  (
            SELECT    lkp.agmt_id,
                      lkp.agmt_opn_dttm,
                      lkp.agmt_plnd_expn_dttm,
                      lkp.agmt_type_cd,
                      lkp.agmt_cur_sts_cd,
                      lkp.agmt_sbtype_cd,
                      lkp.insrnc_agmt_sbtype_cd,
                      lkp.bilg_meth_type_cd,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      lkp.nk_src_key,
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
                               WHERE    agmt_type_cd=''ACT'' qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_src_key = exp_data_transformation.accountnumber
            AND       lkp.agmt_type_cd = exp_data_transformation.agmt_type_cd qualify rnk = 1 );
  -- Component exp_for_cdc_check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_for_cdc_check AS
  (
             SELECT     lkp_agmt.agmt_id                                   AS lkp_agmt_id,
                        exp_data_transformation.accountnumber              AS accountnumber,
                        exp_data_transformation.out_accountorg_type        AS accountorgtype,
                        exp_data_transformation.accountname                AS accountname,
                        exp_data_transformation.agmt_type_cd               AS out_agmt_type_cd,
                        exp_data_transformation.out_agmt_cur_sts_cd        AS out_agmt_cur_sts_cd,
                        exp_data_transformation.out_agmt_cur_sts_rsn_cd    AS out_agmt_cur_sts_rsn_cd,
                        exp_data_transformation.out_agmt_obtnd_cd          AS out_agmt_obtnd_cd,
                        exp_data_transformation.o_agmt_sbtype              AS out_agmt_sbtype_cd,
                        exp_data_transformation.out_agmt_objtv_type_cd     AS out_agmt_objtv_type_cd,
                        exp_data_transformation.out_mkt_risk_type_cd       AS out_mkt_risk_type_cd,
                        exp_data_transformation.out_ntwk_srvc_agmt_type_cd AS out_ntwk_srvc_agmt_type_cd,
                        exp_data_transformation.out_frmlty_type_cd         AS out_frmlty_type_cd,
                        exp_data_transformation.out_agmt_idntftn_cd        AS out_agmt_idntftn_cd,
                        exp_data_transformation.out_trmtn_type_cd          AS out_trmtn_type_cd,
                        exp_data_transformation.out_int_pmt_meth_cd        AS out_int_pmt_meth_cd,
                        exp_data_transformation.out_cntrl_id               AS out_cntrl_id,
                        exp_data_transformation.out_prcs_id                AS out_prcs_id,
                        exp_data_transformation.o_accounttype              AS out_bilg_meth_type_cd,
                        exp_data_transformation.agmt_opn_dttm              AS in_agmt_opn_dttm,
                        exp_data_transformation.updatetime                 AS updatetime,
                        lkp_agmt.edw_strt_dttm                             AS edw_strt_dttm,
                        lkp_agmt.edw_end_dttm                              AS lkp_edw_end_dttm,
                        exp_data_transformation.o_src_cd                   AS in_src_cd,
                        exp_data_transformation.retired                    AS retired,
                        md5 ( ltrim ( rtrim ( upper ( exp_data_transformation.agmt_type_cd ) ) )
                                   || ltrim ( rtrim ( upper ( exp_data_transformation.out_agmt_cur_sts_cd ) ) )
                                   || ltrim ( rtrim ( upper ( exp_data_transformation.o_agmt_sbtype ) ) )
                                   || ltrim ( rtrim ( upper ( exp_data_transformation.out_accountorg_type ) ) )
                                   || ltrim ( rtrim ( exp_data_transformation.o_accounttype ) )
                                   || ltrim ( rtrim ( exp_data_transformation.agmt_opn_dttm ) )
                                   || ltrim ( rtrim ( exp_data_transformation.closedate ) ) ) AS v_md5_src,
                        md5 ( ltrim ( rtrim ( upper ( lkp_agmt.agmt_type_cd ) ) )
                                   || ltrim ( rtrim ( upper ( lkp_agmt.agmt_cur_sts_cd ) ) )
                                   || ltrim ( rtrim ( upper ( lkp_agmt.agmt_sbtype_cd ) ) )
                                   || ltrim ( rtrim ( upper ( lkp_agmt.insrnc_agmt_sbtype_cd ) ) )
                                   || ltrim ( rtrim ( lkp_agmt.bilg_meth_type_cd ) )
                                   || ltrim ( rtrim ( lkp_agmt.agmt_opn_dttm ) )
                                   || ltrim ( rtrim ( lkp_agmt.agmt_plnd_expn_dttm ) ) ) AS v_md5_tgt,
                        CASE
                                   WHEN v_md5_tgt IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_md5_src = v_md5_tgt THEN ''R''
                                                         ELSE ''U''
                                              END
                        END                                                                    AS o_insert_update,
                        current_timestamp                                                      AS startdate,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS enddate,
                        lkp_agmt.nk_src_key                                                    AS lkp_nk_src_key,
                        lkp_1.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DATA_SRC */
                                                          AS agmt_src_cd,
                        exp_data_transformation.closedate AS closedate,
                        exp_data_transformation.source_record_id,
                        row_number() over (PARTITION BY exp_data_transformation.source_record_id ORDER BY exp_data_transformation.source_record_id) AS rnk
             FROM       exp_data_transformation
             inner join lkp_agmt
             ON         exp_data_transformation.source_record_id = lkp_agmt.source_record_id
             left join  lkp_teradata_etl_ref_xlat_data_src lkp_1
             ON         lkp_1.src_idntftn_val = ''DATA_SRC_TYPE2'' 
			 qualify row_number() over (PARTITION BY exp_data_transformation.source_record_id ORDER BY exp_data_transformation.source_record_id) 
			 = 1 );
  -- Component rtr_AGMT_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_AGMT_INSERT as
  SELECT exp_for_cdc_check.lkp_agmt_id                AS agmt_id,
         exp_for_cdc_check.accountnumber              AS accountnumber,
         exp_for_cdc_check.accountorgtype             AS accountorgtype,
         exp_for_cdc_check.accountname                AS accountname,
         exp_for_cdc_check.out_agmt_type_cd           AS agmt_type_cd,
         exp_for_cdc_check.out_agmt_cur_sts_cd        AS agmt_cur_sts_cd,
         exp_for_cdc_check.out_agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
         exp_for_cdc_check.out_agmt_obtnd_cd          AS agmt_obtnd_cd,
         exp_for_cdc_check.out_agmt_sbtype_cd         AS agmt_sbtype_cd,
         exp_for_cdc_check.out_agmt_objtv_type_cd     AS agmt_objtv_type_cd,
         exp_for_cdc_check.out_mkt_risk_type_cd       AS mkt_risk_type_cd,
         exp_for_cdc_check.out_ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
         exp_for_cdc_check.out_frmlty_type_cd         AS frmlty_type_cd,
         exp_for_cdc_check.out_agmt_idntftn_cd        AS agmt_idntftn_cd,
         exp_for_cdc_check.out_trmtn_type_cd          AS trmtn_type_cd,
         exp_for_cdc_check.out_int_pmt_meth_cd        AS int_pmt_meth_cd,
         exp_for_cdc_check.out_bilg_meth_type_cd      AS bilg_meth_type_cd,
         exp_for_cdc_check.out_cntrl_id               AS cntrl_id,
         exp_for_cdc_check.out_prcs_id                AS prcs_id,
         exp_for_cdc_check.o_insert_update            AS o_insert_update,
         exp_for_cdc_check.startdate                  AS startdate,
         exp_for_cdc_check.enddate                    AS enddate,
         exp_for_cdc_check.edw_strt_dttm              AS edw_strt_dttm,
         exp_for_cdc_check.in_src_cd                  AS in_src_cd,
         exp_for_cdc_check.in_agmt_opn_dttm           AS in_agmt_opn_dttm,
         exp_for_cdc_check.updatetime                 AS updatetime,
         exp_for_cdc_check.retired                    AS retired,
         exp_for_cdc_check.lkp_edw_end_dttm           AS lkp_edw_end_dttm,
         exp_for_cdc_check.lkp_nk_src_key             AS lkp_nk_src_key,
         exp_for_cdc_check.agmt_src_cd                AS agmt_src_cd,
         exp_for_cdc_check.closedate                  AS closedate,
         exp_for_cdc_check.source_record_id
  FROM   exp_for_cdc_check
  WHERE  exp_for_cdc_check.o_insert_update = ''I''
  OR     (
                exp_for_cdc_check.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_for_cdc_check.retired = 0 );
  
  -- Component rtr_AGMT_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_AGMT_RETIRED as
  SELECT exp_for_cdc_check.lkp_agmt_id                AS agmt_id,
         exp_for_cdc_check.accountnumber              AS accountnumber,
         exp_for_cdc_check.accountorgtype             AS accountorgtype,
         exp_for_cdc_check.accountname                AS accountname,
         exp_for_cdc_check.out_agmt_type_cd           AS agmt_type_cd,
         exp_for_cdc_check.out_agmt_cur_sts_cd        AS agmt_cur_sts_cd,
         exp_for_cdc_check.out_agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
         exp_for_cdc_check.out_agmt_obtnd_cd          AS agmt_obtnd_cd,
         exp_for_cdc_check.out_agmt_sbtype_cd         AS agmt_sbtype_cd,
         exp_for_cdc_check.out_agmt_objtv_type_cd     AS agmt_objtv_type_cd,
         exp_for_cdc_check.out_mkt_risk_type_cd       AS mkt_risk_type_cd,
         exp_for_cdc_check.out_ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
         exp_for_cdc_check.out_frmlty_type_cd         AS frmlty_type_cd,
         exp_for_cdc_check.out_agmt_idntftn_cd        AS agmt_idntftn_cd,
         exp_for_cdc_check.out_trmtn_type_cd          AS trmtn_type_cd,
         exp_for_cdc_check.out_int_pmt_meth_cd        AS int_pmt_meth_cd,
         exp_for_cdc_check.out_bilg_meth_type_cd      AS bilg_meth_type_cd,
         exp_for_cdc_check.out_cntrl_id               AS cntrl_id,
         exp_for_cdc_check.out_prcs_id                AS prcs_id,
         exp_for_cdc_check.o_insert_update            AS o_insert_update,
         exp_for_cdc_check.startdate                  AS startdate,
         exp_for_cdc_check.enddate                    AS enddate,
         exp_for_cdc_check.edw_strt_dttm              AS edw_strt_dttm,
         exp_for_cdc_check.in_src_cd                  AS in_src_cd,
         exp_for_cdc_check.in_agmt_opn_dttm           AS in_agmt_opn_dttm,
         exp_for_cdc_check.updatetime                 AS updatetime,
         exp_for_cdc_check.retired                    AS retired,
         exp_for_cdc_check.lkp_edw_end_dttm           AS lkp_edw_end_dttm,
         exp_for_cdc_check.lkp_nk_src_key             AS lkp_nk_src_key,
         exp_for_cdc_check.agmt_src_cd                AS agmt_src_cd,
         exp_for_cdc_check.closedate                  AS closedate,
         exp_for_cdc_check.source_record_id
  FROM   exp_for_cdc_check
  WHERE  exp_for_cdc_check.o_insert_update = ''R''
  AND    exp_for_cdc_check.retired != 0
  AND    exp_for_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_AGMT_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_AGMT_UPDATE as
  SELECT exp_for_cdc_check.lkp_agmt_id                AS agmt_id,
         exp_for_cdc_check.accountnumber              AS accountnumber,
         exp_for_cdc_check.accountorgtype             AS accountorgtype,
         exp_for_cdc_check.accountname                AS accountname,
         exp_for_cdc_check.out_agmt_type_cd           AS agmt_type_cd,
         exp_for_cdc_check.out_agmt_cur_sts_cd        AS agmt_cur_sts_cd,
         exp_for_cdc_check.out_agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
         exp_for_cdc_check.out_agmt_obtnd_cd          AS agmt_obtnd_cd,
         exp_for_cdc_check.out_agmt_sbtype_cd         AS agmt_sbtype_cd,
         exp_for_cdc_check.out_agmt_objtv_type_cd     AS agmt_objtv_type_cd,
         exp_for_cdc_check.out_mkt_risk_type_cd       AS mkt_risk_type_cd,
         exp_for_cdc_check.out_ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
         exp_for_cdc_check.out_frmlty_type_cd         AS frmlty_type_cd,
         exp_for_cdc_check.out_agmt_idntftn_cd        AS agmt_idntftn_cd,
         exp_for_cdc_check.out_trmtn_type_cd          AS trmtn_type_cd,
         exp_for_cdc_check.out_int_pmt_meth_cd        AS int_pmt_meth_cd,
         exp_for_cdc_check.out_bilg_meth_type_cd      AS bilg_meth_type_cd,
         exp_for_cdc_check.out_cntrl_id               AS cntrl_id,
         exp_for_cdc_check.out_prcs_id                AS prcs_id,
         exp_for_cdc_check.o_insert_update            AS o_insert_update,
         exp_for_cdc_check.startdate                  AS startdate,
         exp_for_cdc_check.enddate                    AS enddate,
         exp_for_cdc_check.edw_strt_dttm              AS edw_strt_dttm,
         exp_for_cdc_check.in_src_cd                  AS in_src_cd,
         exp_for_cdc_check.in_agmt_opn_dttm           AS in_agmt_opn_dttm,
         exp_for_cdc_check.updatetime                 AS updatetime,
         exp_for_cdc_check.retired                    AS retired,
         exp_for_cdc_check.lkp_edw_end_dttm           AS lkp_edw_end_dttm,
         exp_for_cdc_check.lkp_nk_src_key             AS lkp_nk_src_key,
         exp_for_cdc_check.agmt_src_cd                AS agmt_src_cd,
         exp_for_cdc_check.closedate                  AS closedate,
         exp_for_cdc_check.source_record_id
  FROM   exp_for_cdc_check
  WHERE  exp_for_cdc_check.o_insert_update = ''U''
  AND    exp_for_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_AGMT_NEW, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_insert.accountnumber          AS accountnumber,
                rtr_agmt_insert.accountorgtype         AS accountorgtype,
                rtr_agmt_insert.accountname            AS accountname,
                rtr_agmt_insert.agmt_type_cd           AS agmt_type_cd,
                rtr_agmt_insert.agmt_cur_sts_cd        AS agmt_cur_sts_cd,
                rtr_agmt_insert.agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
                rtr_agmt_insert.agmt_obtnd_cd          AS agmt_obtnd_cd,
                rtr_agmt_insert.agmt_sbtype_cd         AS agmt_sbtype_cd,
                rtr_agmt_insert.agmt_objtv_type_cd     AS agmt_objtv_type_cd,
                rtr_agmt_insert.mkt_risk_type_cd       AS mkt_risk_type_cd,
                rtr_agmt_insert.ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
                rtr_agmt_insert.frmlty_type_cd         AS frmlty_type_cd,
                rtr_agmt_insert.agmt_idntftn_cd        AS agmt_idntftn_cd,
                rtr_agmt_insert.trmtn_type_cd          AS trmtn_type_cd,
                rtr_agmt_insert.int_pmt_meth_cd        AS int_pmt_meth_cd,
                rtr_agmt_insert.cntrl_id               AS cntrl_id,
                rtr_agmt_insert.prcs_id                AS prcs_id,
                rtr_agmt_insert.startdate              AS startdate1,
                rtr_agmt_insert.enddate                AS enddate1,
                rtr_agmt_insert.in_src_cd              AS in_src_cd,
                rtr_agmt_insert.bilg_meth_type_cd      AS bilg_meth_type_cd1,
                rtr_agmt_insert.in_agmt_opn_dttm       AS in_agmt_opn_dttm1,
                rtr_agmt_insert.updatetime             AS updatetime,
                rtr_agmt_insert.retired                AS retired1,
                rtr_agmt_insert.agmt_src_cd            AS agmt_src_cd1,
                rtr_agmt_insert.closedate              AS closedate1,
                0                                      AS update_strategy_action,
				rtr_agmt_insert.source_record_id
         FROM   rtr_agmt_insert );
  -- Component upd_AGMT_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_update.agmt_id                AS agmt_id3,
                rtr_agmt_update.accountnumber          AS accountnumber,
                rtr_agmt_update.accountorgtype         AS accountorgtype,
                rtr_agmt_update.accountname            AS accountname,
                rtr_agmt_update.agmt_type_cd           AS agmt_type_cd,
                rtr_agmt_update.agmt_cur_sts_cd        AS agmt_cur_sts_cd,
                rtr_agmt_update.agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
                rtr_agmt_update.agmt_obtnd_cd          AS agmt_obtnd_cd,
                rtr_agmt_update.agmt_sbtype_cd         AS agmt_sbtype_cd,
                rtr_agmt_update.agmt_objtv_type_cd     AS agmt_objtv_type_cd,
                rtr_agmt_update.mkt_risk_type_cd       AS mkt_risk_type_cd,
                rtr_agmt_update.ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
                rtr_agmt_update.frmlty_type_cd         AS frmlty_type_cd,
                rtr_agmt_update.agmt_idntftn_cd        AS agmt_idntftn_cd,
                rtr_agmt_update.trmtn_type_cd          AS trmtn_type_cd,
                rtr_agmt_update.int_pmt_meth_cd        AS int_pmt_meth_cd,
                rtr_agmt_update.cntrl_id               AS cntrl_id,
                rtr_agmt_update.prcs_id                AS prcs_id,
                rtr_agmt_update.startdate              AS startdate1,
                rtr_agmt_update.enddate                AS enddate1,
                rtr_agmt_update.in_src_cd              AS in_src_cd,
                rtr_agmt_update.bilg_meth_type_cd      AS bilg_meth_type_cd3,
                rtr_agmt_update.in_agmt_opn_dttm       AS in_agmt_opn_dttm3,
                rtr_agmt_update.updatetime             AS updatetime,
                rtr_agmt_update.retired                AS retired3,
                NULL                                   AS lkp_edw_end_dttm,
                rtr_agmt_update.agmt_src_cd            AS agmt_src_cd3,
                rtr_agmt_update.closedate              AS closedate3,
                0                                      AS update_strategy_action,
				rtr_agmt_update.source_record_id
         FROM   rtr_agmt_update );
  -- Component upd_AGMT_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_update.agmt_id          AS agmt_id,
                rtr_agmt_update.startdate        AS startdate3,
                rtr_agmt_update.enddate          AS enddate3,
                rtr_agmt_update.edw_strt_dttm    AS edw_strt_dttm3,
                rtr_agmt_update.accountname      AS accountname3,
                rtr_agmt_update.lkp_edw_end_dttm AS lkp_edw_end_dttm3,
                rtr_agmt_update.lkp_nk_src_key   AS lkp_nk_src_key3,
                rtr_agmt_update.updatetime       AS updatetime3,
                1                                AS update_strategy_action,
				rtr_agmt_update.source_record_id
         FROM   rtr_agmt_update );
  -- Component FILTRANS, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE filtrans AS
  (
         SELECT upd_agmt_ins.agmt_id3               AS agmt_id3,
                upd_agmt_ins.accountnumber          AS accountnumber,
                upd_agmt_ins.accountname            AS accountname,
                upd_agmt_ins.in_agmt_opn_dttm3      AS in_agmt_opn_dttm3,
                NULL                                AS agmt_cls_dttm,
                upd_agmt_ins.updatetime             AS updatetime,
                upd_agmt_ins.agmt_type_cd           AS agmt_type_cd,
                NULL                                AS agmt_legly_bindg_ind,
                upd_agmt_ins.in_src_cd              AS in_src_cd,
                upd_agmt_ins.agmt_cur_sts_cd        AS agmt_cur_sts_cd,
                upd_agmt_ins.agmt_cur_sts_rsn_cd    AS agmt_cur_sts_rsn_cd,
                upd_agmt_ins.agmt_obtnd_cd          AS agmt_obtnd_cd,
                upd_agmt_ins.agmt_sbtype_cd         AS agmt_sbtype_cd,
                NULL                                AS alt_agmt_name,
                NULL                                AS asset_liabty_cd,
                NULL                                AS bal_shet_cd,
                NULL                                AS stmt_cycl_cd,
                NULL                                AS stmt_ml_type_cd,
                NULL                                AS prposl_id,
                upd_agmt_ins.agmt_objtv_type_cd     AS agmt_objtv_type_cd,
                NULL                                AS fincl_agmt_sbtype_cd,
                upd_agmt_ins.mkt_risk_type_cd       AS mkt_risk_type_cd,
                NULL                                AS orignl_maturty_dt,
                NULL                                AS risk_expsr_mtgnt_sbtype_cd,
                NULL                                AS bnk_trd_bk_cd,
                NULL                                AS prcg_meth_sbtype_cd,
                NULL                                AS fincl_agmt_type_cd,
                NULL                                AS dy_cnt_bss_cd,
                NULL                                AS frst_prem_due_dt,
                upd_agmt_ins.accountorgtype         AS accountorgtype,
                upd_agmt_ins.ntwk_srvc_agmt_type_cd AS ntwk_srvc_agmt_type_cd,
                upd_agmt_ins.frmlty_type_cd         AS frmlty_type_cd,
                upd_agmt_ins.agmt_idntftn_cd        AS agmt_idntftn_cd,
                upd_agmt_ins.trmtn_type_cd          AS trmtn_type_cd,
                upd_agmt_ins.int_pmt_meth_cd        AS int_pmt_meth_cd,
                upd_agmt_ins.cntrl_id               AS cntrl_id,
                upd_agmt_ins.prcs_id                AS prcs_id,
                upd_agmt_ins.startdate1             AS startdate1,
                upd_agmt_ins.enddate1               AS enddate1,
                upd_agmt_ins.in_src_cd              AS in_src_cd1,
                upd_agmt_ins.bilg_meth_type_cd3     AS bilg_meth_type_cd3,
                upd_agmt_ins.in_agmt_opn_dttm3      AS in_agmt_opn_dttm31,
                upd_agmt_ins.updatetime             AS updatetime1,
                upd_agmt_ins.retired3               AS retired3,
                upd_agmt_ins.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
                upd_agmt_ins.agmt_src_cd3           AS agmt_src_cd3,
                upd_agmt_ins.closedate3             AS closedate3,
                upd_agmt_ins.source_record_id
         FROM   upd_agmt_ins
         WHERE  upd_agmt_ins.retired3 = 0 );
  -- Component exp_pass_to_target_new, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_new AS
  (
            SELECT    lkp_1.agmt_id
                      /* replaced lookup LKP_XREF_AGMNT */
                                                                                             AS agmt_id,
                      upd_agmt_new.accountnumber                                             AS host_agmt_num,
                      upd_agmt_new.accountname                                               AS agmt_name,
                      upd_agmt_new.in_agmt_opn_dttm1                                         AS agmt_opn_dttm,
                      NULL                                                                   AS agmt_cls_dttm,
                      upd_agmt_new.updatetime                                                AS updatetime,
                      to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS agmt_signd_dt,
                      upd_agmt_new.agmt_type_cd                                              AS agmt_type_cd,
                      NULL                                                                   AS agmt_legly_bindg_ind,
                      upd_agmt_new.agmt_cur_sts_cd                                           AS agmt_cur_sts_cd,
                      upd_agmt_new.agmt_cur_sts_rsn_cd                                       AS agmt_cur_sts_rsn_cd,
                      upd_agmt_new.agmt_obtnd_cd                                             AS agmt_obtnd_cd,
                      upd_agmt_new.agmt_sbtype_cd                                            AS agmt_sbtype_cd,
                      to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS agmt_prcsg_dt,
                      NULL                                                                   AS alt_agmt_name,
                      NULL                                                                   AS asset_liabty_cd,
                      NULL                                                                   AS bal_shet_cd,
                      NULL                                                                   AS stmt_cycl_cd,
                      NULL                                                                   AS stmt_ml_type_cd,
                      NULL                                                                   AS prposl_id,
                      upd_agmt_new.agmt_objtv_type_cd                                        AS agmt_objtv_type_cd,
                      NULL                                                                   AS fincl_agmt_sbtype_cd,
                      upd_agmt_new.ntwk_srvc_agmt_type_cd                                    AS mkt_risk_type_cd,
                      NULL                                                                   AS orignl_maturty_dt,
                      NULL                                                                   AS risk_expsr_mtgnt_sbtype_cd,
                      NULL                                                                   AS bnk_trd_bk_cd,
                      NULL                                                                   AS prcg_meth_sbtype_cd,
                      NULL                                                                   AS fincl_agmt_type_cd,
                      NULL                                                                   AS dy_cnt_bss_cd,
                      NULL                                                                   AS frst_prem_due_dt,
                      upd_agmt_new.accountorgtype                                            AS insrnc_agmt_sbtype_cd,
                      NULL                                                                   AS insrnc_agmt_type_cd,
                      upd_agmt_new.ntwk_srvc_agmt_type_cd                                    AS ntwk_srvc_agmt_type_cd,
                      upd_agmt_new.frmlty_type_cd                                            AS frmlty_type_cd,
                      NULL                                                                   AS cntrct_term_num,
                      NULL                                                                   AS rate_rprcg_cycl_mth_num,
                      NULL                                                                   AS cmpnd_int_cycl_mth_num,
                      NULL                                                                   AS mdterm_int_pmt_cycl_mth_num,
                      NULL                                                                   AS prev_mdterm_int_pmt_dt,
                      NULL                                                                   AS nxt_mdterm_int_pmt_dt,
                      NULL                                                                   AS prev_int_rate_rvsd_dt,
                      NULL                                                                   AS nxt_int_rate_rvsd_dt,
                      NULL                                                                   AS prev_ref_dt_int_rate,
                      NULL                                                                   AS nxt_ref_dt_for_int_rate,
                      NULL                                                                   AS mdterm_cncltn_dt,
                      NULL                                                                   AS stk_flow_clas_in_mth_ind,
                      NULL                                                                   AS stk_flow_clas_in_term_ind,
                      upd_agmt_new.agmt_idntftn_cd                                           AS agmt_idntftn_cd,
                      upd_agmt_new.trmtn_type_cd                                             AS trmtn_type_cd,
                      upd_agmt_new.int_pmt_meth_cd                                           AS int_pmt_meth_cd,
                      NULL                                                                   AS lbr_agmt_desc,
                      NULL                                                                   AS guartd_imprsns_cnt,
                      NULL                                                                   AS cost_per_imprsn_amt,
                      NULL                                                                   AS guartd_clkthru_cnt,
                      NULL                                                                   AS cost_per_clkthru_amt,
                      NULL                                                                   AS busn_prty_id,
                      upd_agmt_new.prcs_id                                                   AS prcs_id,
                      upd_agmt_new.startdate1                                                AS startdate1,
                      to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS businessdate,
                      to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS businessenddate,
                      upd_agmt_new.in_src_cd                                                 AS in_src_cd,
                      upd_agmt_new.bilg_meth_type_cd1                                        AS bilg_meth_type_cd1,
                      CASE
                                WHEN upd_agmt_new.retired1 = 0 THEN upd_agmt_new.enddate1
                                ELSE current_timestamp
                      END                                     AS edw_end_dttm,
                      to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) AS cntnus_srvc_dttm,
                      upd_agmt_new.agmt_src_cd1               AS agmt_src_cd1,
                      CASE
                                WHEN upd_agmt_new.retired1 <> 0 THEN upd_agmt_new.updatetime
                                ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                      END                     AS trans_end_dttm,
                      ''N''                     AS lgcy_plcy_ind,
                      upd_agmt_new.closedate1 AS closedate1,
                      upd_agmt_new.source_record_id,
                      row_number() over (PARTITION BY upd_agmt_new.source_record_id ORDER BY upd_agmt_new.source_record_id) AS rnk
            FROM      upd_agmt_new
            left join lkp_xref_agmnt lkp_1
            ON        lkp_1.nk_src_key = ltrim ( rtrim ( upd_agmt_new.accountnumber ) )
            AND       lkp_1.term_num = NULL
            AND       lkp_1.agmt_type_cd = ltrim ( rtrim ( upd_agmt_new.agmt_type_cd ) ) 
			qualify row_number() over (PARTITION BY upd_agmt_new.source_record_id ORDER BY upd_agmt_new.source_record_id) 
			= 1 );
  -- Component AGMT_NEW, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_name,
                          agmt_opn_dttm,
                          agmt_cls_dttm,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_legly_bindg_ind,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_cur_sts_rsn_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          alt_agmt_name,
                          asset_liabty_cd,
                          bal_shet_cd,
                          stmt_cycl_cd,
                          stmt_ml_type_cd,
                          prposl_id,
                          agmt_objtv_type_cd,
                          fincl_agmt_sbtype_cd,
                          mkt_risk_type_cd,
                          orignl_maturty_dt,
                          risk_expsr_mtgnt_sbtype_cd,
                          bnk_trd_bk_cd,
                          prcg_meth_sbtype_cd,
                          fincl_agmt_type_cd,
                          dy_cnt_bss_cd,
                          frst_prem_due_dt,
                          insrnc_agmt_sbtype_cd,
                          insrnc_busn_type_cd,
                          insrnc_agmt_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          cntrct_term_num,
                          rate_rprcg_cycl_mth_num,
                          cmpnd_int_cycl_mth_num,
                          mdterm_int_pmt_cycl_mth_num,
                          prev_mdterm_int_pmt_dt,
                          nxt_mdterm_int_pmt_dt,
                          prev_int_rate_rvsd_dt,
                          nxt_int_rate_rvsd_dt,
                          prev_ref_dt_int_rate,
                          nxt_ref_dt_for_int_rate,
                          mdterm_cncltn_dt,
                          stk_flow_clas_in_mth_ind,
                          stk_flow_clas_in_term_ind,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          lbr_agmt_desc,
                          guartd_imprsns_cnt,
                          cost_per_imprsn_amt,
                          guartd_clkthru_cnt,
                          cost_per_clkthru_amt,
                          busn_prty_id,
                          bilg_meth_type_cd,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          nk_src_key,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          lgcy_plcy_ind,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_new.agmt_id                     AS agmt_id,
         exp_pass_to_target_new.host_agmt_num               AS host_agmt_num,
         exp_pass_to_target_new.agmt_name                   AS agmt_name,
         exp_pass_to_target_new.agmt_opn_dttm               AS agmt_opn_dttm,
         exp_pass_to_target_new.agmt_cls_dttm               AS agmt_cls_dttm,
         exp_pass_to_target_new.closedate1                  AS agmt_plnd_expn_dttm,
         exp_pass_to_target_new.agmt_signd_dt               AS agmt_signd_dttm,
         exp_pass_to_target_new.agmt_type_cd                AS agmt_type_cd,
         exp_pass_to_target_new.agmt_legly_bindg_ind        AS agmt_legly_bindg_ind,
         exp_pass_to_target_new.agmt_src_cd1                AS agmt_src_cd,
         exp_pass_to_target_new.agmt_cur_sts_cd             AS agmt_cur_sts_cd,
         exp_pass_to_target_new.agmt_cur_sts_rsn_cd         AS agmt_cur_sts_rsn_cd,
         exp_pass_to_target_new.agmt_obtnd_cd               AS agmt_obtnd_cd,
         exp_pass_to_target_new.agmt_sbtype_cd              AS agmt_sbtype_cd,
         exp_pass_to_target_new.agmt_prcsg_dt               AS agmt_prcsg_dttm,
         exp_pass_to_target_new.alt_agmt_name               AS alt_agmt_name,
         exp_pass_to_target_new.asset_liabty_cd             AS asset_liabty_cd,
         exp_pass_to_target_new.bal_shet_cd                 AS bal_shet_cd,
         exp_pass_to_target_new.stmt_cycl_cd                AS stmt_cycl_cd,
         exp_pass_to_target_new.stmt_ml_type_cd             AS stmt_ml_type_cd,
         exp_pass_to_target_new.prposl_id                   AS prposl_id,
         exp_pass_to_target_new.agmt_objtv_type_cd          AS agmt_objtv_type_cd,
         exp_pass_to_target_new.fincl_agmt_sbtype_cd        AS fincl_agmt_sbtype_cd,
         exp_pass_to_target_new.mkt_risk_type_cd            AS mkt_risk_type_cd,
         exp_pass_to_target_new.orignl_maturty_dt           AS orignl_maturty_dt,
         exp_pass_to_target_new.risk_expsr_mtgnt_sbtype_cd  AS risk_expsr_mtgnt_sbtype_cd,
         exp_pass_to_target_new.bnk_trd_bk_cd               AS bnk_trd_bk_cd,
         exp_pass_to_target_new.prcg_meth_sbtype_cd         AS prcg_meth_sbtype_cd,
         exp_pass_to_target_new.fincl_agmt_type_cd          AS fincl_agmt_type_cd,
         exp_pass_to_target_new.dy_cnt_bss_cd               AS dy_cnt_bss_cd,
         exp_pass_to_target_new.frst_prem_due_dt            AS frst_prem_due_dt,
         exp_pass_to_target_new.insrnc_agmt_sbtype_cd       AS insrnc_agmt_sbtype_cd,
         exp_pass_to_target_new.agmt_sbtype_cd              AS insrnc_busn_type_cd,
         exp_pass_to_target_new.insrnc_agmt_type_cd         AS insrnc_agmt_type_cd,
         exp_pass_to_target_new.ntwk_srvc_agmt_type_cd      AS ntwk_srvc_agmt_type_cd,
         exp_pass_to_target_new.frmlty_type_cd              AS frmlty_type_cd,
         exp_pass_to_target_new.cntrct_term_num             AS cntrct_term_num,
         exp_pass_to_target_new.rate_rprcg_cycl_mth_num     AS rate_rprcg_cycl_mth_num,
         exp_pass_to_target_new.cmpnd_int_cycl_mth_num      AS cmpnd_int_cycl_mth_num,
         exp_pass_to_target_new.mdterm_int_pmt_cycl_mth_num AS mdterm_int_pmt_cycl_mth_num,
         exp_pass_to_target_new.prev_mdterm_int_pmt_dt      AS prev_mdterm_int_pmt_dt,
         exp_pass_to_target_new.nxt_mdterm_int_pmt_dt       AS nxt_mdterm_int_pmt_dt,
         exp_pass_to_target_new.prev_int_rate_rvsd_dt       AS prev_int_rate_rvsd_dt,
         exp_pass_to_target_new.nxt_int_rate_rvsd_dt        AS nxt_int_rate_rvsd_dt,
         exp_pass_to_target_new.prev_ref_dt_int_rate        AS prev_ref_dt_int_rate,
         exp_pass_to_target_new.nxt_ref_dt_for_int_rate     AS nxt_ref_dt_for_int_rate,
         exp_pass_to_target_new.mdterm_cncltn_dt            AS mdterm_cncltn_dt,
         exp_pass_to_target_new.stk_flow_clas_in_mth_ind    AS stk_flow_clas_in_mth_ind,
         exp_pass_to_target_new.stk_flow_clas_in_term_ind   AS stk_flow_clas_in_term_ind,
         exp_pass_to_target_new.agmt_idntftn_cd             AS agmt_idntftn_cd,
         exp_pass_to_target_new.trmtn_type_cd               AS trmtn_type_cd,
         exp_pass_to_target_new.int_pmt_meth_cd             AS int_pmt_meth_cd,
         exp_pass_to_target_new.lbr_agmt_desc               AS lbr_agmt_desc,
         exp_pass_to_target_new.guartd_imprsns_cnt          AS guartd_imprsns_cnt,
         exp_pass_to_target_new.cost_per_imprsn_amt         AS cost_per_imprsn_amt,
         exp_pass_to_target_new.guartd_clkthru_cnt          AS guartd_clkthru_cnt,
         exp_pass_to_target_new.cost_per_clkthru_amt        AS cost_per_clkthru_amt,
         exp_pass_to_target_new.busn_prty_id                AS busn_prty_id,
         exp_pass_to_target_new.bilg_meth_type_cd1          AS bilg_meth_type_cd,
         exp_pass_to_target_new.businessdate                AS modl_eff_dttm,
         exp_pass_to_target_new.prcs_id                     AS prcs_id,
         exp_pass_to_target_new.businessenddate             AS modl_actl_end_dttm,
         exp_pass_to_target_new.cntnus_srvc_dttm            AS cntnus_srvc_dttm,
         exp_pass_to_target_new.host_agmt_num               AS nk_src_key,
         exp_pass_to_target_new.in_src_cd                   AS src_sys_cd,
         exp_pass_to_target_new.startdate1                  AS edw_strt_dttm,
         exp_pass_to_target_new.edw_end_dttm                AS edw_end_dttm,
         exp_pass_to_target_new.lgcy_plcy_ind               AS lgcy_plcy_ind,
         exp_pass_to_target_new.updatetime                  AS trans_strt_dttm,
         exp_pass_to_target_new.trans_end_dttm              AS trans_end_dttm
  FROM   exp_pass_to_target_new;
  
  -- Component FILTRANS1, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE filtrans1 AS
  (
         SELECT upd_agmt_upd.agmt_id           AS agmt_id,
                upd_agmt_upd.startdate3        AS startdate3,
                upd_agmt_upd.enddate3          AS enddate3,
                upd_agmt_upd.edw_strt_dttm3    AS edw_strt_dttm3,
                upd_agmt_upd.accountname3      AS accountname3,
                upd_agmt_upd.lkp_edw_end_dttm3 AS lkp_edw_end_dttm,
                upd_agmt_upd.lkp_nk_src_key3   AS lkp_nk_src_key3,
                upd_agmt_upd.updatetime3       AS updatetime3,
                upd_agmt_upd.source_record_id
         FROM   upd_agmt_upd
         WHERE  upd_agmt_upd.lkp_edw_end_dttm3 = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  -- Component exp_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired AS
  (
         SELECT rtr_agmt_retired.agmt_id        AS agmt_id4,
                rtr_agmt_retired.edw_strt_dttm  AS edw_strt_dttm3,
                rtr_agmt_retired.lkp_nk_src_key AS lkp_nk_src_key4,
                current_timestamp               AS edw_end_dttm,
                rtr_agmt_retired.updatetime     AS updatetime4,
                rtr_agmt_retired.source_record_id
         FROM   rtr_agmt_retired );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT filtrans.agmt_id3                                                      AS agmt_id,
                filtrans.accountnumber                                                 AS host_agmt_num,
                filtrans.accountname                                                   AS agmt_name,
                filtrans.in_agmt_opn_dttm3                                             AS agmt_opn_dttm,
                filtrans.agmt_cls_dttm                                                 AS agmt_cls_dttm,
                filtrans.updatetime                                                    AS updatetime,
                to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS agmt_signd_dt,
                filtrans.agmt_type_cd                                                  AS agmt_type_cd,
                filtrans.agmt_legly_bindg_ind                                          AS agmt_legly_bindg_ind,
                filtrans.agmt_cur_sts_cd                                               AS agmt_cur_sts_cd,
                filtrans.agmt_cur_sts_rsn_cd                                           AS agmt_cur_sts_rsn_cd,
                filtrans.agmt_obtnd_cd                                                 AS agmt_obtnd_cd,
                filtrans.agmt_sbtype_cd                                                AS agmt_sbtype_cd,
                to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS agmt_prcsg_dt,
                filtrans.alt_agmt_name                                                 AS alt_agmt_name,
                filtrans.asset_liabty_cd                                               AS asset_liabty_cd,
                filtrans.bal_shet_cd                                                   AS bal_shet_cd,
                filtrans.stmt_cycl_cd                                                  AS stmt_cycl_cd,
                filtrans.stmt_ml_type_cd                                               AS stmt_ml_type_cd,
                filtrans.prposl_id                                                     AS prposl_id,
                filtrans.agmt_objtv_type_cd                                            AS agmt_objtv_type_cd,
                filtrans.fincl_agmt_sbtype_cd                                          AS fincl_agmt_sbtype_cd,
                filtrans.mkt_risk_type_cd                                              AS mkt_risk_type_cd,
                filtrans.orignl_maturty_dt                                             AS orignl_maturty_dt,
                filtrans.risk_expsr_mtgnt_sbtype_cd                                    AS risk_expsr_mtgnt_sbtype_cd,
                filtrans.bnk_trd_bk_cd                                                 AS bnk_trd_bk_cd,
                filtrans.prcg_meth_sbtype_cd                                           AS prcg_meth_sbtype_cd,
                filtrans.fincl_agmt_type_cd                                            AS fincl_agmt_type_cd,
                filtrans.dy_cnt_bss_cd                                                 AS dy_cnt_bss_cd,
                filtrans.frst_prem_due_dt                                              AS frst_prem_due_dt,
                filtrans.accountorgtype                                                AS insrnc_agmt_sbtype_cd,
                filtrans.ntwk_srvc_agmt_type_cd                                        AS ntwk_srvc_agmt_type_cd,
                filtrans.frmlty_type_cd                                                AS frmlty_type_cd,
                filtrans.agmt_idntftn_cd                                               AS agmt_idntftn_cd,
                filtrans.trmtn_type_cd                                                 AS trmtn_type_cd,
                filtrans.int_pmt_meth_cd                                               AS int_pmt_meth_cd,
                filtrans.prcs_id                                                       AS prcs_id,
                filtrans.startdate1                                                    AS startdate1,
                to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )                                AS businessdate,
                to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS businessenddate,
                filtrans.in_src_cd1                                                    AS in_src_cd,
                filtrans.bilg_meth_type_cd3                                            AS bilg_meth_type_cd3,
                CASE
                       WHEN filtrans.retired3 != 0 THEN current_timestamp
                       ELSE filtrans.enddate1
                END                                     AS edw_end_dttm,
                to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) AS cntnus_srvc_dttm,
                filtrans.agmt_src_cd3                   AS agmt_src_cd3,
                filtrans.closedate3                     AS closedate3,
                ''N''                                     AS lgcy_plcy_ind,
                filtrans.source_record_id
         FROM   filtrans );
  -- Component exp_pass_to_target_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd AS
  (
         SELECT filtrans1.agmt_id                                  AS agmt_id,
                filtrans1.edw_strt_dttm3                           AS edw_strt_dttm3,
                dateadd ( second, -1, filtrans1.startdate3  )  AS out_edw_end_dttm,
                filtrans1.lkp_nk_src_key3                          AS lkp_nk_src_key3,
                dateadd (second, -1,  filtrans1.updatetime3  ) AS trans_end_dttm,
                ''N''                                                AS lgcy_plcy_ind,
                filtrans1.source_record_id
         FROM   filtrans1 );
  -- Component UPD_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT exp_retired.agmt_id4        AS agmt_id4,
                exp_retired.edw_strt_dttm3  AS edw_strt_dttm3,
                exp_retired.lkp_nk_src_key4 AS lkp_nk_src_key4,
                exp_retired.edw_end_dttm    AS edw_end_dttm,
                exp_retired.updatetime4     AS updatetime4,
                1                           AS update_strategy_action,
				exp_retired.source_record_id
         FROM   exp_retired );
  -- Component AGMT_Upd, Type TARGET
  merge
  INTO         db_t_prod_core.agmt
  USING        exp_pass_to_target_upd
  ON (
                            agmt.agmt_id = exp_pass_to_target_upd.agmt_id
               AND          agmt.nk_src_key = exp_pass_to_target_upd.lkp_nk_src_key3
               AND          agmt.edw_strt_dttm = exp_pass_to_target_upd.edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_pass_to_target_upd.agmt_id,
         nk_src_key = exp_pass_to_target_upd.lkp_nk_src_key3,
         edw_strt_dttm = exp_pass_to_target_upd.edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_upd.out_edw_end_dttm,
         lgcy_plcy_ind = exp_pass_to_target_upd.lgcy_plcy_ind,
         trans_end_dttm = exp_pass_to_target_upd.trans_end_dttm;
  
  -- Component AGMT_ins, Type TARGET
  INSERT INTO db_t_prod_core.agmt
              (
                          agmt_id,
                          host_agmt_num,
                          agmt_name,
                          agmt_opn_dttm,
                          agmt_cls_dttm,
                          agmt_plnd_expn_dttm,
                          agmt_signd_dttm,
                          agmt_type_cd,
                          agmt_legly_bindg_ind,
                          agmt_src_cd,
                          agmt_cur_sts_cd,
                          agmt_cur_sts_rsn_cd,
                          agmt_obtnd_cd,
                          agmt_sbtype_cd,
                          agmt_prcsg_dttm,
                          alt_agmt_name,
                          asset_liabty_cd,
                          bal_shet_cd,
                          stmt_cycl_cd,
                          stmt_ml_type_cd,
                          prposl_id,
                          agmt_objtv_type_cd,
                          fincl_agmt_sbtype_cd,
                          mkt_risk_type_cd,
                          orignl_maturty_dt,
                          risk_expsr_mtgnt_sbtype_cd,
                          bnk_trd_bk_cd,
                          prcg_meth_sbtype_cd,
                          fincl_agmt_type_cd,
                          dy_cnt_bss_cd,
                          frst_prem_due_dt,
                          insrnc_agmt_sbtype_cd,
                          insrnc_busn_type_cd,
                          ntwk_srvc_agmt_type_cd,
                          frmlty_type_cd,
                          agmt_idntftn_cd,
                          trmtn_type_cd,
                          int_pmt_meth_cd,
                          bilg_meth_type_cd,
                          modl_eff_dttm,
                          prcs_id,
                          modl_actl_end_dttm,
                          cntnus_srvc_dttm,
                          nk_src_key,
                          src_sys_cd,
                          edw_strt_dttm,
                          edw_end_dttm,
                          lgcy_plcy_ind,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_target_ins.agmt_id                    AS agmt_id,
         exp_pass_to_target_ins.host_agmt_num              AS host_agmt_num,
         exp_pass_to_target_ins.agmt_name                  AS agmt_name,
         exp_pass_to_target_ins.agmt_opn_dttm              AS agmt_opn_dttm,
         exp_pass_to_target_ins.agmt_cls_dttm              AS agmt_cls_dttm,
         exp_pass_to_target_ins.closedate3                 AS agmt_plnd_expn_dttm,
         exp_pass_to_target_ins.agmt_signd_dt              AS agmt_signd_dttm,
         exp_pass_to_target_ins.agmt_type_cd               AS agmt_type_cd,
         exp_pass_to_target_ins.agmt_legly_bindg_ind       AS agmt_legly_bindg_ind,
         exp_pass_to_target_ins.agmt_src_cd3               AS agmt_src_cd,
         exp_pass_to_target_ins.agmt_cur_sts_cd            AS agmt_cur_sts_cd,
         exp_pass_to_target_ins.agmt_cur_sts_rsn_cd        AS agmt_cur_sts_rsn_cd,
         exp_pass_to_target_ins.agmt_obtnd_cd              AS agmt_obtnd_cd,
         exp_pass_to_target_ins.agmt_sbtype_cd             AS agmt_sbtype_cd,
         exp_pass_to_target_ins.agmt_prcsg_dt              AS agmt_prcsg_dttm,
         exp_pass_to_target_ins.alt_agmt_name              AS alt_agmt_name,
         exp_pass_to_target_ins.asset_liabty_cd            AS asset_liabty_cd,
         exp_pass_to_target_ins.bal_shet_cd                AS bal_shet_cd,
         exp_pass_to_target_ins.stmt_cycl_cd               AS stmt_cycl_cd,
         exp_pass_to_target_ins.stmt_ml_type_cd            AS stmt_ml_type_cd,
         exp_pass_to_target_ins.prposl_id                  AS prposl_id,
         exp_pass_to_target_ins.agmt_objtv_type_cd         AS agmt_objtv_type_cd,
         exp_pass_to_target_ins.fincl_agmt_sbtype_cd       AS fincl_agmt_sbtype_cd,
         exp_pass_to_target_ins.mkt_risk_type_cd           AS mkt_risk_type_cd,
         exp_pass_to_target_ins.orignl_maturty_dt          AS orignl_maturty_dt,
         exp_pass_to_target_ins.risk_expsr_mtgnt_sbtype_cd AS risk_expsr_mtgnt_sbtype_cd,
         exp_pass_to_target_ins.bnk_trd_bk_cd              AS bnk_trd_bk_cd,
         exp_pass_to_target_ins.prcg_meth_sbtype_cd        AS prcg_meth_sbtype_cd,
         exp_pass_to_target_ins.fincl_agmt_type_cd         AS fincl_agmt_type_cd,
         exp_pass_to_target_ins.dy_cnt_bss_cd              AS dy_cnt_bss_cd,
         exp_pass_to_target_ins.frst_prem_due_dt           AS frst_prem_due_dt,
         exp_pass_to_target_ins.insrnc_agmt_sbtype_cd      AS insrnc_agmt_sbtype_cd,
         exp_pass_to_target_ins.agmt_sbtype_cd             AS insrnc_busn_type_cd,
         exp_pass_to_target_ins.ntwk_srvc_agmt_type_cd     AS ntwk_srvc_agmt_type_cd,
         exp_pass_to_target_ins.frmlty_type_cd             AS frmlty_type_cd,
         exp_pass_to_target_ins.agmt_idntftn_cd            AS agmt_idntftn_cd,
         exp_pass_to_target_ins.trmtn_type_cd              AS trmtn_type_cd,
         exp_pass_to_target_ins.int_pmt_meth_cd            AS int_pmt_meth_cd,
         exp_pass_to_target_ins.bilg_meth_type_cd3         AS bilg_meth_type_cd,
         exp_pass_to_target_ins.businessdate               AS modl_eff_dttm,
         exp_pass_to_target_ins.prcs_id                    AS prcs_id,
         exp_pass_to_target_ins.businessenddate            AS modl_actl_end_dttm,
         exp_pass_to_target_ins.cntnus_srvc_dttm           AS cntnus_srvc_dttm,
         exp_pass_to_target_ins.host_agmt_num              AS nk_src_key,
         exp_pass_to_target_ins.in_src_cd                  AS src_sys_cd,
         exp_pass_to_target_ins.startdate1                 AS edw_strt_dttm,
         exp_pass_to_target_ins.edw_end_dttm               AS edw_end_dttm,
         exp_pass_to_target_ins.lgcy_plcy_ind              AS lgcy_plcy_ind,
         exp_pass_to_target_ins.updatetime                 AS trans_strt_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component exp_pass_to_target_ret, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ret AS
  (
         SELECT upd_retired.agmt_id4        AS agmt_id4,
                upd_retired.edw_strt_dttm3  AS edw_strt_dttm3,
                upd_retired.lkp_nk_src_key4 AS lkp_nk_src_key4,
                upd_retired.edw_end_dttm    AS edw_end_dttm,
                upd_retired.updatetime4     AS updatetime4,
                ''N''                         AS lgcy_plcy_ind,
                upd_retired.source_record_id
         FROM   upd_retired );
  -- Component AGMT_retired, Type TARGET
  merge
  INTO         db_t_prod_core.agmt
  USING        exp_pass_to_target_ret
  ON (
                            agmt.agmt_id = exp_pass_to_target_ret.agmt_id4
               AND          agmt.nk_src_key = exp_pass_to_target_ret.lkp_nk_src_key4
               AND          agmt.edw_strt_dttm = exp_pass_to_target_ret.edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_pass_to_target_ret.agmt_id4,
         nk_src_key = exp_pass_to_target_ret.lkp_nk_src_key4,
         edw_strt_dttm = exp_pass_to_target_ret.edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_ret.edw_end_dttm,
         lgcy_plcy_ind = exp_pass_to_target_ret.lgcy_plcy_ind,
         trans_end_dttm = exp_pass_to_target_ret.updatetime4;

END;
';