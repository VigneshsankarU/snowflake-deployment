-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_WRK_EDW_EBLM_CLM_DTL1_EXTRACT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       PRCS_ID STRING;
	   v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
	   v_start_time := CURRENT_TIMESTAMP();

-- Component sq_edw_eblm_clm_dtl11, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_edw_eblm_clm_dtl11 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PLCY_VEH_KEY,
$2 as CLM_NBR,
$3 as CLM_LOSS_DT,
$4 as ACC_ST_CD,
$5 as LOI_DCC_EXPENSE_AMT,
$6 as LOI_DCC_RECOVERY_AMT,
$7 as LOI_AO_EXPENSE_AMT,
$8 as LOI_AO_RECOVERY_AMT,
$9 as LOU_DIRECT_LOSS_AMT,
$10 as LOU_DIR_LOSS_RECOVERY_AMT,
$11 as LOU_SALVAGE_RECOVERY_AMT,
$12 as LOU_SALVAGE_EXPENSE_AMT,
$13 as LOU_SUBR_RECOVERY_AMT,
$14 as LOU_SUBR_EXPENSE_AMT,
$15 as LOU_DCC_EXPENSE_AMT,
$16 as LOU_DCC_RECOVERY_AMT,
$17 as LOU_AO_EXPENSE_AMT,
$18 as LOU_AO_RECOVERY_AMT,
$19 as MED_DIRECT_LOSS_AMT,
$20 as MED_DIR_LOSS_RECOVERY_AMT,
$21 as MED_SALVAGE_RECOVERY_AMT,
$22 as MED_SALVAGE_EXPENSE_AMT,
$23 as MED_SUBR_RECOVERY_AMT,
$24 as MED_SUBR_EXPENSE_AMT,
$25 as MED_DCC_EXPENSE_AMT,
$26 as MED_DCC_RECOVERY_AMT,
$27 as MED_AO_EXPENSE_AMT,
$28 as MED_AO_RECOVERY_AMT,
$29 as UNB_DIRECT_LOSS_AMT,
$30 as UNB_DIR_LOSS_RECOVERY_AMT,
$31 as UNB_SALVAGE_RECOVERY_AMT,
$32 as UNB_SALVAGE_EXPENSE_AMT,
$33 as UNB_SUBR_RECOVERY_AMT,
$34 as UNB_SUBR_EXPENSE_AMT,
$35 as UNB_DCC_EXPENSE_AMT,
$36 as UNB_DCC_RECOVERY_AMT,
$37 as UNB_AO_EXPENSE_AMT,
$38 as UNB_AO_RECOVERY_AMT,
$39 as UNP_DIRECT_LOSS_AMT,
$40 as UNP_DIR_LOSS_RECOVERY_AMT,
$41 as UNP_SALVAGE_RECOVERY_AMT,
$42 as UNP_SALVAGE_EXPENSE_AMT,
$43 as UNP_SUBR_RECOVERY_AMT,
$44 as UNP_SUBR_EXPENSE_AMT,
$45 as UNP_DCC_EXPENSE_AMT,
$46 as UNP_DCC_RECOVERY_AMT,
$47 as UNP_AO_EXPENSE_AMT,
$48 as UNP_AO_RECOVERY_AMT,
$49 as CS_DIRECT_LOSS_AMT,
$50 as CS_DIR_LOSS_RECOVERY_AMT,
$51 as CS_SALVAGE_RECOVERY_AMT,
$52 as CS_SALVAGE_EXPENSE_AMT,
$53 as CS_SUBR_RECOVERY_AMT,
$54 as CS_SUBR_EXPENSE_AMT,
$55 as CS_DCC_EXPENSE_AMT,
$56 as CS_DCC_RECOVERY_AMT,
$57 as CS_AO_EXPENSE_AMT,
$58 as CS_AO_RECOVERY_AMT,
$59 as EXN_DIRECT_LOSS_AMT,
$60 as EXN_DIR_LOSS_RECOVERY_AMT,
$61 as EXN_SALVAGE_RECOVERY_AMT,
$62 as EXN_SALVAGE_EXPENSE_AMT,
$63 as EXN_SUBR_RECOVERY_AMT,
$64 as EXN_SUBR_EXPENSE_AMT,
$65 as EXN_DCC_EXPENSE_AMT,
$66 as EXN_DCC_RECOVERY_AMT,
$67 as EXN_AO_EXPENSE_AMT,
$68 as EXN_AO_RECOVERY_AMT,
$69 as SG_DIRECT_LOSS_AMT,
$70 as SG_DIR_LOSS_RECOVERY_AMT,
$71 as SG_SALVAGE_RECOVERY_AMT,
$72 as SG_SALVAGE_EXPENSE_AMT,
$73 as SG_SUBR_RECOVERY_AMT,
$74 as SG_SUBR_EXPENSE_AMT,
$75 as SG_DCC_EXPENSE_AMT,
$76 as SG_DCC_RECOVERY_AMT,
$77 as SG_AO_EXPENSE_AMT,
$78 as SG_AO_RECOVERY_AMT,
$79 as CUS_DIRECT_LOSS_AMT,
$80 as CUS_DIR_LOSS_RECOVERY_AMT,
$81 as CUS_SALVAGE_RECOVERY_AMT,
$82 as CUS_SALVAGE_EXPENSE_AMT,
$83 as CUS_SUBR_RECOVERY_AMT,
$84 as CUS_SUBR_EXPENSE_AMT,
$85 as CUS_DCC_EXPENSE_AMT,
$86 as CUS_DCC_RECOVERY_AMT,
$87 as CUS_AO_EXPENSE_AMT,
$88 as CUS_AO_RECOVERY_AMT,
$89 as GAP_DIRECT_LOSS_AMT,
$90 as GAP_DIR_LOSS_RECOVERY_AMT,
$91 as GAP_SALVAGE_RECOVERY_AMT,
$92 as GAP_SALVAGE_EXPENSE_AMT,
$93 as GAP_SUBR_RECOVERY_AMT,
$94 as GAP_SUBR_EXPENSE_AMT,
$95 as GAP_DCC_EXPENSE_AMT,
$96 as GAP_DCC_RECOVERY_AMT,
$97 as GAP_AO_EXPENSE_AMT,
$98 as GAP_AO_RECOVERY_AMT,
$99 as SL_DIRECT_LOSS_AMT,
$100 as SL_DIR_LOSS_RECOVERY_AMT,
$101 as SL_SALVAGE_RECOVERY_AMT,
$102 as SL_SALVAGE_EXPENSE_AMT,
$103 as SL_SUBR_RECOVERY_AMT,
$104 as SL_SUBR_EXPENSE_AMT,
$105 as SL_DCC_EXPENSE_AMT,
$106 as SL_DCC_RECOVERY_AMT,
$107 as SL_AO_EXPENSE_AMT,
$108 as SL_AO_RECOVERY_AMT,
$109 as UND_DIRECT_LOSS_AMT,
$110 as UND_DIR_LOSS_RECOVERY_AMT,
$111 as UND_SALVAGE_RECOVERY_AMT,
$112 as UND_SALVAGE_EXPENSE_AMT,
$113 as UND_SUBR_RECOVERY_AMT,
$114 as UND_SUBR_EXPENSE_AMT,
$115 as UND_DCC_EXPENSE_AMT,
$116 as UND_DCC_RECOVERY_AMT,
$117 as UND_AO_EXPENSE_AMT,
$118 as UND_AO_RECOVERY_AMT,
$119 as UNSL_DIRECT_LOSS_AMT,
$120 as UNSL_DIR_LOSS_RECOVERY_AMT,
$121 as UNSL_SALVAGE_RECOVERY_AMT,
$122 as UNSL_SALVAGE_EXPENSE_AMT,
$123 as UNSL_SUBR_RECOVERY_AMT,
$124 as UNSL_SUBR_EXPENSE_AMT,
$125 as UNSL_DCC_EXPENSE_AMT,
$126 as UNSL_DCC_RECOVERY_AMT,
$127 as UNSL_AO_EXPENSE_AMT,
$128 as UNSL_AO_RECOVERY_AMT,
$129 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT

/* CAST(AG.agmt_id||agmt_asset.prty_asset_id||asset_cntrct_role_sbtype_cd||to_char(agmt_asset_strt_dttm,''MM/DD/YYYY'') AS VARCHAR(200)) as -----plcy_veh_key, */
/* CAST(TRIM(AG.agmt_id)||TRIM(agmt_ast.prty_asset_id)||TRIM(agmt_ast.asset_cntrct_role_sbtype_cd)||TRIM(to_char-----(agmt_ast.agmt_asset_strt_dttm,''MM/DD/YYYY'')) AS VARCHAR(200)) as plcy_veh_key, */
CAST(TRIM(AG.agmt_id)||TRIM(MOTR_VEH.MOTR_VEH_SER_NUM) AS VARCHAR(200)) as plcy_veh_key,

clm.clm_num,

clm_dt.clm_dttm AS clm_loss_dt,

terr.geogrcl_area_shrt_name AS acc_st_cd,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS loi_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_dcc_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS loi_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_ao_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LOSS'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS lou_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'')  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS lou_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS lou_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS lou_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO''  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS lou_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''LOU'' THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END - CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS lou_subr_expense_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS lou_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS lou_dcc_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS lou_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''LOU''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS lou_ao_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LOSS'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS med_direct_loss_amt,

SUM(0 -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'')  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_salvage_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_salvage_expense_amt,

SUM(0-  CASE   WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''   THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_subr_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_subr_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_dcc_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_ao_expense_amt,

SUM( 0-  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''MED''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS med_ao_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_dir_loss_recovery_amt,

SUM(0-CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_salvage_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_subr_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_subr_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS unb_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_dcc_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMUIM''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unb_ao_recovery_amt,

SUM(0-  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_direct_loss_amt,

SUM( 0-  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_salvage_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_subr_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_subr_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_dcc_recovery_amt,

SUM(0 -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UMPD''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unp_ao_recovery_amt,

NULL AS cs_direct_loss_amt,NULL AS cs_dir_loss_recovery_amt,NULL AS cs_salvage_recovery_amt,NULL AS cs_salvage_expense_amt,NULL AS cs_subr_recovery_amt,NULL AS cs_subr_expense_amt,NULL AS cs_dcc_expense_amt,NULL AS cs_dcc_recovery_amt,NULL AS cs_ao_expense_amt,NULL AS cs_ao_recovery_amt,NULL AS exn_direct_loss_amt,NULL AS exn_dir_loss_recovery_amt,NULL AS exn_salvage_recovery_amt,NULL AS exn_salvage_expense_amt,NULL AS exn_subr_recovery_amt,NULL AS exn_subr_expense_amt,NULL AS exn_dcc_expense_amt,NULL AS exn_dcc_recovery_amt,NULL AS exn_ao_expense_amt,NULL AS exn_ao_recovery_amt,NULL AS sg_direct_loss_amt,NULL AS sg_dir_loss_recovery_amt,NULL AS sg_salvage_recovery_amt,NULL AS sg_salvage_expense_amt,NULL AS sg_subr_recovery_amt,NULL AS sg_subr_expense_amt,NULL AS sg_dcc_expense_amt,NULL AS sg_dcc_recovery_amt,NULL AS sg_ao_expense_amt,NULL AS sg_ao_recovery_amt,NULL AS cus_direct_loss_amt,NULL AS cmp_direct_loss_amt,NULL AS cus_dir_loss_recovery_amt,NULL AS cus_salvage_recovery_amt,NULL AS cus_salvage_expense_amt,NULL AS cus_subr_recovery_amt,NULL AS cus_subr_expense_amt,NULL AS cus_dcc_expense_amt,NULL AS cus_dcc_recovery_amt,NULL AS cus_ao_expense_amt,NULL AS cus_ao_recovery_amt,NULL AS gap_direct_loss_amt,NULL AS gap_dir_loss_recovery_amt,NULL AS gap_salvage_recovery_amt,NULL AS gap_salvage_expense_amt,NULL AS gap_subr_recovery_amt,NULL AS gap_subr_expense_amt,NULL AS gap_dcc_expense_amt,NULL AS gap_dcc_recovery_amt,NULL AS gap_ao_expense_amt,NULL AS gap_ao_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_direct_loss_amt,

SUM(0-  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_salvage_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_subr_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_subr_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_dcc_recovery_amt,

SUM(0-  CASE  WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''SL'' THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS sl_ao_recovery_amt,

NULL AS und_direct_loss_amt,NULL AS und_dir_loss_recovery_amt,NULL AS und_salvage_recovery_amt,NULL AS und_salvage_expense_amt,NULL AS und_subr_recovery_amt,NULL AS und_subr_expense_amt,NULL AS und_dcc_expense_amt,NULL AS und_dcc_recovery_amt,NULL AS und_ao_expense_amt,NULL AS und_ao_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_dir_loss_recovery_amt,

SUM(0-  CASE  WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_salvage_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_subr_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_subr_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_dcc_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''UNS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS unsl_ao_recovery_amt

FROM    

    DB_T_PROD_CORE.CLM

    INNER JOIN DB_T_PROD_CORE.clm_expsr ON          clm.clm_id=clm_expsr.clm_id

    LEFT JOIN  DB_T_PROD_CORE.clm_dt ON  clm.clm_id=clm_dt.clm_id AND           clm_dt_type_cd=''LOSS'' AND clm_dt.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    LEFT JOIN  DB_T_PROD_CORE.clm_loctr ON              clm.clm_id=clm_loctr.clm_id AND       clm_loctr_role_cd=''LOSSSTADRS'' AND clm_loctr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    INNER JOIN DB_T_PROD_CORE.street_addr ON clm_loctr.loc_id = street_addr.street_addr_id AND street_addr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    INNER JOIN DB_T_PROD_CORE.terr ON      terr.terr_id=street_addr.terr_id AND terr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  

    LEFT JOIN  DB_T_PROD_CORE.clm_expsr_trans ON  clm_expsr_trans.clm_expsr_id=clm_expsr.clm_expsr_id AND clm_expsr_trans.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    LEFT JOIN  DB_T_PROD_CORE.clm_expsr_trans_lnitm ON        clm_expsr_trans_lnitm.clm_expsr_trans_id=clm_expsr_trans.clm_expsr_trans_id AND clm_expsr_trans_lnitm.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    LEFT JOIN  DB_T_PROD_CORE.feat ON       clm_expsr.cvge_feat_id=feat.feat_id AND feat.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    LEFT JOIN  DB_T_PROD_CORE.clm_insrbl_int ON      clm_insrbl_int.clm_id=clm.clm_id AND clm_insrbl_int.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  and clm_insrbl_int.clm_insrbl_int_type_cd=''VEH'' 

    LEFT JOIN  DB_T_PROD_CORE.insrbl_int ON              clm_insrbl_int.insrbl_int_id=insrbl_int.insrbl_int_id AND insrbl_int.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

   LEFT JOIN DB_T_PROD_CORE.prty_asset ON              prty_asset.prty_asset_id=insrbl_int.prty_asset_id AND     prty_asset_sbtype_cd=''MVEH'' AND                prty_asset_clasfcn_cd=''MV'' AND prty_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

/*   LEFT JOIN  DB_T_PROD_CORE.motr_veh ON              prty_asset.prty_asset_id=motr_veh.prty_asset_id AND motr_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  */
       JOIN

(

Select AC.AGMT_ID,AC.CLM_ID from DB_T_PROD_CORE.AGMT_CLM AC

JOIN

DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=AC.AGMT_ID AND AG.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

AND       AG.modl_crtn_dttm = (

                                                                                SELECT                MAX(aa.modl_crtn_dttm)  

                                                                                FROM    DB_T_PROD_CORE.AGMT aa 

                                                                                WHERE aa.AGMT_TYPE_CD = ''PPV''

                                                                                AND aa.AGMT_CUR_STS_CD=''BOUND''

AND       aa.host_agmt_num=AG.host_agmt_num)             /*  To capture only the most recent model for a DB_T_CORE_DM_PROD.policy  */
WHERE

AC.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

)  AG ON CLM.CLM_ID=AG.CLM_ID



LEFT JOIN  

(SELECT AGMT_ID,PRTY_ASSET_ID,asset_cntrct_role_sbtype_cd,MAX(AGMT_ASSET_STRT_DTTM) AS AGMT_ASSET_STRT_DTTM FROM DB_T_PROD_CORE.AGMT_ASSET WHERE edw_end_dttm=''9999-12-31 23:59:59.999999'' GROUP BY 1,2,3)  

agmt_ast ON          AG.AGMT_ID=agmt_ast.AGMT_ID /* AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
JOIN  DB_T_PROD_CORE.motr_veh ON              prty_asset.prty_asset_id=motr_veh.prty_asset_id AND motr_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

JOIN DB_T_PROD_CORE.AGMT_PROD 											

	ON	AG.AGMT_ID=AGMT_PROD.AGMT_ID AND AGMT_PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''									

JOIN DB_T_PROD_CORE.PROD 											

	ON	AGMT_PROD.PROD_ID=PROD.PROD_ID AND PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''	

	and PROD.PROD_NAME IN (''PPV'', ''AUTO'', ''COMMERCIAL'', ''PERSONAL AUTO'') 



  

GROUP BY 

                plcy_veh_key,

    clm_num,

    clm_loss_dt,

    acc_st_cd
) SRC
)
);


-- Component sq_edw_eblm_clm_dtl1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_edw_eblm_clm_dtl1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PLCY_VEH_KEY,
$2 as CLM_NBR,
$3 as CLM_LOSS_DT,
$4 as ACC_ST_CD,
$5 as BI_DIRECT_LOSS_AMT,
$6 as BI_DIR_LOSS_RECOVERY_AMT,
$7 as BI_SALVAGE_RECOVERY_AMT,
$8 as BI_SALVAGE_EXPENSE_AMT,
$9 as BI_SUBR_RECOVERY_AMT,
$10 as BI_SUBR_EXPENSE_AMT,
$11 as BI_DCC_EXPENSE_AMT,
$12 as BI_DCC_RECOVERY_AMT,
$13 as BI_AO_EXPENSE_AMT,
$14 as BI_AO_RECOVERY_AMT,
$15 as PD_DIRECT_LOSS_AMT,
$16 as PD_DIR_LOSS_RECOVERY_AMT,
$17 as PD_SALVAGE_RECOVERY_AMT,
$18 as PD_SALVAGE_EXPENSE_AMT,
$19 as PD_SUBR_RECOVERY_AMT,
$20 as PD_SUBR_EXPENSE_AMT,
$21 as PD_DCC_EXPENSE_AMT,
$22 as PD_DCC_RECOVERY_AMT,
$23 as PD_AO_EXPENSE_AMT,
$24 as PD_AO_RECOVERY_AMT,
$25 as CMP_DIR_LOSS_RECOVERY_AMT,
$26 as CMP_SALVAGE_RECOVERY_AMT,
$27 as CMP_SALVAGE_EXPENSE_AMT,
$28 as CMP_SUBR_RECOVERY_AMT,
$29 as CMP_SUBR_EXPENSE_AMT,
$30 as CMP_DCC_EXPENSE_AMT,
$31 as CMP_DCC_RECOVERY_AMT,
$32 as CMP_AO_EXPENSE_AMT,
$33 as CMP_AO_RECOVERY_AMT,
$34 as CMP_DIRECT_LOSS_AMT,
$35 as COL_DIRECT_LOSS_AMT,
$36 as COL_DIR_LOSS_RECOVERY_AMT,
$37 as COL_SALVAGE_RECOVERY_AMT,
$38 as COL_SALVAGE_EXPENSE_AMT,
$39 as COL_SUBR_RECOVERY_AMT,
$40 as COL_SUBR_EXPENSE_AMT,
$41 as COL_DCC_EXPENSE_AMT,
$42 as COL_DCC_RECOVERY_AMT,
$43 as COL_AO_EXPENSE_AMT,
$44 as COL_AO_RECOVERY_AMT,
$45 as ERS_DIRECT_LOSS_AMT,
$46 as ERS_DIR_LOSS_RECOVERY_AMT,
$47 as ERS_SALVAGE_RECOVERY_AMT,
$48 as ERS_SALVAGE_EXPENSE_AMT,
$49 as ERS_SUBR_RECOVERY_AMT,
$50 as ERS_SUBR_EXPENSE_AMT,
$51 as ERS_DCC_EXPENSE_AMT,
$52 as ERS_DCC_RECOVERY_AMT,
$53 as ERS_AO_EXPENSE_AMT,
$54 as ERS_AO_RECOVERY_AMT,
$55 as LOI_DIRECT_LOSS_AMT,
$56 as LOI_DIR_LOSS_RECOVERY_AMT,
$57 as LOI_SALVAGE_RECOVERY_AMT,
$58 as LOI_SALVAGE_EXPENSE_AMT,
$59 as LOI_SUBR_RECOVERY_AMT,
$60 as LOI_SUBR_EXPENSE_AMT,
$61 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT

/* CAST(AG.agmt_id||agmt_asset.prty_asset_id||asset_cntrct_role_sbtype_cd||to_char(agmt_asset_strt_dttm,''MM/DD/YYYY'') AS VARCHAR(200)) as plcy_veh_key, */
/* CAST(TRIM(AG.agmt_id)||TRIM(agmt_ast.prty_asset_id)||TRIM(agmt_ast.asset_cntrct_role_sbtype_cd)||TRIM(to_char(agmt_ast.agmt_asset_strt_dttm,''MM/DD/YYYY'')) AS VARCHAR(200)) as plcy_veh_key, */
CAST(TRIM(AG.agmt_id)||TRIM(MOTR_VEH.MOTR_VEH_SER_NUM) AS VARCHAR(200)) as plcy_veh_key,

clm.clm_num,

clm_dt.clm_dttm AS clm_loss_dt,

terr.geogrcl_area_shrt_name AS acc_st_cd,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.rcvry_ctgy_type_cd IS NULL AND clm_expsr_trans.expsr_cost_type_cd=''PDL'' AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS bi_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'') AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'') AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_subr_expense_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS bi_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_dcc_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS bi_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''BI''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS bi_ao_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LOSS'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS pd_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'') AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_subr_expense_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS pd_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_dcc_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS pd_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''PD''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS pd_ao_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'') AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS cmp_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS cmp_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS cmp_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS cmp_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''COMP''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS cmp_subr_expense_amt,

SUM( CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''COMP''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END - 0) AS cmp_dcc_expense_amt,

SUM(0 - CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''COMP''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS cmp_dcc_recovery_amt,

SUM(CASE  WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''COMP''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- 0) AS cmp_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''COMP''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS cmp_ao_recovery_amt,

/*   start added code for Defect 15621 */
SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

AND clm_expsr_trans.rcvry_ctgy_type_cd IS NULL  

AND clm_expsr_trans.expsr_cost_type_cd =''PDL''

AND clm_expsr_trans.expsr_cost_ctgy_type_cd =''LOSS'' 

AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd =''LOSS''

AND feat.insrnc_cvge_type_cd = ''COMP'' 

THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS cmp_direct_loss_amt,

/*   end added code for Defect 15621 */
SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LOSS'' AND feat.insrnc_cvge_type_cd = ''COLL''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS col_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'')  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''COLL''THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END ) AS col_direct_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS col_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS col_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO''  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS col_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END) AS col_subr_expense_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS col_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS col_dcc_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  -  0 ) AS col_ao_expense_amt,

SUM(0- CASE  WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''COLL''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS col_ao_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LOSS'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  -  0 ) AS ers_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'')  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_direct_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO''  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_subr_expense_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  -  0 ) AS ers_dcc_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LEGDEF'' AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_dcc_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''IAENG'',''RPRTS'',''LEGCOV'') AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS ers_ao_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''CREXP''  AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IN (''LEGCOV'',''IAENG'',''RPRTS'') AND feat.insrnc_cvge_type_cd = ''ERS''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS ers_ao_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''LOSS'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  0 ) AS loi_direct_loss_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd IN (''CRLOSS'',''SALVAGE'',''SUBRO'')  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND (clm_expsr_trans_lnitm.lnitm_ctgy_type_cd IS NULL OR  clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'') AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_dir_loss_recovery_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''LOI'' THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_salvage_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''LOI'' THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SALVAGE'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SALVEXP'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_salvage_expense_amt,

SUM(0- CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd = ''SUBRO''  AND clm_expsr_trans.expsr_cost_type_cd=''PDL''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''RCVRY'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_subr_recovery_amt,

SUM(CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END -  CASE WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RCVRY'' AND clm_expsr_trans.rcvry_ctgy_type_cd=''SUBRO'' AND clm_expsr_trans.expsr_cost_type_cd=''EXPNS''AND clm_expsr_trans.expsr_cost_ctgy_type_cd=''EXPNS'' AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''SUBROEXP'' AND feat.insrnc_cvge_type_cd = ''LOI''  THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt ELSE 0 END  ) AS loi_subr_expense_amt

FROM    

    DB_T_PROD_CORE.CLM

    INNER JOIN DB_T_PROD_CORE.clm_expsr ON          clm.clm_id=clm_expsr.clm_id

    LEFT JOIN  DB_T_PROD_CORE.clm_dt ON  clm.clm_id=clm_dt.clm_id AND           clm_dt_type_cd=''LOSS'' AND clm_dt.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    LEFT JOIN  DB_T_PROD_CORE.clm_loctr ON              clm.clm_id=clm_loctr.clm_id AND       clm_loctr_role_cd=''LOSSSTADRS'' AND clm_loctr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    INNER JOIN DB_T_PROD_CORE.street_addr ON clm_loctr.loc_id = street_addr.street_addr_id AND street_addr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    INNER JOIN DB_T_PROD_CORE.terr ON      terr.terr_id=street_addr.terr_id AND terr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  

    LEFT JOIN  DB_T_PROD_CORE.clm_expsr_trans ON  clm_expsr_trans.clm_expsr_id=clm_expsr.clm_expsr_id AND clm_expsr_trans.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    LEFT JOIN  DB_T_PROD_CORE.clm_expsr_trans_lnitm ON        clm_expsr_trans_lnitm.clm_expsr_trans_id=clm_expsr_trans.clm_expsr_trans_id AND clm_expsr_trans_lnitm.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    LEFT JOIN  DB_T_PROD_CORE.feat ON       clm_expsr.cvge_feat_id=feat.feat_id AND feat.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    LEFT JOIN  DB_T_PROD_CORE.clm_insrbl_int ON      clm_insrbl_int.clm_id=clm.clm_id AND clm_insrbl_int.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  and clm_insrbl_int.clm_insrbl_int_type_cd=''VEH'' 

    LEFT JOIN  DB_T_PROD_CORE.insrbl_int ON              clm_insrbl_int.insrbl_int_id=insrbl_int.insrbl_int_id AND insrbl_int.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

   LEFT JOIN DB_T_PROD_CORE.prty_asset ON              prty_asset.prty_asset_id=insrbl_int.prty_asset_id AND     prty_asset_sbtype_cd=''MVEH'' AND                prty_asset_clasfcn_cd=''MV'' AND prty_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

/*   LEFT JOIN  DB_T_PROD_CORE.motr_veh ON              prty_asset.prty_asset_id=motr_veh.prty_asset_id AND motr_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  */
       JOIN

(

Select AC.AGMT_ID,AC.CLM_ID from DB_T_PROD_CORE.AGMT_CLM AC

JOIN

DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=AC.AGMT_ID AND AG.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

AND       AG.modl_crtn_dttm = (

                                                                                SELECT                MAX(aa.modl_crtn_dttm)  

                                                                                FROM    DB_T_PROD_CORE.AGMT aa 

                                                                                WHERE aa.AGMT_TYPE_CD = ''PPV''

                                                                                AND aa.AGMT_CUR_STS_CD=''BOUND''

AND       aa.host_agmt_num=AG.host_agmt_num)             /*  To capture only the most recent model for a DB_T_CORE_DM_PROD.policy  */
WHERE

AC.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

)  AG ON CLM.CLM_ID=AG.CLM_ID



/* LEFT JOIN  DB_T_PROD_CORE.AGMT_ASSET ON          agmt_asset.prty_asset_id=prty_asset.prty_asset_id AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
/* LEFT JOIN  DB_T_PROD_CORE.AGMT_ASSET ON          AG.AGMT_ID=agmt_asset.AGMT_ID AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
LEFT JOIN  

(SELECT AGMT_ID,PRTY_ASSET_ID,asset_cntrct_role_sbtype_cd,MAX(AGMT_ASSET_STRT_DTTM) AS AGMT_ASSET_STRT_DTTM FROM DB_T_PROD_CORE.AGMT_ASSET WHERE edw_end_dttm=''9999-12-31 23:59:59.999999'' GROUP BY 1,2,3)  

agmt_ast ON          AG.AGMT_ID=agmt_ast.AGMT_ID /* AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
JOIN  DB_T_PROD_CORE.AGMT_ASSET ON          AG.AGMT_ID=agmt_asset.AGMT_ID AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

/* JOIN  DB_T_PROD_CORE.prty_asset  ON          agmt_asset.prty_asset_id=prty_asset.prty_asset_id AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
JOIN  DB_T_PROD_CORE.motr_veh ON              prty_asset.prty_asset_id=motr_veh.prty_asset_id AND motr_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

JOIN DB_T_PROD_CORE.AGMT_PROD 											

	ON	AG.AGMT_ID=AGMT_PROD.AGMT_ID AND AGMT_PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''									

JOIN DB_T_PROD_CORE.PROD 											

	ON	AGMT_PROD.PROD_ID=PROD.PROD_ID AND PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''	

	and PROD.PROD_NAME IN (''PPV'', ''AUTO'', ''COMMERCIAL'', ''PERSONAL AUTO'') 



GROUP BY 

                plcy_veh_key,

    clm_num,

    clm_loss_dt,

    acc_st_cd
) SRC
)
);


-- Component jnr_split_query, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE jnr_split_query AS
(
SELECT
sq_edw_eblm_clm_dtl1.PLCY_VEH_KEY as PLCY_VEH_KEY,
sq_edw_eblm_clm_dtl1.CLM_NBR as CLM_NBR,
sq_edw_eblm_clm_dtl1.CLM_LOSS_DT as CLM_LOSS_DT,
sq_edw_eblm_clm_dtl1.ACC_ST_CD as ACC_ST_CD,
sq_edw_eblm_clm_dtl1.BI_DIRECT_LOSS_AMT as BI_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl1.BI_DIR_LOSS_RECOVERY_AMT as BI_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.BI_SALVAGE_RECOVERY_AMT as BI_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.BI_SALVAGE_EXPENSE_AMT as BI_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.BI_SUBR_RECOVERY_AMT as BI_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.BI_SUBR_EXPENSE_AMT as BI_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.BI_DCC_EXPENSE_AMT as BI_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.BI_DCC_RECOVERY_AMT as BI_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.BI_AO_EXPENSE_AMT as BI_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.BI_AO_RECOVERY_AMT as BI_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.PD_DIRECT_LOSS_AMT as PD_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl1.PD_DIR_LOSS_RECOVERY_AMT as PD_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.PD_SALVAGE_RECOVERY_AMT as PD_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.PD_SALVAGE_EXPENSE_AMT as PD_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.PD_SUBR_RECOVERY_AMT as PD_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.PD_SUBR_EXPENSE_AMT as PD_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.PD_DCC_EXPENSE_AMT as PD_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.PD_DCC_RECOVERY_AMT as PD_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.PD_AO_EXPENSE_AMT as PD_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.PD_AO_RECOVERY_AMT as PD_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.CMP_DIR_LOSS_RECOVERY_AMT as CMP_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.CMP_SALVAGE_RECOVERY_AMT as CMP_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.CMP_SALVAGE_EXPENSE_AMT as CMP_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.CMP_SUBR_RECOVERY_AMT as CMP_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.CMP_SUBR_EXPENSE_AMT as CMP_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.CMP_DCC_EXPENSE_AMT as CMP_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.CMP_DCC_RECOVERY_AMT as CMP_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.CMP_AO_EXPENSE_AMT as CMP_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.CMP_AO_RECOVERY_AMT as CMP_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.COL_DIRECT_LOSS_AMT as COL_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl1.COL_DIR_LOSS_RECOVERY_AMT as COL_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.COL_SALVAGE_RECOVERY_AMT as COL_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.COL_SALVAGE_EXPENSE_AMT as COL_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.COL_SUBR_RECOVERY_AMT as COL_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.COL_SUBR_EXPENSE_AMT as COL_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.COL_DCC_EXPENSE_AMT as COL_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.COL_DCC_RECOVERY_AMT as COL_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.COL_AO_EXPENSE_AMT as COL_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.COL_AO_RECOVERY_AMT as COL_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.ERS_DIRECT_LOSS_AMT as ERS_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl1.ERS_DIR_LOSS_RECOVERY_AMT as ERS_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.ERS_SALVAGE_RECOVERY_AMT as ERS_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.ERS_SALVAGE_EXPENSE_AMT as ERS_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.ERS_SUBR_RECOVERY_AMT as ERS_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.ERS_SUBR_EXPENSE_AMT as ERS_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.ERS_DCC_EXPENSE_AMT as ERS_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.ERS_DCC_RECOVERY_AMT as ERS_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.ERS_AO_EXPENSE_AMT as ERS_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.ERS_AO_RECOVERY_AMT as ERS_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.LOI_DIRECT_LOSS_AMT as LOI_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl1.LOI_DIR_LOSS_RECOVERY_AMT as LOI_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.LOI_SALVAGE_RECOVERY_AMT as LOI_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.LOI_SALVAGE_EXPENSE_AMT as LOI_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.LOI_SUBR_RECOVERY_AMT as LOI_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl1.LOI_SUBR_EXPENSE_AMT as LOI_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl1.CMP_DIRECT_LOSS_AMT as CMP_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.PLCY_VEH_KEY as PLCY_VEH_KEY1,
sq_edw_eblm_clm_dtl11.CLM_NBR as CLM_NBR1,
sq_edw_eblm_clm_dtl11.CLM_LOSS_DT as CLM_LOSS_DT1,
sq_edw_eblm_clm_dtl11.ACC_ST_CD as ACC_ST_CD1,
sq_edw_eblm_clm_dtl11.LOI_DCC_EXPENSE_AMT as LOI_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.LOI_DCC_RECOVERY_AMT as LOI_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.LOI_AO_EXPENSE_AMT as LOI_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.LOI_AO_RECOVERY_AMT as LOI_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.LOU_DIRECT_LOSS_AMT as LOU_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.LOU_DIR_LOSS_RECOVERY_AMT as LOU_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.LOU_SALVAGE_RECOVERY_AMT as LOU_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.LOU_SALVAGE_EXPENSE_AMT as LOU_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.LOU_SUBR_RECOVERY_AMT as LOU_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.LOU_SUBR_EXPENSE_AMT as LOU_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.LOU_DCC_EXPENSE_AMT as LOU_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.LOU_DCC_RECOVERY_AMT as LOU_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.LOU_AO_EXPENSE_AMT as LOU_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.LOU_AO_RECOVERY_AMT as LOU_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.MED_DIRECT_LOSS_AMT as MED_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.MED_DIR_LOSS_RECOVERY_AMT as MED_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.MED_SALVAGE_RECOVERY_AMT as MED_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.MED_SALVAGE_EXPENSE_AMT as MED_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.MED_SUBR_RECOVERY_AMT as MED_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.MED_SUBR_EXPENSE_AMT as MED_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.MED_DCC_EXPENSE_AMT as MED_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.MED_DCC_RECOVERY_AMT as MED_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.MED_AO_EXPENSE_AMT as MED_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.MED_AO_RECOVERY_AMT as MED_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNB_DIRECT_LOSS_AMT as UNB_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.UNB_DIR_LOSS_RECOVERY_AMT as UNB_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNB_SALVAGE_RECOVERY_AMT as UNB_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNB_SALVAGE_EXPENSE_AMT as UNB_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNB_SUBR_RECOVERY_AMT as UNB_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNB_SUBR_EXPENSE_AMT as UNB_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNB_DCC_EXPENSE_AMT as UNB_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNB_DCC_RECOVERY_AMT as UNB_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNB_AO_EXPENSE_AMT as UNB_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNB_AO_RECOVERY_AMT as UNB_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNP_DIRECT_LOSS_AMT as UNP_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.UNP_DIR_LOSS_RECOVERY_AMT as UNP_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNP_SALVAGE_RECOVERY_AMT as UNP_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNP_SALVAGE_EXPENSE_AMT as UNP_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNP_SUBR_RECOVERY_AMT as UNP_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNP_SUBR_EXPENSE_AMT as UNP_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNP_DCC_EXPENSE_AMT as UNP_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNP_DCC_RECOVERY_AMT as UNP_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNP_AO_EXPENSE_AMT as UNP_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNP_AO_RECOVERY_AMT as UNP_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CS_DIRECT_LOSS_AMT as CS_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.CS_DIR_LOSS_RECOVERY_AMT as CS_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CS_SALVAGE_RECOVERY_AMT as CS_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CS_SALVAGE_EXPENSE_AMT as CS_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CS_SUBR_RECOVERY_AMT as CS_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CS_SUBR_EXPENSE_AMT as CS_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CS_DCC_EXPENSE_AMT as CS_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CS_DCC_RECOVERY_AMT as CS_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CS_AO_EXPENSE_AMT as CS_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CS_AO_RECOVERY_AMT as CS_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.EXN_DIRECT_LOSS_AMT as EXN_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.EXN_DIR_LOSS_RECOVERY_AMT as EXN_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.EXN_SALVAGE_RECOVERY_AMT as EXN_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.EXN_SALVAGE_EXPENSE_AMT as EXN_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.EXN_SUBR_RECOVERY_AMT as EXN_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.EXN_SUBR_EXPENSE_AMT as EXN_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.EXN_DCC_EXPENSE_AMT as EXN_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.EXN_DCC_RECOVERY_AMT as EXN_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.EXN_AO_EXPENSE_AMT as EXN_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.EXN_AO_RECOVERY_AMT as EXN_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SG_DIRECT_LOSS_AMT as SG_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.SG_DIR_LOSS_RECOVERY_AMT as SG_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SG_SALVAGE_RECOVERY_AMT as SG_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SG_SALVAGE_EXPENSE_AMT as SG_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SG_SUBR_RECOVERY_AMT as SG_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SG_SUBR_EXPENSE_AMT as SG_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SG_DCC_EXPENSE_AMT as SG_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SG_DCC_RECOVERY_AMT as SG_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SG_AO_EXPENSE_AMT as SG_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SG_AO_RECOVERY_AMT as SG_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CUS_DIRECT_LOSS_AMT as CUS_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.CUS_DIR_LOSS_RECOVERY_AMT as CUS_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CUS_SALVAGE_RECOVERY_AMT as CUS_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CUS_SALVAGE_EXPENSE_AMT as CUS_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CUS_SUBR_RECOVERY_AMT as CUS_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CUS_SUBR_EXPENSE_AMT as CUS_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CUS_DCC_EXPENSE_AMT as CUS_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CUS_DCC_RECOVERY_AMT as CUS_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.CUS_AO_EXPENSE_AMT as CUS_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.CUS_AO_RECOVERY_AMT as CUS_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.GAP_DIRECT_LOSS_AMT as GAP_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.GAP_DIR_LOSS_RECOVERY_AMT as GAP_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.GAP_SALVAGE_RECOVERY_AMT as GAP_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.GAP_SALVAGE_EXPENSE_AMT as GAP_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.GAP_SUBR_RECOVERY_AMT as GAP_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.GAP_SUBR_EXPENSE_AMT as GAP_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.GAP_DCC_EXPENSE_AMT as GAP_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.GAP_DCC_RECOVERY_AMT as GAP_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.GAP_AO_EXPENSE_AMT as GAP_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.GAP_AO_RECOVERY_AMT as GAP_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SL_DIRECT_LOSS_AMT as SL_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.SL_DIR_LOSS_RECOVERY_AMT as SL_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SL_SALVAGE_RECOVERY_AMT as SL_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SL_SALVAGE_EXPENSE_AMT as SL_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SL_SUBR_RECOVERY_AMT as SL_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SL_SUBR_EXPENSE_AMT as SL_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SL_DCC_EXPENSE_AMT as SL_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SL_DCC_RECOVERY_AMT as SL_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.SL_AO_EXPENSE_AMT as SL_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.SL_AO_RECOVERY_AMT as SL_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UND_DIRECT_LOSS_AMT as UND_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.UND_DIR_LOSS_RECOVERY_AMT as UND_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UND_SALVAGE_RECOVERY_AMT as UND_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UND_SALVAGE_EXPENSE_AMT as UND_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UND_SUBR_RECOVERY_AMT as UND_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UND_SUBR_EXPENSE_AMT as UND_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UND_DCC_EXPENSE_AMT as UND_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UND_DCC_RECOVERY_AMT as UND_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UND_AO_EXPENSE_AMT as UND_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UND_AO_RECOVERY_AMT as UND_AO_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNSL_DIRECT_LOSS_AMT as UNSL_DIRECT_LOSS_AMT,
sq_edw_eblm_clm_dtl11.UNSL_DIR_LOSS_RECOVERY_AMT as UNSL_DIR_LOSS_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNSL_SALVAGE_RECOVERY_AMT as UNSL_SALVAGE_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNSL_SALVAGE_EXPENSE_AMT as UNSL_SALVAGE_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNSL_SUBR_RECOVERY_AMT as UNSL_SUBR_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNSL_SUBR_EXPENSE_AMT as UNSL_SUBR_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNSL_DCC_EXPENSE_AMT as UNSL_DCC_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNSL_DCC_RECOVERY_AMT as UNSL_DCC_RECOVERY_AMT,
sq_edw_eblm_clm_dtl11.UNSL_AO_EXPENSE_AMT as UNSL_AO_EXPENSE_AMT,
sq_edw_eblm_clm_dtl11.UNSL_AO_RECOVERY_AMT as UNSL_AO_RECOVERY_AMT,
row_number() over (order by 1) AS source_record_id
FROM
sq_edw_eblm_clm_dtl11
INNER JOIN sq_edw_eblm_clm_dtl1 ON sq_edw_eblm_clm_dtl11.PLCY_VEH_KEY = sq_edw_eblm_clm_dtl1.PLCY_VEH_KEY AND sq_edw_eblm_clm_dtl11.CLM_NBR = sq_edw_eblm_clm_dtl1.CLM_NBR AND sq_edw_eblm_clm_dtl11.CLM_LOSS_DT = sq_edw_eblm_clm_dtl1.CLM_LOSS_DT AND sq_edw_eblm_clm_dtl11.ACC_ST_CD = sq_edw_eblm_clm_dtl1.ACC_ST_CD
);


-- Component flt_zero_amt, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_zero_amt AS
(
SELECT
jnr_split_query.PLCY_VEH_KEY as PLCY_VEH_KEY,
jnr_split_query.CLM_NBR as CLM_NBR,
jnr_split_query.CLM_LOSS_DT as CLM_LOSS_DT,
jnr_split_query.ACC_ST_CD as ACC_ST_CD,
jnr_split_query.BI_DIRECT_LOSS_AMT as BI_DIRECT_LOSS_AMT,
jnr_split_query.BI_DIR_LOSS_RECOVERY_AMT as BI_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.BI_SALVAGE_RECOVERY_AMT as BI_SALVAGE_RECOVERY_AMT,
jnr_split_query.BI_SALVAGE_EXPENSE_AMT as BI_SALVAGE_EXPENSE_AMT,
jnr_split_query.BI_SUBR_RECOVERY_AMT as BI_SUBR_RECOVERY_AMT,
jnr_split_query.BI_SUBR_EXPENSE_AMT as BI_SUBR_EXPENSE_AMT,
jnr_split_query.BI_DCC_EXPENSE_AMT as BI_DCC_EXPENSE_AMT,
jnr_split_query.BI_DCC_RECOVERY_AMT as BI_DCC_RECOVERY_AMT,
jnr_split_query.BI_AO_EXPENSE_AMT as BI_AO_EXPENSE_AMT,
jnr_split_query.BI_AO_RECOVERY_AMT as BI_AO_RECOVERY_AMT,
jnr_split_query.PD_DIRECT_LOSS_AMT as PD_DIRECT_LOSS_AMT,
jnr_split_query.PD_DIR_LOSS_RECOVERY_AMT as PD_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.PD_SALVAGE_RECOVERY_AMT as PD_SALVAGE_RECOVERY_AMT,
jnr_split_query.PD_SALVAGE_EXPENSE_AMT as PD_SALVAGE_EXPENSE_AMT,
jnr_split_query.PD_SUBR_RECOVERY_AMT as PD_SUBR_RECOVERY_AMT,
jnr_split_query.PD_SUBR_EXPENSE_AMT as PD_SUBR_EXPENSE_AMT,
jnr_split_query.PD_DCC_EXPENSE_AMT as PD_DCC_EXPENSE_AMT,
jnr_split_query.PD_DCC_RECOVERY_AMT as PD_DCC_RECOVERY_AMT,
jnr_split_query.PD_AO_EXPENSE_AMT as PD_AO_EXPENSE_AMT,
jnr_split_query.PD_AO_RECOVERY_AMT as PD_AO_RECOVERY_AMT,
jnr_split_query.CMP_DIR_LOSS_RECOVERY_AMT as CMP_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.CMP_SALVAGE_RECOVERY_AMT as CMP_SALVAGE_RECOVERY_AMT,
jnr_split_query.CMP_SALVAGE_EXPENSE_AMT as CMP_SALVAGE_EXPENSE_AMT,
jnr_split_query.CMP_SUBR_RECOVERY_AMT as CMP_SUBR_RECOVERY_AMT,
jnr_split_query.CMP_SUBR_EXPENSE_AMT as CMP_SUBR_EXPENSE_AMT,
jnr_split_query.CMP_DCC_EXPENSE_AMT as CMP_DCC_EXPENSE_AMT,
jnr_split_query.CMP_DCC_RECOVERY_AMT as CMP_DCC_RECOVERY_AMT,
jnr_split_query.CMP_AO_EXPENSE_AMT as CMP_AO_EXPENSE_AMT,
jnr_split_query.CMP_AO_RECOVERY_AMT as CMP_AO_RECOVERY_AMT,
jnr_split_query.COL_DIRECT_LOSS_AMT as COL_DIRECT_LOSS_AMT,
jnr_split_query.COL_DIR_LOSS_RECOVERY_AMT as COL_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.COL_SALVAGE_RECOVERY_AMT as COL_SALVAGE_RECOVERY_AMT,
jnr_split_query.COL_SALVAGE_EXPENSE_AMT as COL_SALVAGE_EXPENSE_AMT,
jnr_split_query.COL_SUBR_RECOVERY_AMT as COL_SUBR_RECOVERY_AMT,
jnr_split_query.COL_SUBR_EXPENSE_AMT as COL_SUBR_EXPENSE_AMT,
jnr_split_query.COL_DCC_EXPENSE_AMT as COL_DCC_EXPENSE_AMT,
jnr_split_query.COL_DCC_RECOVERY_AMT as COL_DCC_RECOVERY_AMT,
jnr_split_query.COL_AO_EXPENSE_AMT as COL_AO_EXPENSE_AMT,
jnr_split_query.COL_AO_RECOVERY_AMT as COL_AO_RECOVERY_AMT,
jnr_split_query.ERS_DIRECT_LOSS_AMT as ERS_DIRECT_LOSS_AMT,
jnr_split_query.ERS_DIR_LOSS_RECOVERY_AMT as ERS_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.ERS_SALVAGE_RECOVERY_AMT as ERS_SALVAGE_RECOVERY_AMT,
jnr_split_query.ERS_SALVAGE_EXPENSE_AMT as ERS_SALVAGE_EXPENSE_AMT,
jnr_split_query.ERS_SUBR_RECOVERY_AMT as ERS_SUBR_RECOVERY_AMT,
jnr_split_query.ERS_SUBR_EXPENSE_AMT as ERS_SUBR_EXPENSE_AMT,
jnr_split_query.ERS_DCC_EXPENSE_AMT as ERS_DCC_EXPENSE_AMT,
jnr_split_query.ERS_DCC_RECOVERY_AMT as ERS_DCC_RECOVERY_AMT,
jnr_split_query.ERS_AO_EXPENSE_AMT as ERS_AO_EXPENSE_AMT,
jnr_split_query.ERS_AO_RECOVERY_AMT as ERS_AO_RECOVERY_AMT,
jnr_split_query.LOI_DIRECT_LOSS_AMT as LOI_DIRECT_LOSS_AMT,
jnr_split_query.LOI_DIR_LOSS_RECOVERY_AMT as LOI_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.LOI_SALVAGE_RECOVERY_AMT as LOI_SALVAGE_RECOVERY_AMT,
jnr_split_query.LOI_SALVAGE_EXPENSE_AMT as LOI_SALVAGE_EXPENSE_AMT,
jnr_split_query.LOI_SUBR_RECOVERY_AMT as LOI_SUBR_RECOVERY_AMT,
jnr_split_query.LOI_SUBR_EXPENSE_AMT as LOI_SUBR_EXPENSE_AMT,
jnr_split_query.LOI_DCC_EXPENSE_AMT as LOI_DCC_EXPENSE_AMT,
jnr_split_query.LOI_DCC_RECOVERY_AMT as LOI_DCC_RECOVERY_AMT,
jnr_split_query.LOI_AO_EXPENSE_AMT as LOI_AO_EXPENSE_AMT,
jnr_split_query.LOI_AO_RECOVERY_AMT as LOI_AO_RECOVERY_AMT,
jnr_split_query.LOU_DIRECT_LOSS_AMT as LOU_DIRECT_LOSS_AMT,
jnr_split_query.LOU_DIR_LOSS_RECOVERY_AMT as LOU_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.LOU_SALVAGE_RECOVERY_AMT as LOU_SALVAGE_RECOVERY_AMT,
jnr_split_query.LOU_SALVAGE_EXPENSE_AMT as LOU_SALVAGE_EXPENSE_AMT,
jnr_split_query.LOU_SUBR_RECOVERY_AMT as LOU_SUBR_RECOVERY_AMT,
jnr_split_query.LOU_SUBR_EXPENSE_AMT as LOU_SUBR_EXPENSE_AMT,
jnr_split_query.LOU_DCC_EXPENSE_AMT as LOU_DCC_EXPENSE_AMT,
jnr_split_query.LOU_DCC_RECOVERY_AMT as LOU_DCC_RECOVERY_AMT,
jnr_split_query.LOU_AO_EXPENSE_AMT as LOU_AO_EXPENSE_AMT,
jnr_split_query.LOU_AO_RECOVERY_AMT as LOU_AO_RECOVERY_AMT,
jnr_split_query.MED_DIRECT_LOSS_AMT as MED_DIRECT_LOSS_AMT,
jnr_split_query.MED_DIR_LOSS_RECOVERY_AMT as MED_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.MED_SALVAGE_RECOVERY_AMT as MED_SALVAGE_RECOVERY_AMT,
jnr_split_query.MED_SALVAGE_EXPENSE_AMT as MED_SALVAGE_EXPENSE_AMT,
jnr_split_query.MED_SUBR_RECOVERY_AMT as MED_SUBR_RECOVERY_AMT,
jnr_split_query.MED_SUBR_EXPENSE_AMT as MED_SUBR_EXPENSE_AMT,
jnr_split_query.MED_DCC_EXPENSE_AMT as MED_DCC_EXPENSE_AMT,
jnr_split_query.MED_DCC_RECOVERY_AMT as MED_DCC_RECOVERY_AMT,
jnr_split_query.MED_AO_EXPENSE_AMT as MED_AO_EXPENSE_AMT,
jnr_split_query.MED_AO_RECOVERY_AMT as MED_AO_RECOVERY_AMT,
jnr_split_query.UNB_DIRECT_LOSS_AMT as UNB_DIRECT_LOSS_AMT,
jnr_split_query.UNB_DIR_LOSS_RECOVERY_AMT as UNB_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.UNB_SALVAGE_RECOVERY_AMT as UNB_SALVAGE_RECOVERY_AMT,
jnr_split_query.UNB_SALVAGE_EXPENSE_AMT as UNB_SALVAGE_EXPENSE_AMT,
jnr_split_query.UNB_SUBR_RECOVERY_AMT as UNB_SUBR_RECOVERY_AMT,
jnr_split_query.UNB_SUBR_EXPENSE_AMT as UNB_SUBR_EXPENSE_AMT,
jnr_split_query.UNB_DCC_EXPENSE_AMT as UNB_DCC_EXPENSE_AMT,
jnr_split_query.UNB_DCC_RECOVERY_AMT as UNB_DCC_RECOVERY_AMT,
jnr_split_query.UNB_AO_EXPENSE_AMT as UNB_AO_EXPENSE_AMT,
jnr_split_query.UNB_AO_RECOVERY_AMT as UNB_AO_RECOVERY_AMT,
jnr_split_query.UNP_DIRECT_LOSS_AMT as UNP_DIRECT_LOSS_AMT,
jnr_split_query.UNP_DIR_LOSS_RECOVERY_AMT as UNP_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.UNP_SALVAGE_RECOVERY_AMT as UNP_SALVAGE_RECOVERY_AMT,
jnr_split_query.UNP_SALVAGE_EXPENSE_AMT as UNP_SALVAGE_EXPENSE_AMT,
jnr_split_query.UNP_SUBR_RECOVERY_AMT as UNP_SUBR_RECOVERY_AMT,
jnr_split_query.UNP_SUBR_EXPENSE_AMT as UNP_SUBR_EXPENSE_AMT,
jnr_split_query.UNP_DCC_EXPENSE_AMT as UNP_DCC_EXPENSE_AMT,
jnr_split_query.UNP_DCC_RECOVERY_AMT as UNP_DCC_RECOVERY_AMT,
jnr_split_query.UNP_AO_EXPENSE_AMT as UNP_AO_EXPENSE_AMT,
jnr_split_query.UNP_AO_RECOVERY_AMT as UNP_AO_RECOVERY_AMT,
jnr_split_query.CS_DIRECT_LOSS_AMT as CS_DIRECT_LOSS_AMT,
jnr_split_query.CS_DIR_LOSS_RECOVERY_AMT as CS_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.CS_SALVAGE_RECOVERY_AMT as CS_SALVAGE_RECOVERY_AMT,
jnr_split_query.CS_SALVAGE_EXPENSE_AMT as CS_SALVAGE_EXPENSE_AMT,
jnr_split_query.CS_SUBR_RECOVERY_AMT as CS_SUBR_RECOVERY_AMT,
jnr_split_query.CS_SUBR_EXPENSE_AMT as CS_SUBR_EXPENSE_AMT,
jnr_split_query.CS_DCC_EXPENSE_AMT as CS_DCC_EXPENSE_AMT,
jnr_split_query.CS_DCC_RECOVERY_AMT as CS_DCC_RECOVERY_AMT,
jnr_split_query.CS_AO_EXPENSE_AMT as CS_AO_EXPENSE_AMT,
jnr_split_query.CS_AO_RECOVERY_AMT as CS_AO_RECOVERY_AMT,
jnr_split_query.EXN_DIRECT_LOSS_AMT as EXN_DIRECT_LOSS_AMT,
jnr_split_query.EXN_DIR_LOSS_RECOVERY_AMT as EXN_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.EXN_SALVAGE_RECOVERY_AMT as EXN_SALVAGE_RECOVERY_AMT,
jnr_split_query.EXN_SALVAGE_EXPENSE_AMT as EXN_SALVAGE_EXPENSE_AMT,
jnr_split_query.EXN_SUBR_RECOVERY_AMT as EXN_SUBR_RECOVERY_AMT,
jnr_split_query.EXN_SUBR_EXPENSE_AMT as EXN_SUBR_EXPENSE_AMT,
jnr_split_query.EXN_DCC_EXPENSE_AMT as EXN_DCC_EXPENSE_AMT,
jnr_split_query.EXN_DCC_RECOVERY_AMT as EXN_DCC_RECOVERY_AMT,
jnr_split_query.EXN_AO_EXPENSE_AMT as EXN_AO_EXPENSE_AMT,
jnr_split_query.EXN_AO_RECOVERY_AMT as EXN_AO_RECOVERY_AMT,
jnr_split_query.SG_DIRECT_LOSS_AMT as SG_DIRECT_LOSS_AMT,
jnr_split_query.SG_DIR_LOSS_RECOVERY_AMT as SG_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.SG_SALVAGE_RECOVERY_AMT as SG_SALVAGE_RECOVERY_AMT,
jnr_split_query.SG_SALVAGE_EXPENSE_AMT as SG_SALVAGE_EXPENSE_AMT,
jnr_split_query.SG_SUBR_RECOVERY_AMT as SG_SUBR_RECOVERY_AMT,
jnr_split_query.SG_SUBR_EXPENSE_AMT as SG_SUBR_EXPENSE_AMT,
jnr_split_query.SG_DCC_EXPENSE_AMT as SG_DCC_EXPENSE_AMT,
jnr_split_query.SG_DCC_RECOVERY_AMT as SG_DCC_RECOVERY_AMT,
jnr_split_query.SG_AO_EXPENSE_AMT as SG_AO_EXPENSE_AMT,
jnr_split_query.SG_AO_RECOVERY_AMT as SG_AO_RECOVERY_AMT,
jnr_split_query.CUS_DIRECT_LOSS_AMT as CUS_DIRECT_LOSS_AMT,
jnr_split_query.CMP_DIRECT_LOSS_AMT as CMP_DIRECT_LOSS_AMT,
jnr_split_query.CUS_DIR_LOSS_RECOVERY_AMT as CUS_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.CUS_SALVAGE_RECOVERY_AMT as CUS_SALVAGE_RECOVERY_AMT,
jnr_split_query.CUS_SALVAGE_EXPENSE_AMT as CUS_SALVAGE_EXPENSE_AMT,
jnr_split_query.CUS_SUBR_RECOVERY_AMT as CUS_SUBR_RECOVERY_AMT,
jnr_split_query.CUS_SUBR_EXPENSE_AMT as CUS_SUBR_EXPENSE_AMT,
jnr_split_query.CUS_DCC_EXPENSE_AMT as CUS_DCC_EXPENSE_AMT,
jnr_split_query.CUS_DCC_RECOVERY_AMT as CUS_DCC_RECOVERY_AMT,
jnr_split_query.CUS_AO_EXPENSE_AMT as CUS_AO_EXPENSE_AMT,
jnr_split_query.CUS_AO_RECOVERY_AMT as CUS_AO_RECOVERY_AMT,
jnr_split_query.GAP_DIRECT_LOSS_AMT as GAP_DIRECT_LOSS_AMT,
jnr_split_query.GAP_DIR_LOSS_RECOVERY_AMT as GAP_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.GAP_SALVAGE_RECOVERY_AMT as GAP_SALVAGE_RECOVERY_AMT,
jnr_split_query.GAP_SALVAGE_EXPENSE_AMT as GAP_SALVAGE_EXPENSE_AMT,
jnr_split_query.GAP_SUBR_RECOVERY_AMT as GAP_SUBR_RECOVERY_AMT,
jnr_split_query.GAP_SUBR_EXPENSE_AMT as GAP_SUBR_EXPENSE_AMT,
jnr_split_query.GAP_DCC_EXPENSE_AMT as GAP_DCC_EXPENSE_AMT,
jnr_split_query.GAP_DCC_RECOVERY_AMT as GAP_DCC_RECOVERY_AMT,
jnr_split_query.GAP_AO_EXPENSE_AMT as GAP_AO_EXPENSE_AMT,
jnr_split_query.GAP_AO_RECOVERY_AMT as GAP_AO_RECOVERY_AMT,
jnr_split_query.SL_DIRECT_LOSS_AMT as SL_DIRECT_LOSS_AMT,
jnr_split_query.SL_DIR_LOSS_RECOVERY_AMT as SL_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.SL_SALVAGE_RECOVERY_AMT as SL_SALVAGE_RECOVERY_AMT,
jnr_split_query.SL_SALVAGE_EXPENSE_AMT as SL_SALVAGE_EXPENSE_AMT,
jnr_split_query.SL_SUBR_RECOVERY_AMT as SL_SUBR_RECOVERY_AMT,
jnr_split_query.SL_SUBR_EXPENSE_AMT as SL_SUBR_EXPENSE_AMT,
jnr_split_query.SL_DCC_EXPENSE_AMT as SL_DCC_EXPENSE_AMT,
jnr_split_query.SL_DCC_RECOVERY_AMT as SL_DCC_RECOVERY_AMT,
jnr_split_query.SL_AO_EXPENSE_AMT as SL_AO_EXPENSE_AMT,
jnr_split_query.SL_AO_RECOVERY_AMT as SL_AO_RECOVERY_AMT,
jnr_split_query.UND_DIRECT_LOSS_AMT as UND_DIRECT_LOSS_AMT,
jnr_split_query.UND_DIR_LOSS_RECOVERY_AMT as UND_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.UND_SALVAGE_RECOVERY_AMT as UND_SALVAGE_RECOVERY_AMT,
jnr_split_query.UND_SALVAGE_EXPENSE_AMT as UND_SALVAGE_EXPENSE_AMT,
jnr_split_query.UND_SUBR_RECOVERY_AMT as UND_SUBR_RECOVERY_AMT,
jnr_split_query.UND_SUBR_EXPENSE_AMT as UND_SUBR_EXPENSE_AMT,
jnr_split_query.UND_DCC_EXPENSE_AMT as UND_DCC_EXPENSE_AMT,
jnr_split_query.UND_DCC_RECOVERY_AMT as UND_DCC_RECOVERY_AMT,
jnr_split_query.UND_AO_EXPENSE_AMT as UND_AO_EXPENSE_AMT,
jnr_split_query.UND_AO_RECOVERY_AMT as UND_AO_RECOVERY_AMT,
jnr_split_query.UNSL_DIRECT_LOSS_AMT as UNSL_DIRECT_LOSS_AMT,
jnr_split_query.UNSL_DIR_LOSS_RECOVERY_AMT as UNSL_DIR_LOSS_RECOVERY_AMT,
jnr_split_query.UNSL_SALVAGE_RECOVERY_AMT as UNSL_SALVAGE_RECOVERY_AMT,
jnr_split_query.UNSL_SALVAGE_EXPENSE_AMT as UNSL_SALVAGE_EXPENSE_AMT,
jnr_split_query.UNSL_SUBR_RECOVERY_AMT as UNSL_SUBR_RECOVERY_AMT,
jnr_split_query.UNSL_SUBR_EXPENSE_AMT as UNSL_SUBR_EXPENSE_AMT,
jnr_split_query.UNSL_DCC_EXPENSE_AMT as UNSL_DCC_EXPENSE_AMT,
jnr_split_query.UNSL_DCC_RECOVERY_AMT as UNSL_DCC_RECOVERY_AMT,
jnr_split_query.UNSL_AO_EXPENSE_AMT as UNSL_AO_EXPENSE_AMT,
jnr_split_query.UNSL_AO_RECOVERY_AMT as UNSL_AO_RECOVERY_AMT,
jnr_split_query.source_record_id
FROM
jnr_split_query
WHERE jnr_split_query.BI_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.BI_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.BI_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.BI_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.BI_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.BI_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.BI_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.BI_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.BI_AO_EXPENSE_AMT <> 0 OR jnr_split_query.BI_AO_RECOVERY_AMT <> 0 OR jnr_split_query.PD_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.PD_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.PD_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.PD_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.PD_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.PD_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.PD_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.PD_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.PD_AO_EXPENSE_AMT <> 0 OR jnr_split_query.PD_AO_RECOVERY_AMT <> 0 OR jnr_split_query.CMP_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.CMP_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.CMP_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.CMP_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.CMP_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.CMP_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.CMP_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.CMP_AO_EXPENSE_AMT <> 0 OR jnr_split_query.CMP_AO_RECOVERY_AMT <> 0 OR jnr_split_query.COL_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.COL_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.COL_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.COL_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.COL_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.COL_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.COL_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.COL_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.COL_AO_EXPENSE_AMT <> 0 OR jnr_split_query.COL_AO_RECOVERY_AMT <> 0 OR jnr_split_query.ERS_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.ERS_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.ERS_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.ERS_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.ERS_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.ERS_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.ERS_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.ERS_AO_EXPENSE_AMT <> 0 OR jnr_split_query.ERS_AO_RECOVERY_AMT <> 0 OR jnr_split_query.LOI_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.LOI_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.LOI_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.LOI_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.LOI_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.LOI_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.LOI_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.LOI_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.LOI_AO_EXPENSE_AMT <> 0 OR jnr_split_query.LOI_AO_RECOVERY_AMT <> 0 OR jnr_split_query.LOU_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.LOU_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.LOU_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.LOU_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.LOU_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.LOU_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.LOU_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.LOU_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.LOU_AO_EXPENSE_AMT <> 0 OR jnr_split_query.LOU_AO_RECOVERY_AMT <> 0 OR jnr_split_query.MED_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.MED_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.MED_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.MED_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.MED_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.MED_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.MED_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.MED_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.MED_AO_EXPENSE_AMT <> 0 OR jnr_split_query.MED_AO_RECOVERY_AMT <> 0 OR jnr_split_query.UNB_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.UNB_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.UNB_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.UNB_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.UNB_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.UNB_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.UNB_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.UNB_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.UNB_AO_EXPENSE_AMT <> 0 OR jnr_split_query.UNB_AO_RECOVERY_AMT <> 0 OR jnr_split_query.UNP_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.UNP_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.UNP_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.UNP_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.UNP_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.UNP_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.UNP_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.UNP_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.UNP_AO_EXPENSE_AMT <> 0 OR jnr_split_query.UNP_AO_RECOVERY_AMT <> 0 OR jnr_split_query.SL_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.SL_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.SL_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.SL_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.SL_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.SL_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.SL_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.SL_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.SL_AO_EXPENSE_AMT <> 0 OR jnr_split_query.SL_AO_RECOVERY_AMT <> 0 OR jnr_split_query.UNSL_DIRECT_LOSS_AMT <> 0 OR jnr_split_query.UNSL_DIR_LOSS_RECOVERY_AMT <> 0 OR jnr_split_query.UNSL_SALVAGE_RECOVERY_AMT <> 0 OR jnr_split_query.UNSL_SALVAGE_EXPENSE_AMT <> 0 OR jnr_split_query.UNSL_SUBR_RECOVERY_AMT <> 0 OR jnr_split_query.UNSL_SUBR_EXPENSE_AMT <> 0 OR jnr_split_query.UNSL_DCC_EXPENSE_AMT <> 0 OR jnr_split_query.UNSL_DCC_RECOVERY_AMT <> 0 OR jnr_split_query.UNSL_AO_EXPENSE_AMT <> 0 OR jnr_split_query.UNSL_AO_RECOVERY_AMT <> 0
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT
flt_zero_amt.PLCY_VEH_KEY as PLCY_VEH_KEY,
flt_zero_amt.CLM_NBR as CLM_NBR,
flt_zero_amt.CLM_LOSS_DT as CLM_LOSS_DT,
flt_zero_amt.ACC_ST_CD as ACC_ST_CD,
flt_zero_amt.BI_DIRECT_LOSS_AMT as BI_DIRECT_LOSS_AMT,
flt_zero_amt.BI_DIR_LOSS_RECOVERY_AMT as BI_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.BI_SALVAGE_RECOVERY_AMT as BI_SALVAGE_RECOVERY_AMT,
flt_zero_amt.BI_SALVAGE_EXPENSE_AMT as BI_SALVAGE_EXPENSE_AMT,
flt_zero_amt.BI_SUBR_RECOVERY_AMT as BI_SUBR_RECOVERY_AMT,
flt_zero_amt.BI_SUBR_EXPENSE_AMT as BI_SUBR_EXPENSE_AMT,
flt_zero_amt.BI_DCC_EXPENSE_AMT as BI_DCC_EXPENSE_AMT,
flt_zero_amt.BI_DCC_RECOVERY_AMT as BI_DCC_RECOVERY_AMT,
flt_zero_amt.BI_AO_EXPENSE_AMT as BI_AO_EXPENSE_AMT,
flt_zero_amt.BI_AO_RECOVERY_AMT as BI_AO_RECOVERY_AMT,
flt_zero_amt.PD_DIRECT_LOSS_AMT as PD_DIRECT_LOSS_AMT,
flt_zero_amt.PD_DIR_LOSS_RECOVERY_AMT as PD_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.PD_SALVAGE_RECOVERY_AMT as PD_SALVAGE_RECOVERY_AMT,
flt_zero_amt.PD_SALVAGE_EXPENSE_AMT as PD_SALVAGE_EXPENSE_AMT,
flt_zero_amt.PD_SUBR_RECOVERY_AMT as PD_SUBR_RECOVERY_AMT,
flt_zero_amt.PD_SUBR_EXPENSE_AMT as PD_SUBR_EXPENSE_AMT,
flt_zero_amt.PD_DCC_EXPENSE_AMT as PD_DCC_EXPENSE_AMT,
flt_zero_amt.PD_DCC_RECOVERY_AMT as PD_DCC_RECOVERY_AMT,
flt_zero_amt.PD_AO_EXPENSE_AMT as PD_AO_EXPENSE_AMT,
flt_zero_amt.PD_AO_RECOVERY_AMT as PD_AO_RECOVERY_AMT,
flt_zero_amt.CMP_DIR_LOSS_RECOVERY_AMT as CMP_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.CMP_SALVAGE_RECOVERY_AMT as CMP_SALVAGE_RECOVERY_AMT,
flt_zero_amt.CMP_SALVAGE_EXPENSE_AMT as CMP_SALVAGE_EXPENSE_AMT,
flt_zero_amt.CMP_SUBR_RECOVERY_AMT as CMP_SUBR_RECOVERY_AMT,
flt_zero_amt.CMP_SUBR_EXPENSE_AMT as CMP_SUBR_EXPENSE_AMT,
flt_zero_amt.CMP_DCC_EXPENSE_AMT as CMP_DCC_EXPENSE_AMT,
flt_zero_amt.CMP_DCC_RECOVERY_AMT as CMP_DCC_RECOVERY_AMT,
flt_zero_amt.CMP_AO_EXPENSE_AMT as CMP_AO_EXPENSE_AMT,
flt_zero_amt.CMP_AO_RECOVERY_AMT as CMP_AO_RECOVERY_AMT,
flt_zero_amt.COL_DIRECT_LOSS_AMT as COL_DIRECT_LOSS_AMT,
flt_zero_amt.COL_DIR_LOSS_RECOVERY_AMT as COL_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.COL_SALVAGE_RECOVERY_AMT as COL_SALVAGE_RECOVERY_AMT,
flt_zero_amt.COL_SALVAGE_EXPENSE_AMT as COL_SALVAGE_EXPENSE_AMT,
flt_zero_amt.COL_SUBR_RECOVERY_AMT as COL_SUBR_RECOVERY_AMT,
flt_zero_amt.COL_SUBR_EXPENSE_AMT as COL_SUBR_EXPENSE_AMT,
flt_zero_amt.COL_DCC_EXPENSE_AMT as COL_DCC_EXPENSE_AMT,
flt_zero_amt.COL_DCC_RECOVERY_AMT as COL_DCC_RECOVERY_AMT,
flt_zero_amt.COL_AO_EXPENSE_AMT as COL_AO_EXPENSE_AMT,
flt_zero_amt.COL_AO_RECOVERY_AMT as COL_AO_RECOVERY_AMT,
flt_zero_amt.ERS_DIRECT_LOSS_AMT as ERS_DIRECT_LOSS_AMT,
flt_zero_amt.ERS_DIR_LOSS_RECOVERY_AMT as ERS_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.ERS_SALVAGE_RECOVERY_AMT as ERS_SALVAGE_RECOVERY_AMT,
flt_zero_amt.ERS_SALVAGE_EXPENSE_AMT as ERS_SALVAGE_EXPENSE_AMT,
flt_zero_amt.ERS_SUBR_RECOVERY_AMT as ERS_SUBR_RECOVERY_AMT,
flt_zero_amt.ERS_SUBR_EXPENSE_AMT as ERS_SUBR_EXPENSE_AMT,
flt_zero_amt.ERS_DCC_EXPENSE_AMT as ERS_DCC_EXPENSE_AMT,
flt_zero_amt.ERS_DCC_RECOVERY_AMT as ERS_DCC_RECOVERY_AMT,
flt_zero_amt.ERS_AO_EXPENSE_AMT as ERS_AO_EXPENSE_AMT,
flt_zero_amt.ERS_AO_RECOVERY_AMT as ERS_AO_RECOVERY_AMT,
flt_zero_amt.LOI_DIRECT_LOSS_AMT as LOI_DIRECT_LOSS_AMT,
flt_zero_amt.LOI_DIR_LOSS_RECOVERY_AMT as LOI_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.LOI_SALVAGE_RECOVERY_AMT as LOI_SALVAGE_RECOVERY_AMT,
flt_zero_amt.LOI_SALVAGE_EXPENSE_AMT as LOI_SALVAGE_EXPENSE_AMT,
flt_zero_amt.LOI_SUBR_RECOVERY_AMT as LOI_SUBR_RECOVERY_AMT,
flt_zero_amt.LOI_SUBR_EXPENSE_AMT as LOI_SUBR_EXPENSE_AMT,
flt_zero_amt.LOI_DCC_EXPENSE_AMT as LOI_DCC_EXPENSE_AMT,
flt_zero_amt.LOI_DCC_RECOVERY_AMT as LOI_DCC_RECOVERY_AMT,
flt_zero_amt.LOI_AO_EXPENSE_AMT as LOI_AO_EXPENSE_AMT,
flt_zero_amt.LOI_AO_RECOVERY_AMT as LOI_AO_RECOVERY_AMT,
flt_zero_amt.LOU_DIRECT_LOSS_AMT as LOU_DIRECT_LOSS_AMT,
flt_zero_amt.LOU_DIR_LOSS_RECOVERY_AMT as LOU_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.LOU_SALVAGE_RECOVERY_AMT as LOU_SALVAGE_RECOVERY_AMT,
flt_zero_amt.LOU_SALVAGE_EXPENSE_AMT as LOU_SALVAGE_EXPENSE_AMT,
flt_zero_amt.LOU_SUBR_RECOVERY_AMT as LOU_SUBR_RECOVERY_AMT,
flt_zero_amt.LOU_SUBR_EXPENSE_AMT as LOU_SUBR_EXPENSE_AMT,
flt_zero_amt.LOU_DCC_EXPENSE_AMT as LOU_DCC_EXPENSE_AMT,
flt_zero_amt.LOU_DCC_RECOVERY_AMT as LOU_DCC_RECOVERY_AMT,
flt_zero_amt.LOU_AO_EXPENSE_AMT as LOU_AO_EXPENSE_AMT,
flt_zero_amt.LOU_AO_RECOVERY_AMT as LOU_AO_RECOVERY_AMT,
flt_zero_amt.MED_DIRECT_LOSS_AMT as MED_DIRECT_LOSS_AMT,
flt_zero_amt.MED_DIR_LOSS_RECOVERY_AMT as MED_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.MED_SALVAGE_RECOVERY_AMT as MED_SALVAGE_RECOVERY_AMT,
flt_zero_amt.MED_SALVAGE_EXPENSE_AMT as MED_SALVAGE_EXPENSE_AMT,
flt_zero_amt.MED_SUBR_RECOVERY_AMT as MED_SUBR_RECOVERY_AMT,
flt_zero_amt.MED_SUBR_EXPENSE_AMT as MED_SUBR_EXPENSE_AMT,
flt_zero_amt.MED_DCC_EXPENSE_AMT as MED_DCC_EXPENSE_AMT,
flt_zero_amt.MED_DCC_RECOVERY_AMT as MED_DCC_RECOVERY_AMT,
flt_zero_amt.MED_AO_EXPENSE_AMT as MED_AO_EXPENSE_AMT,
flt_zero_amt.MED_AO_RECOVERY_AMT as MED_AO_RECOVERY_AMT,
flt_zero_amt.UNB_DIRECT_LOSS_AMT as UNB_DIRECT_LOSS_AMT,
flt_zero_amt.UNB_DIR_LOSS_RECOVERY_AMT as UNB_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.UNB_SALVAGE_RECOVERY_AMT as UNB_SALVAGE_RECOVERY_AMT,
flt_zero_amt.UNB_SALVAGE_EXPENSE_AMT as UNB_SALVAGE_EXPENSE_AMT,
flt_zero_amt.UNB_SUBR_RECOVERY_AMT as UNB_SUBR_RECOVERY_AMT,
flt_zero_amt.UNB_SUBR_EXPENSE_AMT as UNB_SUBR_EXPENSE_AMT,
flt_zero_amt.UNB_DCC_EXPENSE_AMT as UNB_DCC_EXPENSE_AMT,
flt_zero_amt.UNB_DCC_RECOVERY_AMT as UNB_DCC_RECOVERY_AMT,
flt_zero_amt.UNB_AO_EXPENSE_AMT as UNB_AO_EXPENSE_AMT,
flt_zero_amt.UNB_AO_RECOVERY_AMT as UNB_AO_RECOVERY_AMT,
flt_zero_amt.UNP_DIRECT_LOSS_AMT as UNP_DIRECT_LOSS_AMT,
flt_zero_amt.UNP_DIR_LOSS_RECOVERY_AMT as UNP_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.UNP_SALVAGE_RECOVERY_AMT as UNP_SALVAGE_RECOVERY_AMT,
flt_zero_amt.UNP_SALVAGE_EXPENSE_AMT as UNP_SALVAGE_EXPENSE_AMT,
flt_zero_amt.UNP_SUBR_RECOVERY_AMT as UNP_SUBR_RECOVERY_AMT,
flt_zero_amt.UNP_SUBR_EXPENSE_AMT as UNP_SUBR_EXPENSE_AMT,
flt_zero_amt.UNP_DCC_EXPENSE_AMT as UNP_DCC_EXPENSE_AMT,
flt_zero_amt.UNP_DCC_RECOVERY_AMT as UNP_DCC_RECOVERY_AMT,
flt_zero_amt.UNP_AO_EXPENSE_AMT as UNP_AO_EXPENSE_AMT,
flt_zero_amt.UNP_AO_RECOVERY_AMT as UNP_AO_RECOVERY_AMT,
flt_zero_amt.CMP_DIRECT_LOSS_AMT as CMP_DIRECT_LOSS_AMT,
flt_zero_amt.SL_DIRECT_LOSS_AMT as SL_DIRECT_LOSS_AMT,
flt_zero_amt.SL_DIR_LOSS_RECOVERY_AMT as SL_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.SL_SALVAGE_RECOVERY_AMT as SL_SALVAGE_RECOVERY_AMT,
flt_zero_amt.SL_SALVAGE_EXPENSE_AMT as SL_SALVAGE_EXPENSE_AMT,
flt_zero_amt.SL_SUBR_RECOVERY_AMT as SL_SUBR_RECOVERY_AMT,
flt_zero_amt.SL_SUBR_EXPENSE_AMT as SL_SUBR_EXPENSE_AMT,
flt_zero_amt.SL_DCC_EXPENSE_AMT as SL_DCC_EXPENSE_AMT,
flt_zero_amt.SL_DCC_RECOVERY_AMT as SL_DCC_RECOVERY_AMT,
flt_zero_amt.SL_AO_EXPENSE_AMT as SL_AO_EXPENSE_AMT,
flt_zero_amt.SL_AO_RECOVERY_AMT as SL_AO_RECOVERY_AMT,
flt_zero_amt.UNSL_DIRECT_LOSS_AMT as UNSL_DIRECT_LOSS_AMT,
flt_zero_amt.UNSL_DIR_LOSS_RECOVERY_AMT as UNSL_DIR_LOSS_RECOVERY_AMT,
flt_zero_amt.UNSL_SALVAGE_RECOVERY_AMT as UNSL_SALVAGE_RECOVERY_AMT,
flt_zero_amt.UNSL_SALVAGE_EXPENSE_AMT as UNSL_SALVAGE_EXPENSE_AMT,
flt_zero_amt.UNSL_SUBR_RECOVERY_AMT as UNSL_SUBR_RECOVERY_AMT,
flt_zero_amt.UNSL_SUBR_EXPENSE_AMT as UNSL_SUBR_EXPENSE_AMT,
flt_zero_amt.UNSL_DCC_EXPENSE_AMT as UNSL_DCC_EXPENSE_AMT,
flt_zero_amt.UNSL_DCC_RECOVERY_AMT as UNSL_DCC_RECOVERY_AMT,
flt_zero_amt.UNSL_AO_EXPENSE_AMT as UNSL_AO_EXPENSE_AMT,
flt_zero_amt.UNSL_AO_RECOVERY_AMT as UNSL_AO_RECOVERY_AMT,
:PRCS_ID as prcs_id,
CURRENT_TIMESTAMP as load_dt,
flt_zero_amt.source_record_id
FROM
flt_zero_amt
);


-- Component tgt_edw_eblm_clm_dtl1, Type TARGET 
INSERT INTO DB_T_PROD_WRK.EDW_EBLM_CLM_DTL1
(
PLCY_VEH_KEY,
CLM_NBR,
CLM_LOSS_DT,
ACC_ST_CD,
BI_DIRECT_LOSS_AMT,
BI_DIR_LOSS_RECOVERY_AMT,
BI_SALVAGE_RECOVERY_AMT,
BI_SALVAGE_EXPENSE_AMT,
BI_SUBR_RECOVERY_AMT,
BI_SUBR_EXPENSE_AMT,
BI_DCC_EXPENSE_AMT,
BI_DCC_RECOVERY_AMT,
BI_AO_EXPENSE_AMT,
BI_AO_RECOVERY_AMT,
PD_DIRECT_LOSS_AMT,
PD_DIR_LOSS_RECOVERY_AMT,
PD_SALVAGE_RECOVERY_AMT,
PD_SALVAGE_EXPENSE_AMT,
PD_SUBR_RECOVERY_AMT,
PD_SUBR_EXPENSE_AMT,
PD_DCC_EXPENSE_AMT,
PD_DCC_RECOVERY_AMT,
PD_AO_EXPENSE_AMT,
PD_AO_RECOVERY_AMT,
CMP_DIR_LOSS_RECOVERY_AMT,
CMP_SALVAGE_RECOVERY_AMT,
CMP_SALVAGE_EXPENSE_AMT,
CMP_SUBR_RECOVERY_AMT,
CMP_SUBR_EXPENSE_AMT,
CMP_DCC_EXPENSE_AMT,
CMP_DCC_RECOVERY_AMT,
CMP_AO_EXPENSE_AMT,
CMP_AO_RECOVERY_AMT,
CMP_DIRECT_LOSS_AMT,
COL_DIRECT_LOSS_AMT,
COL_DIR_LOSS_RECOVERY_AMT,
COL_SALVAGE_RECOVERY_AMT,
COL_SALVAGE_EXPENSE_AMT,
COL_SUBR_RECOVERY_AMT,
COL_SUBR_EXPENSE_AMT,
COL_DCC_EXPENSE_AMT,
COL_DCC_RECOVERY_AMT,
COL_AO_EXPENSE_AMT,
COL_AO_RECOVERY_AMT,
ERS_DIRECT_LOSS_AMT,
ERS_DIR_LOSS_RECOVERY_AMT,
ERS_SALVAGE_RECOVERY_AMT,
ERS_SALVAGE_EXPENSE_AMT,
ERS_SUBR_RECOVERY_AMT,
ERS_SUBR_EXPENSE_AMT,
ERS_DCC_EXPENSE_AMT,
ERS_DCC_RECOVERY_AMT,
ERS_AO_EXPENSE_AMT,
ERS_AO_RECOVERY_AMT,
LOI_DIRECT_LOSS_AMT,
LOI_DIR_LOSS_RECOVERY_AMT,
LOI_SALVAGE_RECOVERY_AMT,
LOI_SALVAGE_EXPENSE_AMT,
LOI_SUBR_RECOVERY_AMT,
LOI_SUBR_EXPENSE_AMT,
LOI_DCC_EXPENSE_AMT,
LOI_DCC_RECOVERY_AMT,
LOI_AO_EXPENSE_AMT,
LOI_AO_RECOVERY_AMT,
LOU_DIRECT_LOSS_AMT,
LOU_DIR_LOSS_RECOVERY_AMT,
LOU_SALVAGE_RECOVERY_AMT,
LOU_SALVAGE_EXPENSE_AMT,
LOU_SUBR_RECOVERY_AMT,
LOU_SUBR_EXPENSE_AMT,
LOU_DCC_EXPENSE_AMT,
LOU_DCC_RECOVERY_AMT,
LOU_AO_EXPENSE_AMT,
LOU_AO_RECOVERY_AMT,
MED_DIRECT_LOSS_AMT,
MED_DIR_LOSS_RECOVERY_AMT,
MED_SALVAGE_RECOVERY_AMT,
MED_SALVAGE_EXPENSE_AMT,
MED_SUBR_RECOVERY_AMT,
MED_SUBR_EXPENSE_AMT,
MED_DCC_EXPENSE_AMT,
MED_DCC_RECOVERY_AMT,
MED_AO_EXPENSE_AMT,
MED_AO_RECOVERY_AMT,
UNB_DIRECT_LOSS_AMT,
UNB_DIR_LOSS_RECOVERY_AMT,
UNB_SALVAGE_RECOVERY_AMT,
UNB_SALVAGE_EXPENSE_AMT,
UNB_SUBR_RECOVERY_AMT,
UNB_SUBR_EXPENSE_AMT,
UNB_DCC_EXPENSE_AMT,
UNB_DCC_RECOVERY_AMT,
UNB_AO_EXPENSE_AMT,
UNB_AO_RECOVERY_AMT,
UNP_DIRECT_LOSS_AMT,
UNP_DIR_LOSS_RECOVERY_AMT,
UNP_SALVAGE_RECOVERY_AMT,
UNP_SALVAGE_EXPENSE_AMT,
UNP_SUBR_RECOVERY_AMT,
UNP_SUBR_EXPENSE_AMT,
UNP_DCC_EXPENSE_AMT,
UNP_DCC_RECOVERY_AMT,
UNP_AO_EXPENSE_AMT,
UNP_AO_RECOVERY_AMT,
SL_DIRECT_LOSS_AMT,
SL_DIR_LOSS_RECOVERY_AMT,
SL_SALVAGE_RECOVERY_AMT,
SL_SALVAGE_EXPENSE_AMT,
SL_SUBR_RECOVERY_AMT,
SL_SUBR_EXPENSE_AMT,
SL_DCC_EXPENSE_AMT,
SL_DCC_RECOVERY_AMT,
SL_AO_EXPENSE_AMT,
SL_AO_RECOVERY_AMT,
UNSL_DIRECT_LOSS_AMT,
UNSL_DIR_LOSS_RECOVERY_AMT,
UNSL_SALVAGE_RECOVERY_AMT,
UNSL_SALVAGE_EXPENSE_AMT,
UNSL_SUBR_RECOVERY_AMT,
UNSL_SUBR_EXPENSE_AMT,
UNSL_DCC_EXPENSE_AMT,
UNSL_DCC_RECOVERY_AMT,
UNSL_AO_EXPENSE_AMT,
UNSL_AO_RECOVERY_AMT,
PRCS_ID,
LOAD_DT
)
SELECT
exp_pass_to_target.PLCY_VEH_KEY as PLCY_VEH_KEY,
exp_pass_to_target.CLM_NBR as CLM_NBR,
exp_pass_to_target.CLM_LOSS_DT as CLM_LOSS_DT,
exp_pass_to_target.ACC_ST_CD as ACC_ST_CD,
exp_pass_to_target.BI_DIRECT_LOSS_AMT as BI_DIRECT_LOSS_AMT,
exp_pass_to_target.BI_DIR_LOSS_RECOVERY_AMT as BI_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.BI_SALVAGE_RECOVERY_AMT as BI_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.BI_SALVAGE_EXPENSE_AMT as BI_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.BI_SUBR_RECOVERY_AMT as BI_SUBR_RECOVERY_AMT,
exp_pass_to_target.BI_SUBR_EXPENSE_AMT as BI_SUBR_EXPENSE_AMT,
exp_pass_to_target.BI_DCC_EXPENSE_AMT as BI_DCC_EXPENSE_AMT,
exp_pass_to_target.BI_DCC_RECOVERY_AMT as BI_DCC_RECOVERY_AMT,
exp_pass_to_target.BI_AO_EXPENSE_AMT as BI_AO_EXPENSE_AMT,
exp_pass_to_target.BI_AO_RECOVERY_AMT as BI_AO_RECOVERY_AMT,
exp_pass_to_target.PD_DIRECT_LOSS_AMT as PD_DIRECT_LOSS_AMT,
exp_pass_to_target.PD_DIR_LOSS_RECOVERY_AMT as PD_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.PD_SALVAGE_RECOVERY_AMT as PD_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.PD_SALVAGE_EXPENSE_AMT as PD_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.PD_SUBR_RECOVERY_AMT as PD_SUBR_RECOVERY_AMT,
exp_pass_to_target.PD_SUBR_EXPENSE_AMT as PD_SUBR_EXPENSE_AMT,
exp_pass_to_target.PD_DCC_EXPENSE_AMT as PD_DCC_EXPENSE_AMT,
exp_pass_to_target.PD_DCC_RECOVERY_AMT as PD_DCC_RECOVERY_AMT,
exp_pass_to_target.PD_AO_EXPENSE_AMT as PD_AO_EXPENSE_AMT,
exp_pass_to_target.PD_AO_RECOVERY_AMT as PD_AO_RECOVERY_AMT,
exp_pass_to_target.CMP_DIR_LOSS_RECOVERY_AMT as CMP_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.CMP_SALVAGE_RECOVERY_AMT as CMP_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.CMP_SALVAGE_EXPENSE_AMT as CMP_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.CMP_SUBR_RECOVERY_AMT as CMP_SUBR_RECOVERY_AMT,
exp_pass_to_target.CMP_SUBR_EXPENSE_AMT as CMP_SUBR_EXPENSE_AMT,
exp_pass_to_target.CMP_DCC_EXPENSE_AMT as CMP_DCC_EXPENSE_AMT,
exp_pass_to_target.CMP_DCC_RECOVERY_AMT as CMP_DCC_RECOVERY_AMT,
exp_pass_to_target.CMP_AO_EXPENSE_AMT as CMP_AO_EXPENSE_AMT,
exp_pass_to_target.CMP_AO_RECOVERY_AMT as CMP_AO_RECOVERY_AMT,
exp_pass_to_target.CMP_DIRECT_LOSS_AMT as CMP_DIRECT_LOSS_AMT,
exp_pass_to_target.COL_DIRECT_LOSS_AMT as COL_DIRECT_LOSS_AMT,
exp_pass_to_target.COL_DIR_LOSS_RECOVERY_AMT as COL_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.COL_SALVAGE_RECOVERY_AMT as COL_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.COL_SALVAGE_EXPENSE_AMT as COL_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.COL_SUBR_RECOVERY_AMT as COL_SUBR_RECOVERY_AMT,
exp_pass_to_target.COL_SUBR_EXPENSE_AMT as COL_SUBR_EXPENSE_AMT,
exp_pass_to_target.COL_DCC_EXPENSE_AMT as COL_DCC_EXPENSE_AMT,
exp_pass_to_target.COL_DCC_RECOVERY_AMT as COL_DCC_RECOVERY_AMT,
exp_pass_to_target.COL_AO_EXPENSE_AMT as COL_AO_EXPENSE_AMT,
exp_pass_to_target.COL_AO_RECOVERY_AMT as COL_AO_RECOVERY_AMT,
exp_pass_to_target.ERS_DIRECT_LOSS_AMT as ERS_DIRECT_LOSS_AMT,
exp_pass_to_target.ERS_DIR_LOSS_RECOVERY_AMT as ERS_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.ERS_SALVAGE_RECOVERY_AMT as ERS_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.ERS_SALVAGE_EXPENSE_AMT as ERS_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.ERS_SUBR_RECOVERY_AMT as ERS_SUBR_RECOVERY_AMT,
exp_pass_to_target.ERS_SUBR_EXPENSE_AMT as ERS_SUBR_EXPENSE_AMT,
exp_pass_to_target.ERS_DCC_EXPENSE_AMT as ERS_DCC_EXPENSE_AMT,
exp_pass_to_target.ERS_DCC_RECOVERY_AMT as ERS_DCC_RECOVERY_AMT,
exp_pass_to_target.ERS_AO_EXPENSE_AMT as ERS_AO_EXPENSE_AMT,
exp_pass_to_target.ERS_AO_RECOVERY_AMT as ERS_AO_RECOVERY_AMT,
exp_pass_to_target.LOI_DIRECT_LOSS_AMT as LOI_DIRECT_LOSS_AMT,
exp_pass_to_target.LOI_DIR_LOSS_RECOVERY_AMT as LOI_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.LOI_SALVAGE_RECOVERY_AMT as LOI_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.LOI_SALVAGE_EXPENSE_AMT as LOI_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.LOI_SUBR_RECOVERY_AMT as LOI_SUBR_RECOVERY_AMT,
exp_pass_to_target.LOI_SUBR_EXPENSE_AMT as LOI_SUBR_EXPENSE_AMT,
exp_pass_to_target.LOI_DCC_EXPENSE_AMT as LOI_DCC_EXPENSE_AMT,
exp_pass_to_target.LOI_DCC_RECOVERY_AMT as LOI_DCC_RECOVERY_AMT,
exp_pass_to_target.LOI_AO_EXPENSE_AMT as LOI_AO_EXPENSE_AMT,
exp_pass_to_target.LOI_AO_RECOVERY_AMT as LOI_AO_RECOVERY_AMT,
exp_pass_to_target.LOU_DIRECT_LOSS_AMT as LOU_DIRECT_LOSS_AMT,
exp_pass_to_target.LOU_DIR_LOSS_RECOVERY_AMT as LOU_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.LOU_SALVAGE_RECOVERY_AMT as LOU_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.LOU_SALVAGE_EXPENSE_AMT as LOU_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.LOU_SUBR_RECOVERY_AMT as LOU_SUBR_RECOVERY_AMT,
exp_pass_to_target.LOU_SUBR_EXPENSE_AMT as LOU_SUBR_EXPENSE_AMT,
exp_pass_to_target.LOU_DCC_EXPENSE_AMT as LOU_DCC_EXPENSE_AMT,
exp_pass_to_target.LOU_DCC_RECOVERY_AMT as LOU_DCC_RECOVERY_AMT,
exp_pass_to_target.LOU_AO_EXPENSE_AMT as LOU_AO_EXPENSE_AMT,
exp_pass_to_target.LOU_AO_RECOVERY_AMT as LOU_AO_RECOVERY_AMT,
exp_pass_to_target.MED_DIRECT_LOSS_AMT as MED_DIRECT_LOSS_AMT,
exp_pass_to_target.MED_DIR_LOSS_RECOVERY_AMT as MED_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.MED_SALVAGE_RECOVERY_AMT as MED_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.MED_SALVAGE_EXPENSE_AMT as MED_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.MED_SUBR_RECOVERY_AMT as MED_SUBR_RECOVERY_AMT,
exp_pass_to_target.MED_SUBR_EXPENSE_AMT as MED_SUBR_EXPENSE_AMT,
exp_pass_to_target.MED_DCC_EXPENSE_AMT as MED_DCC_EXPENSE_AMT,
exp_pass_to_target.MED_DCC_RECOVERY_AMT as MED_DCC_RECOVERY_AMT,
exp_pass_to_target.MED_AO_EXPENSE_AMT as MED_AO_EXPENSE_AMT,
exp_pass_to_target.MED_AO_RECOVERY_AMT as MED_AO_RECOVERY_AMT,
exp_pass_to_target.UNB_DIRECT_LOSS_AMT as UNB_DIRECT_LOSS_AMT,
exp_pass_to_target.UNB_DIR_LOSS_RECOVERY_AMT as UNB_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.UNB_SALVAGE_RECOVERY_AMT as UNB_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.UNB_SALVAGE_EXPENSE_AMT as UNB_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.UNB_SUBR_RECOVERY_AMT as UNB_SUBR_RECOVERY_AMT,
exp_pass_to_target.UNB_SUBR_EXPENSE_AMT as UNB_SUBR_EXPENSE_AMT,
exp_pass_to_target.UNB_DCC_EXPENSE_AMT as UNB_DCC_EXPENSE_AMT,
exp_pass_to_target.UNB_DCC_RECOVERY_AMT as UNB_DCC_RECOVERY_AMT,
exp_pass_to_target.UNB_AO_EXPENSE_AMT as UNB_AO_EXPENSE_AMT,
exp_pass_to_target.UNB_AO_RECOVERY_AMT as UNB_AO_RECOVERY_AMT,
exp_pass_to_target.UNP_DIRECT_LOSS_AMT as UNP_DIRECT_LOSS_AMT,
exp_pass_to_target.UNP_DIR_LOSS_RECOVERY_AMT as UNP_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.UNP_SALVAGE_RECOVERY_AMT as UNP_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.UNP_SALVAGE_EXPENSE_AMT as UNP_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.UNP_SUBR_RECOVERY_AMT as UNP_SUBR_RECOVERY_AMT,
exp_pass_to_target.UNP_SUBR_EXPENSE_AMT as UNP_SUBR_EXPENSE_AMT,
exp_pass_to_target.UNP_DCC_EXPENSE_AMT as UNP_DCC_EXPENSE_AMT,
exp_pass_to_target.UNP_DCC_RECOVERY_AMT as UNP_DCC_RECOVERY_AMT,
exp_pass_to_target.UNP_AO_EXPENSE_AMT as UNP_AO_EXPENSE_AMT,
exp_pass_to_target.UNP_AO_RECOVERY_AMT as UNP_AO_RECOVERY_AMT,
exp_pass_to_target.SL_DIRECT_LOSS_AMT as SL_DIRECT_LOSS_AMT,
exp_pass_to_target.SL_DIR_LOSS_RECOVERY_AMT as SL_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.SL_SALVAGE_RECOVERY_AMT as SL_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.SL_SALVAGE_EXPENSE_AMT as SL_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.SL_SUBR_RECOVERY_AMT as SL_SUBR_RECOVERY_AMT,
exp_pass_to_target.SL_SUBR_EXPENSE_AMT as SL_SUBR_EXPENSE_AMT,
exp_pass_to_target.SL_DCC_EXPENSE_AMT as SL_DCC_EXPENSE_AMT,
exp_pass_to_target.SL_DCC_RECOVERY_AMT as SL_DCC_RECOVERY_AMT,
exp_pass_to_target.SL_AO_EXPENSE_AMT as SL_AO_EXPENSE_AMT,
exp_pass_to_target.SL_AO_RECOVERY_AMT as SL_AO_RECOVERY_AMT,
exp_pass_to_target.UNSL_DIRECT_LOSS_AMT as UNSL_DIRECT_LOSS_AMT,
exp_pass_to_target.UNSL_DIR_LOSS_RECOVERY_AMT as UNSL_DIR_LOSS_RECOVERY_AMT,
exp_pass_to_target.UNSL_SALVAGE_RECOVERY_AMT as UNSL_SALVAGE_RECOVERY_AMT,
exp_pass_to_target.UNSL_SALVAGE_EXPENSE_AMT as UNSL_SALVAGE_EXPENSE_AMT,
exp_pass_to_target.UNSL_SUBR_RECOVERY_AMT as UNSL_SUBR_RECOVERY_AMT,
exp_pass_to_target.UNSL_SUBR_EXPENSE_AMT as UNSL_SUBR_EXPENSE_AMT,
exp_pass_to_target.UNSL_DCC_EXPENSE_AMT as UNSL_DCC_EXPENSE_AMT,
exp_pass_to_target.UNSL_DCC_RECOVERY_AMT as UNSL_DCC_RECOVERY_AMT,
exp_pass_to_target.UNSL_AO_EXPENSE_AMT as UNSL_AO_EXPENSE_AMT,
exp_pass_to_target.UNSL_AO_RECOVERY_AMT as UNSL_AO_RECOVERY_AMT,
exp_pass_to_target.prcs_id as PRCS_ID,
exp_pass_to_target.load_dt as LOAD_DT
FROM
exp_pass_to_target;


END; ';