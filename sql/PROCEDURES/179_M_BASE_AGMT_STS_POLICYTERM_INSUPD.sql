-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_STS_POLICYTERM_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	PRCS_ID int;

BEGIN 
start_dttm := current_timestamp();
end_dttm := current_timestamp();
PRCS_ID := 1;

-- Component SQ_pc_policyterm_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyterm_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicyNumber,
$2 as PolicyStatus,
$3 as ConfirmationDate_alfa,
$4 as TermNumber,
$5 as SRC_SYS_CD,
$6 as retired,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT pc_policyterm_x.PolicyNumber, 

''AGMT_STS_TYPE7'' as PolicyStatus, 

pc_policyterm_x.ConfirmationDate_alfa, 

pc_policyterm_x.TermNumber, pc_policyterm_x.SRC_SYS_CD, 

pc_policyterm_x.retired 

FROM

(

select 

PolicyNumber_stg PolicyNumber,

''CONFIRMED'' as PolicyStatus,

ConfirmationDate_alfa_stg ConfirmationDate_alfa, 

TermNumber_stg TermNumber,

''GWPC''  as SRC_SYS_CD,

pc_policyterm.retired_stg retired

from DB_T_PROD_STAG.pc_policyterm

join DB_T_PROD_STAG.pc_policyperiod on pc_policyterm.ID_stg=pc_policyperiod.PolicyTermID_stg

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus 

ON pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

WHERE pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pc_policyperiod.PolicyNumber_stg is not null

and pc_policyperiod.PolicyNumber_stg is not null

and pc_policyterm.UpdateTime_stg > (:start_dttm)

and pc_policyterm.UpdateTime_stg <= (:end_dttm)

group by pc_policyperiod.PolicyNumber_stg,pc_policyperiod.Termnumber_stg,ConfirmationDate_alfa_stg,pc_policyterm.retired_stg

)pc_policyterm_x

where pc_policyterm_x.ConfirmationDate_alfa is not null
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
SQ_pc_policyterm_x.PolicyNumber as PolicyNumber,
SQ_pc_policyterm_x.PolicyStatus as Agmt_Status,
SQ_pc_policyterm_x.ConfirmationDate_alfa as PolicyStatus_Dttm,
SQ_pc_policyterm_x.TermNumber as TermNumber,
SQ_pc_policyterm_x.retired as retired1,
SQ_pc_policyterm_x.source_record_id
FROM
SQ_pc_policyterm_x
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_from_source.PolicyNumber as PolicyNumber,
exp_pass_from_source.Agmt_Status as in_Agmt_Status,
exp_pass_from_source.PolicyStatus_Dttm as PolicyStatus_Dttm,
''POLTRM'' as out_AGMT_TYPE_CD,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_AGMT_STS_END_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
:PRCS_ID as out_PRCS_ID,
exp_pass_from_source.TermNumber as TermNumber,
exp_pass_from_source.retired1 as Retired,
exp_pass_from_source.source_record_id
FROM
exp_pass_from_source
);


-- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	db_t_prod_core.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''AGMT_STS_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''out_EDWPolicyStatus_PC.PolicyStatus'',''cctl_policystatus.typecode'',''derived'') 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'',''DS'') 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_data_transformation.in_Agmt_Status
QUALIFY RNK = 1
);


-- Component LKP_AGMT_POLTRM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_POLTRM AS
(
SELECT
LKP.AGMT_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_BUSN_TYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.MORTGAGEE_PREM_PMT_IND asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_NUM asc,LKP.MODL_CRTN_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.TERM_NUM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.PRIOR_INSRNC_IND asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.NK_SRC_KEY asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_BUSN_TYPE_CD as INSRNC_BUSN_TYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.MORTGAGEE_PREM_PMT_IND as MORTGAGEE_PREM_PMT_IND, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_NUM as MODL_NUM, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.PRIOR_INSRNC_IND as PRIOR_INSRNC_IND, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.TERM_NUM as TERM_NUM, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM db_t_prod_core.AGMT
 QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.HOST_AGMT_NUM, AGMT.TERM_NUM, AGMT.AGMT_TYPE_CD  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.HOST_AGMT_NUM = exp_data_transformation.PolicyNumber AND LKP.TERM_NUM = exp_data_transformation.TermNumber AND LKP.AGMT_TYPE_CD = exp_data_transformation.out_AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_AGMT_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_STS AS
(
SELECT
LKP.AGMT_ID,
LKP.AGMT_STS_CD,
LKP.AGMT_STS_RSN_CD,
LKP.AGMT_STS_STRT_DTTM,
LKP.AGMT_STS_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.AGMT_STS_CD asc,LKP.AGMT_STS_RSN_CD asc,LKP.AGMT_STS_STRT_DTTM asc,LKP.AGMT_STS_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE
INNER JOIN LKP_AGMT_POLTRM ON LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.source_record_id = LKP_AGMT_POLTRM.source_record_id
LEFT JOIN (
SELECT AGMT_STS.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD, AGMT_STS.AGMT_STS_STRT_DTTM as AGMT_STS_STRT_DTTM, AGMT_STS.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM, AGMT_STS.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_STS.EDW_END_DTTM as EDW_END_DTTM, AGMT_STS.AGMT_ID as AGMT_ID, AGMT_STS.AGMT_STS_CD as AGMT_STS_CD FROM db_t_prod_core.AGMT_STS 
WHERE AGMT_STS_CD = ''CNFRMDDT''
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.AGMT_ID = LKP_AGMT_POLTRM.AGMT_ID AND LKP.AGMT_STS_CD = LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.TGT_IDNTFTN_VAL
QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.AGMT_STS_CD asc,LKP.AGMT_STS_RSN_CD asc,LKP.AGMT_STS_STRT_DTTM asc,LKP.AGMT_STS_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc)  
= 1
);


-- Component exp_insert_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_insert_update AS
(
SELECT
LKP_AGMT_STS.AGMT_ID as LKP_AGMT_ID,
LKP_AGMT_STS.AGMT_STS_CD as LKP_AGMT_STS,
LKP_AGMT_STS.AGMT_STS_RSN_CD as LKP_AGMT_STS_RSN_CD,
LKP_AGMT_STS.AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
LKP_AGMT_STS.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
LKP_AGMT_STS.EDW_END_DTTM as LKP_EDW_END_DTTM,
MD5 ( LTRIM ( RTRIM ( LKP_AGMT_STS.AGMT_STS_CD ) ) ) as ORIG_CHKSM,
LKP_AGMT_POLTRM.AGMT_ID as AGMT_ID,
LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.TGT_IDNTFTN_VAL as SRC_AGMT_STS_CD,
exp_data_transformation.PolicyStatus_Dttm as PolicyStatus_Dttm,
exp_data_transformation.out_AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
NULL as CreationTS,
MD5 ( LTRIM ( RTRIM ( LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.TGT_IDNTFTN_VAL ) ) ) as CALC_CHKSM,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
NULL as AGMT_STS_RSN_CD,
exp_data_transformation.out_EDW_END_DTTM as out_EDW_END_DTTM,
NULL as source_name,
CASE WHEN ORIG_CHKSM IS NULL THEN ''I'' ELSE CASE WHEN ORIG_CHKSM != CALC_CHKSM THEN ''U'' ELSE ''R'' END END as out_ins_upd,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
exp_data_transformation.Retired as Retired,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE ON exp_data_transformation.source_record_id = LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.source_record_id
INNER JOIN LKP_AGMT_POLTRM ON LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE.source_record_id = LKP_AGMT_POLTRM.source_record_id
INNER JOIN LKP_AGMT_STS ON LKP_AGMT_POLTRM.source_record_id = LKP_AGMT_STS.source_record_id
);


-- Component exp_check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check AS
(
SELECT
exp_insert_update.AGMT_ID as AGMT_ID,
exp_insert_update.SRC_AGMT_STS_CD as AGMT_STS_CD,
exp_insert_update.PolicyStatus_Dttm as PolicyStatus_Dttm,
exp_insert_update.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
exp_insert_update.LKP_AGMT_ID as LKP_AGMT_ID,
exp_insert_update.LKP_AGMT_STS as LKP_AGMT_STS,
exp_insert_update.LKP_AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
exp_insert_update.PRCS_ID as PRCS_ID,
exp_insert_update.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD,
exp_insert_update.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_insert_update.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_insert_update.out_ins_upd as out_ins_upd,
exp_insert_update.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_insert_update.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_insert_update.Retired as Retired,
exp_insert_update.source_record_id
FROM
exp_insert_update
);


-- Component rtr_AGMT_STS_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_AGMT_STS_INSERT AS
SELECT
exp_check.AGMT_ID as AGMT_ID,
exp_check.AGMT_STS_CD as AGMT_STS_CD,
exp_check.PolicyStatus_Dttm as CreationTS,
exp_check.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
exp_check.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD,
exp_check.PRCS_ID as PRCS_ID,
exp_check.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_check.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_check.out_ins_upd as out_ins_upd,
exp_insert_update.source_name as source_name,
exp_check.Retired as Retired,
exp_check.LKP_AGMT_ID as LKP_AGMT_ID,
exp_check.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM4,
exp_check.LKP_AGMT_STS as LKP_AGMT_STS,
exp_check.LKP_AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
NULL as AGMT_ID_Static,
exp_check.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_check.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_insert_update.source_record_id
FROM
exp_insert_update
LEFT JOIN exp_check ON exp_insert_update.source_record_id = exp_check.source_record_id
WHERE exp_check.out_ins_upd = ''I'' and exp_check.AGMT_ID IS NOT NULL OR ( exp_check.LKP_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_check.Retired = 0 );


-- Component rtr_AGMT_STS_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE rtr_AGMT_STS_RETIRED AS
SELECT
exp_check.AGMT_ID as AGMT_ID,
exp_check.AGMT_STS_CD as AGMT_STS_CD,
exp_check.PolicyStatus_Dttm as CreationTS,
exp_check.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
exp_check.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD,
exp_check.PRCS_ID as PRCS_ID,
exp_check.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_check.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_check.out_ins_upd as out_ins_upd,
exp_insert_update.source_name as source_name,
exp_check.Retired as Retired,
exp_check.LKP_AGMT_ID as LKP_AGMT_ID,
exp_check.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM4,
exp_check.LKP_AGMT_STS as LKP_AGMT_STS,
exp_check.LKP_AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
NULL as AGMT_ID_Static,
exp_check.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_check.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_insert_update.source_record_id
FROM
exp_insert_update
LEFT JOIN exp_check ON exp_insert_update.source_record_id = exp_check.source_record_id
WHERE exp_check.out_ins_upd = ''R'' and exp_check.Retired != 0 and exp_check.LKP_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component rtr_AGMT_STS_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_AGMT_STS_UPDATE AS
SELECT
exp_check.AGMT_ID as AGMT_ID,
exp_check.AGMT_STS_CD as AGMT_STS_CD,
exp_check.PolicyStatus_Dttm as CreationTS,
exp_check.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
exp_check.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD,
exp_check.PRCS_ID as PRCS_ID,
exp_check.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_check.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_check.out_ins_upd as out_ins_upd,
exp_insert_update.source_name as source_name,
exp_check.Retired as Retired,
exp_check.LKP_AGMT_ID as LKP_AGMT_ID,
exp_check.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM4,
exp_check.LKP_AGMT_STS as LKP_AGMT_STS,
exp_check.LKP_AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
NULL as AGMT_ID_Static,
exp_check.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_check.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_insert_update.source_record_id
FROM
exp_insert_update
LEFT JOIN exp_check ON exp_insert_update.source_record_id = exp_check.source_record_id
WHERE exp_check.out_ins_upd = ''U'' AND exp_check.LKP_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component upd_AGMT_STS_upd_upd1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_STS_upd_upd1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_STS_RETIRED.LKP_AGMT_ID as LKP_AGMT_ID,
rtr_AGMT_STS_RETIRED.LKP_AGMT_STS as LKP_AGMT_STS,
rtr_AGMT_STS_RETIRED.LKP_AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
rtr_AGMT_STS_RETIRED.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
SOURCE_RECORD_ID as source_record_id
FROM
rtr_AGMT_STS_RETIRED
);


-- Component upd_AGMT_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_STS_UPDATE.AGMT_ID as AGMT_ID,
rtr_AGMT_STS_UPDATE.AGMT_STS_CD as AGMT_STS_CD,
rtr_AGMT_STS_UPDATE.CreationTS as CreationTS,
rtr_AGMT_STS_UPDATE.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
rtr_AGMT_STS_UPDATE.PRCS_ID as PRCS_ID,
rtr_AGMT_STS_UPDATE.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
rtr_AGMT_STS_UPDATE.out_EDW_END_DTTM as out_EDW_END_DTTM3,
rtr_AGMT_STS_UPDATE.AGMT_STS_END_DTTM4 as AGMT_STS_END_DTTM43,
rtr_AGMT_STS_UPDATE.source_name as source_name3,
rtr_AGMT_STS_UPDATE.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD3,
rtr_AGMT_STS_UPDATE.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM3,
rtr_AGMT_STS_UPDATE.Retired as Retired3,
0 as UPDATE_STRATEGY_ACTION,
SOURCE_RECORD_ID as source_record_id
FROM
rtr_AGMT_STS_UPDATE
);


-- Component upd_AGMT_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_STS_INSERT.AGMT_ID as AGMT_ID,
rtr_AGMT_STS_INSERT.AGMT_STS_CD as AGMT_STS_CD,
rtr_AGMT_STS_INSERT.CreationTS as CreationTS,
rtr_AGMT_STS_INSERT.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
rtr_AGMT_STS_INSERT.PRCS_ID as PRCS_ID,
rtr_AGMT_STS_INSERT.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
rtr_AGMT_STS_INSERT.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_AGMT_STS_INSERT.AGMT_STS_END_DTTM4 as AGMT_STS_END_DTTM41,
rtr_AGMT_STS_INSERT.source_name as source_name1,
rtr_AGMT_STS_INSERT.AGMT_STS_RSN_CD as AGMT_STS_RSN_CD1,
rtr_AGMT_STS_INSERT.Retired as Retired1,
0 as UPDATE_STRATEGY_ACTION,
SOURCE_RECORD_ID as source_record_id
FROM
rtr_AGMT_STS_INSERT
);


-- Component upd_AGMT_STS_upd_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_STS_upd_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_STS_UPDATE.LKP_AGMT_ID as LKP_AGMT_ID,
rtr_AGMT_STS_UPDATE.LKP_AGMT_STS as LKP_AGMT_STS,
rtr_AGMT_STS_UPDATE.LKP_AGMT_STS_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM,
rtr_AGMT_STS_UPDATE.CreationTS as CreationTS,
rtr_AGMT_STS_UPDATE.PRCS_ID as PRCS_ID3,
rtr_AGMT_STS_UPDATE.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
rtr_AGMT_STS_UPDATE.LKP_EDW_STRT_DTTM as LKP_AGMT_STS_STRT_DTTM3,
rtr_AGMT_STS_UPDATE.source_name as source_name3,
1 as UPDATE_STRATEGY_ACTION,
SOURCE_RECORD_ID as source_record_id
FROM
rtr_AGMT_STS_UPDATE
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
upd_AGMT_upd_ins.AGMT_ID as AGMT_ID,
upd_AGMT_upd_ins.AGMT_STS_CD as AGMT_STS_CD,
upd_AGMT_upd_ins.CreationTS as CreationTS,
upd_AGMT_upd_ins.AGMT_STS_END_DTTM as AGMT_STS_END_DTTM,
upd_AGMT_upd_ins.PRCS_ID as PRCS_ID,
upd_AGMT_upd_ins.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
upd_AGMT_upd_ins.out_EDW_END_DTTM3 as out_EDW_END_DTTM3,
upd_AGMT_upd_ins.AGMT_STS_END_DTTM43 as AGMT_STS_END_DTTM43,
upd_AGMT_upd_ins.source_name3 as source_name3,
upd_AGMT_upd_ins.AGMT_STS_RSN_CD3 as AGMT_STS_RSN_CD3,
upd_AGMT_upd_ins.LKP_EDW_END_DTTM3 as LKP_EDW_END_DTTM3,
upd_AGMT_upd_ins.Retired3 as Retired3,
upd_AGMT_upd_ins.source_record_id
FROM
upd_AGMT_upd_ins
WHERE upd_AGMT_upd_ins.Retired3 = 0
);


-- Component exp_pass_to_target_upd_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_upd AS
(
SELECT
upd_AGMT_STS_upd_upd.LKP_AGMT_ID as LKP_AGMT_ID,
dateadd ( second , - 1 , upd_AGMT_STS_upd_upd.out_EDW_STRT_DTTM ) as out_CreationTS,
upd_AGMT_STS_upd_upd.LKP_AGMT_STS_STRT_DTTM3 as LKP_AGMT_STS_STRT_DTTM3,
upd_AGMT_STS_upd_upd.source_record_id
FROM
upd_AGMT_STS_upd_upd
);


-- Component exp_pass_to_target_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_retired AS
(
SELECT
upd_AGMT_STS_upd_upd1.LKP_AGMT_ID as LKP_AGMT_ID,
upd_AGMT_STS_upd_upd1.LKP_AGMT_STS as LKP_AGMT_STS,
upd_AGMT_STS_upd_upd1.out_EDW_STRT_DTTM4 as out_EDW_STRT_DTTM,
upd_AGMT_STS_upd_upd1.source_record_id
FROM
upd_AGMT_STS_upd_upd1
);


-- Component exp_pass_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_tgt AS
(
SELECT
upd_AGMT_ins.AGMT_ID as AGMT_ID,
upd_AGMT_ins.AGMT_STS_CD as AGMT_STS_CD,
upd_AGMT_ins.CreationTS as CreationTS,
upd_AGMT_ins.PRCS_ID as PRCS_ID,
upd_AGMT_ins.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
upd_AGMT_ins.AGMT_STS_END_DTTM41 as AGMT_STS_END_DTTM41,
upd_AGMT_ins.source_name1 as source_name1,
upd_AGMT_ins.AGMT_STS_RSN_CD1 as AGMT_STS_RSN_CD1,
CASE WHEN upd_AGMT_ins.Retired1 = 0 THEN upd_AGMT_ins.out_EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM11,
upd_AGMT_ins.source_record_id
FROM
upd_AGMT_ins
);


-- Component exp_pass_to_target_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_ins AS
(
SELECT
FILTRANS.AGMT_ID as AGMT_ID,
FILTRANS.AGMT_STS_CD as AGMT_STS_CD,
FILTRANS.CreationTS as CreationTS,
FILTRANS.PRCS_ID as PRCS_ID,
FILTRANS.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
FILTRANS.out_EDW_END_DTTM3 as out_EDW_END_DTTM3,
FILTRANS.AGMT_STS_END_DTTM43 as AGMT_STS_END_DTTM43,
FILTRANS.source_name3 as source_name3,
FILTRANS.AGMT_STS_RSN_CD3 as AGMT_STS_RSN_CD3,
FILTRANS.source_record_id
FROM
FILTRANS
);


-- Component tgt_AGMT_STS_ins_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_STS
(
AGMT_ID,
AGMT_STS_CD,
AGMT_STS_STRT_DTTM,
AGMT_STS_RSN_CD,
AGMT_STS_END_DTTM,
AGMT_STS_SRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_tgt.AGMT_ID as AGMT_ID,
exp_pass_tgt.AGMT_STS_CD as AGMT_STS_CD,
exp_pass_tgt.CreationTS as AGMT_STS_STRT_DTTM,
exp_pass_tgt.AGMT_STS_RSN_CD1 as AGMT_STS_RSN_CD,
exp_pass_tgt.AGMT_STS_END_DTTM41 as AGMT_STS_END_DTTM,
exp_pass_tgt.source_name1 as AGMT_STS_SRC_TYPE_CD,
exp_pass_tgt.PRCS_ID as PRCS_ID,
exp_pass_tgt.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_tgt.out_EDW_END_DTTM11 as EDW_END_DTTM
FROM
exp_pass_tgt;


-- Component tgt_AGMT_STS_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_STS
USING exp_pass_to_target_upd_upd ON (AGMT_STS.AGMT_ID = exp_pass_to_target_upd_upd.LKP_AGMT_ID AND AGMT_STS.EDW_STRT_DTTM = exp_pass_to_target_upd_upd.LKP_AGMT_STS_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_target_upd_upd.LKP_AGMT_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd_upd.LKP_AGMT_STS_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_upd.out_CreationTS;


-- Component tgt_AGMT_STS_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_STS
(
AGMT_ID,
AGMT_STS_CD,
AGMT_STS_STRT_DTTM,
AGMT_STS_RSN_CD,
AGMT_STS_END_DTTM,
AGMT_STS_SRC_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_upd_ins.AGMT_ID as AGMT_ID,
exp_pass_to_target_upd_ins.AGMT_STS_CD as AGMT_STS_CD,
exp_pass_to_target_upd_ins.CreationTS as AGMT_STS_STRT_DTTM,
exp_pass_to_target_upd_ins.AGMT_STS_RSN_CD3 as AGMT_STS_RSN_CD,
exp_pass_to_target_upd_ins.AGMT_STS_END_DTTM43 as AGMT_STS_END_DTTM,
exp_pass_to_target_upd_ins.source_name3 as AGMT_STS_SRC_TYPE_CD,
exp_pass_to_target_upd_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_upd_ins.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_upd_ins.out_EDW_END_DTTM3 as EDW_END_DTTM
FROM
exp_pass_to_target_upd_ins;


-- Component tgt_AGMT_STS_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_STS
USING exp_pass_to_target_retired ON (AGMT_STS.AGMT_ID = exp_pass_to_target_retired.LKP_AGMT_ID)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_target_retired.LKP_AGMT_ID,
AGMT_STS_CD = exp_pass_to_target_retired.LKP_AGMT_STS,
EDW_END_DTTM = exp_pass_to_target_retired.out_EDW_STRT_DTTM;


END; ';