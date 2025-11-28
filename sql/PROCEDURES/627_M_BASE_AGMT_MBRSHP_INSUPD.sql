-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_MBRSHP_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    run_id STRING;
    workflow_name STRING;
    session_name STRING;
    start_dttm TIMESTAMP;
    end_dttm TIMESTAMP;
    P_AGMT_TYPE_CD_POLICY_VERSION STRING;
    PRCS_ID STRING;
    v_start_time TIMESTAMP;
BEGIN
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_base_agmt_mbrshp_insupd'';
    start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
    P_AGMT_TYPE_CD_POLICY_VERSION := public.func_get_scoped_param(:run_id, ''P_AGMT_TYPE_CD_POLICY_VERSION'', :workflow_name, :worklet_name, :session_name);
    PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
    v_start_time := CURRENT_TIMESTAMP();

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''MBRSHP_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_clientidtype_alfa.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_AGMT_MBRSHP_X, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AGMT_MBRSHP_X AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ClientId_alfa,
$2 as TYPECODE,
$3 as PublicID,
$4 as periodstart,
$5 as AGMT_SRC_CD,
$6 as UPDATETIME,
$7 as Retired,
$8 as EditeffectiveDate,
$9 as updatetime2,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT distinct pc_effectivedatedfields.ClientId_alfa_stg as ClientId_alfa, pctl_clientidtype_alfa.TYPECODE_stg as TYPECODE,

		pc_policyperiod.PublicID_stg as PublicID,

		case 

			when cast(pc_effectivedatedfields.ContinuousServiceDate_alfa_stg as date) is not null then cast(pc_effectivedatedfields.ContinuousServiceDate_alfa_stg as date)

		else to_date(''01/01/1900'',''mm/dd/yyyy'') 

		end as periodstart,''SRC_SYS4'' as AGMT_SRC_CD,

		pc_effectivedatedfields.ContinuousServiceDate_alfa_stg as updatetime, pc_policyperiod.retired_stg as retired,pc_policyperiod.EditEffectiveDate_stg as EditEffectiveDate,		

		pc_policyperiod.UpdateTime_stg as updatetime2

from	DB_T_PROD_STAG.pc_effectivedatedfields 

inner join DB_T_PROD_STAG.pctl_clientidtype_alfa 

	on pc_effectivedatedfields.CLientIDType_alfa_stg=pctl_clientidtype_alfa.id_stg

inner join DB_T_PROD_STAG.pc_policyperiod 

	on pc_policyperiod.ID_stg=pc_effectivedatedfields.BranchID_stg  

inner join DB_T_PROD_STAG.pctl_policyperiodstatus 

	on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_job 

	on pc_job.ID_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job 

	on pctl_job.ID_stg=pc_job.Subtype_stg

where	pctl_policyperiodstatus.TYPECODE_stg=''Bound'' 

	and pc_effectivedatedfields.ClientId_alfa_stg is not null  

	and pc_effectivedatedfields.expirationdate_stg is null 

	and ((pc_effectivedatedfields.updatetime_stg>(:start_dttm) 

	AND pc_effectivedatedfields.updatetime_stg <= (:End_dttm))

	or (pc_policyperiod.updatetime_stg>(:start_dttm) 

	AND pc_policyperiod.updatetime_stg <= (:End_dttm)))

	AND pc_policyperiod.PolicyNumber_stg  is not  null
) SRC
)
);


-- Component exp_pass_through_mapping, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_mapping AS
(
SELECT
SQ_AGMT_MBRSHP_X.ClientId_alfa as ClientId_alfa,
SQ_AGMT_MBRSHP_X.TYPECODE as Typecode,
SQ_AGMT_MBRSHP_X.PublicID as PublicID,
SQ_AGMT_MBRSHP_X.periodstart as periodstart,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_AGMT_SRC_CD,
to_char ( SQ_AGMT_MBRSHP_X.UPDATETIME , ''YYYY/MM/DD'' ) as v_UPDATETIME,
to_date ( v_UPDATETIME , ''YYYY/MM/DD'' ) as o_UPDATETIME,
SQ_AGMT_MBRSHP_X.Retired as Retired,
SQ_AGMT_MBRSHP_X.EditeffectiveDate as EditeffectiveDate,
SQ_AGMT_MBRSHP_X.updatetime2 as updatetime2,
SQ_AGMT_MBRSHP_X.source_record_id,
row_number() over (partition by SQ_AGMT_MBRSHP_X.source_record_id order by SQ_AGMT_MBRSHP_X.source_record_id) as RNK
FROM
SQ_AGMT_MBRSHP_X
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_AGMT_MBRSHP_X.AGMT_SRC_CD
QUALIFY RNK = 1
);


-- Component exp_lookup, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_lookup AS
(
SELECT
LTRIM ( RTRIM ( exp_pass_through_mapping.ClientId_alfa ) ) as out_ClientId_alfa,
exp_pass_through_mapping.PublicID as PublicID,
LTRIM ( RTRIM ( LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) ) as v_typecode,
:P_AGMT_TYPE_CD_POLICY_VERSION as out_AGMT_TYPE_CD,
v_typecode as out_typecode,
exp_pass_through_mapping.out_AGMT_SRC_CD as out_AGMT_SRC_CD,
CASE WHEN exp_pass_through_mapping.o_UPDATETIME IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE exp_pass_through_mapping.o_UPDATETIME END as v_UPDATETIME,
CASE WHEN exp_pass_through_mapping.ClientId_alfa IS NULL THEN v_UPDATETIME ELSE TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) END as o_EndDate,
exp_pass_through_mapping.Retired as Retired,
exp_pass_through_mapping.source_record_id,
row_number() over (partition by exp_pass_through_mapping.source_record_id order by exp_pass_through_mapping.source_record_id) as RNK
FROM
exp_pass_through_mapping
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_through_mapping.Typecode
QUALIFY RNK = 1
);


-- Component LKP_AGMT_POL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_POL AS
(
SELECT
LKP.AGMT_ID,
exp_lookup.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_lookup.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_lookup
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM db_t_prod_core.AGMT QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_lookup.PublicID AND LKP.AGMT_TYPE_CD = exp_lookup.out_AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_MBRSHP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MBRSHP AS
(
SELECT
LKP.MBRSHP_ID,
exp_lookup.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_lookup.source_record_id ORDER BY LKP.MBRSHP_ID asc) RNK
FROM
exp_lookup
LEFT JOIN (
SELECT MBRSHP.MBRSHP_ID as MBRSHP_ID, LTRIM(RTRIM(MBRSHP.MBRSHP_NUM)) as MBRSHP_NUM, MBRSHP.MBRSHP_TYPE_CD as MBRSHP_TYPE_CD FROM db_t_prod_core.MBRSHP
) LKP ON LKP.MBRSHP_NUM = exp_lookup.out_ClientId_alfa AND LKP.MBRSHP_TYPE_CD = exp_lookup.out_typecode
QUALIFY RNK = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
LKP_MBRSHP.MBRSHP_ID as in_MBRSHP_ID,
LKP_AGMT_POL.AGMT_ID as in_AGMT_ID,
exp_pass_through_mapping.EditeffectiveDate as in_AGMT_MBRSHP_STRT_DT,
exp_lookup.o_EndDate as in_AGMT_MBRSHP_END_DT,
:PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_pass_through_mapping.updatetime2 as in_TRANS_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as in_TRANS_END_DTTM,
exp_lookup.Retired as Retired,
exp_pass_through_mapping.source_record_id
FROM
exp_pass_through_mapping
INNER JOIN exp_lookup ON exp_pass_through_mapping.source_record_id = exp_lookup.source_record_id
INNER JOIN LKP_AGMT_POL ON exp_lookup.source_record_id = LKP_AGMT_POL.source_record_id
INNER JOIN LKP_MBRSHP ON LKP_AGMT_POL.source_record_id = LKP_MBRSHP.source_record_id
);


-- Component LKP_AGMT_MBRSHP, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_MBRSHP AS
(
SELECT
LKP.MBRSHP_ID,
LKP.AGMT_ID,
LKP.AGMT_MBRSHP_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.MBRSHP_ID asc,LKP.AGMT_ID asc,LKP.AGMT_MBRSHP_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT AGMT_MBRSHP.AGMT_MBRSHP_END_DTTM as AGMT_MBRSHP_END_DTTM, AGMT_MBRSHP.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_MBRSHP.EDW_END_DTTM as EDW_END_DTTM, AGMT_MBRSHP.MBRSHP_ID as MBRSHP_ID, AGMT_MBRSHP.AGMT_ID as AGMT_ID 
FROM db_t_prod_core.AGMT_MBRSHP 
QUALIFY ROW_NUMBER() OVER(PARTITION BY MBRSHP_ID,AGMT_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.MBRSHP_ID = exp_SrcFields.in_MBRSHP_ID AND LKP.AGMT_ID = exp_SrcFields.in_AGMT_ID
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_MBRSHP_ID as in_MBRSHP_ID,
exp_SrcFields.in_AGMT_ID as in_AGMT_ID,
exp_SrcFields.in_AGMT_MBRSHP_STRT_DT as in_AGMT_MBRSHP_STRT_DT,
exp_SrcFields.in_AGMT_MBRSHP_END_DT as in_AGMT_MBRSHP_END_DT,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_SrcFields.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_SrcFields.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
LKP_AGMT_MBRSHP.MBRSHP_ID as lkp_MBRSHP_ID,
LKP_AGMT_MBRSHP.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_MBRSHP.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
CASE WHEN LKP_AGMT_MBRSHP.AGMT_MBRSHP_END_DTTM IS NULL THEN ''I'' ELSE CASE WHEN exp_SrcFields.in_AGMT_MBRSHP_END_DT = LKP_AGMT_MBRSHP.AGMT_MBRSHP_END_DTTM THEN ''R'' ELSE ''U'' END END as o_CDC_Check,
exp_SrcFields.Retired as Retired,
LKP_AGMT_MBRSHP.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_AGMT_MBRSHP ON exp_SrcFields.source_record_id = LKP_AGMT_MBRSHP.source_record_id
);


-- Component RTR_AGMT_MBRSHP_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table RTR_AGMT_MBRSHP_INSERT as
SELECT
exp_CDC_Check.in_MBRSHP_ID as in_MBRSHP_ID,
exp_CDC_Check.in_AGMT_ID as in_AGMT_ID,
exp_CDC_Check.in_AGMT_MBRSHP_STRT_DT as in_AGMT_MBRSHP_STRT_DT,
exp_CDC_Check.in_AGMT_MBRSHP_END_DT as in_AGMT_MBRSHP_END_DT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_CDC_Check.lkp_MBRSHP_ID as lkp_MBRSHP_ID,
exp_CDC_Check.lkp_AGMT_ID as lkp_AGMT_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_CDC_Check = ''I'' and exp_CDC_Check.in_AGMT_ID IS NOT NULL OR ( exp_CDC_Check.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_CDC_Check.Retired = 0 ) /*- - exp_CDC_Check.o_CDC_Check = ''I'' and exp_CDC_Check.in_AGMT_ID IS NOT NULL and exp_CDC_Check.in_MBRSHP_ID IS NOT NULL*/
;


-- Component RTR_AGMT_MBRSHP_RETIRED, Type ROUTER Output Group RETIRED
create or replace temporary table RTR_AGMT_MBRSHP_RETIRED as
SELECT
exp_CDC_Check.in_MBRSHP_ID as in_MBRSHP_ID,
exp_CDC_Check.in_AGMT_ID as in_AGMT_ID,
exp_CDC_Check.in_AGMT_MBRSHP_STRT_DT as in_AGMT_MBRSHP_STRT_DT,
exp_CDC_Check.in_AGMT_MBRSHP_END_DT as in_AGMT_MBRSHP_END_DT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_CDC_Check.lkp_MBRSHP_ID as lkp_MBRSHP_ID,
exp_CDC_Check.lkp_AGMT_ID as lkp_AGMT_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_CDC_Check = ''R'' and exp_CDC_Check.Retired != 0 and exp_CDC_Check.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );


-- Component RTR_AGMT_MBRSHP_UPDATE, Type ROUTER Output Group UPDATE
create or replace temporary table RTR_AGMT_MBRSHP_UPDATE as
SELECT
exp_CDC_Check.in_MBRSHP_ID as in_MBRSHP_ID,
exp_CDC_Check.in_AGMT_ID as in_AGMT_ID,
exp_CDC_Check.in_AGMT_MBRSHP_STRT_DT as in_AGMT_MBRSHP_STRT_DT,
exp_CDC_Check.in_AGMT_MBRSHP_END_DT as in_AGMT_MBRSHP_END_DT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_CDC_Check.lkp_MBRSHP_ID as lkp_MBRSHP_ID,
exp_CDC_Check.lkp_AGMT_ID as lkp_AGMT_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
NULL as out_trans_end_dttm,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_CDC_Check = ''U'' AND exp_CDC_Check.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) /*- - exp_CDC_Check.o_CDC_Check = ''U'' and exp_CDC_Check.in_AGMT_ID IS NOT NULL and exp_CDC_Check.in_MBRSHP_ID IS NOT NULL*/
;


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
RTR_AGMT_MBRSHP_UPDATE.in_MBRSHP_ID as in_MBRSHP_ID3,
RTR_AGMT_MBRSHP_UPDATE.in_AGMT_ID as in_AGMT_ID3,
RTR_AGMT_MBRSHP_UPDATE.in_AGMT_MBRSHP_STRT_DT as in_AGMT_MBRSHP_STRT_DT3,
RTR_AGMT_MBRSHP_UPDATE.in_AGMT_MBRSHP_END_DT as in_AGMT_MBRSHP_END_DT3,
RTR_AGMT_MBRSHP_UPDATE.in_PRCS_ID as in_PRCS_ID3,
RTR_AGMT_MBRSHP_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
RTR_AGMT_MBRSHP_UPDATE.in_EDW_END_DTTM as in_EDW_END_DTTM3,
RTR_AGMT_MBRSHP_UPDATE.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM3,
RTR_AGMT_MBRSHP_UPDATE.in_TRANS_END_DTTM as in_TRANS_END_DTTM3,
RTR_AGMT_MBRSHP_UPDATE.Retired as Retired3,
RTR_AGMT_MBRSHP_UPDATE.source_record_id
FROM
RTR_AGMT_MBRSHP_UPDATE
WHERE RTR_AGMT_MBRSHP_UPDATE.Retired = 0
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
RTR_AGMT_MBRSHP_INSERT.in_MBRSHP_ID as in_MBRSHP_ID1,
RTR_AGMT_MBRSHP_INSERT.in_AGMT_ID as in_AGMT_ID1,
RTR_AGMT_MBRSHP_INSERT.in_AGMT_MBRSHP_STRT_DT as in_AGMT_MBRSHP_STRT_DT1,
RTR_AGMT_MBRSHP_INSERT.in_AGMT_MBRSHP_END_DT as in_AGMT_MBRSHP_END_DT1,
RTR_AGMT_MBRSHP_INSERT.in_PRCS_ID as in_PRCS_ID1,
RTR_AGMT_MBRSHP_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
RTR_AGMT_MBRSHP_INSERT.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM1,
CASE WHEN RTR_AGMT_MBRSHP_INSERT.Retired <> 0 THEN RTR_AGMT_MBRSHP_INSERT.in_TRANS_STRT_DTTM ELSE RTR_AGMT_MBRSHP_INSERT.in_TRANS_END_DTTM END as in_TRANS_END_DTTM1,
CASE WHEN RTR_AGMT_MBRSHP_INSERT.Retired <> 0 THEN RTR_AGMT_MBRSHP_INSERT.in_EDW_STRT_DTTM ELSE RTR_AGMT_MBRSHP_INSERT.in_EDW_END_DTTM END as out_EDW_END_DTTM1,
RTR_AGMT_MBRSHP_INSERT.source_record_id
FROM
RTR_AGMT_MBRSHP_INSERT
);


-- Component AGMT_MBRSHP_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_MBRSHP
(
MBRSHP_ID,
AGMT_ID,
AGMT_MBRSHP_STRT_DTTM,
AGMT_MBRSHP_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
EXPTRANS.in_MBRSHP_ID1 as MBRSHP_ID,
EXPTRANS.in_AGMT_ID1 as AGMT_ID,
EXPTRANS.in_AGMT_MBRSHP_STRT_DT1 as AGMT_MBRSHP_STRT_DTTM,
EXPTRANS.in_AGMT_MBRSHP_END_DT1 as AGMT_MBRSHP_END_DTTM,
EXPTRANS.in_PRCS_ID1 as PRCS_ID,
EXPTRANS.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
EXPTRANS.out_EDW_END_DTTM1 as EDW_END_DTTM,
EXPTRANS.in_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
EXPTRANS.in_TRANS_END_DTTM1 as TRANS_END_DTTM
FROM
EXPTRANS;


-- Component upd_agmt_mbrshp, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_mbrshp AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_AGMT_MBRSHP_UPDATE.lkp_MBRSHP_ID as lkp_MBRSHP_ID3,
RTR_AGMT_MBRSHP_UPDATE.lkp_AGMT_ID as lkp_AGMT_ID3,
RTR_AGMT_MBRSHP_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
RTR_AGMT_MBRSHP_UPDATE.out_trans_end_dttm as out_trans_end_dttm3,
RTR_AGMT_MBRSHP_UPDATE.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM3,
RTR_AGMT_MBRSHP_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
RTR_AGMT_MBRSHP_UPDATE.source_record_id
FROM
RTR_AGMT_MBRSHP_UPDATE
);


-- Component upd_agmt_mbrshp_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_mbrshp_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_AGMT_MBRSHP_RETIRED.lkp_MBRSHP_ID as lkp_MBRSHP_ID3,
RTR_AGMT_MBRSHP_RETIRED.lkp_AGMT_ID as lkp_AGMT_ID3,
RTR_AGMT_MBRSHP_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
RTR_AGMT_MBRSHP_RETIRED.out_trans_end_dttm as out_trans_end_dttm4,
RTR_AGMT_MBRSHP_RETIRED.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
RTR_AGMT_MBRSHP_RETIRED.source_record_id
FROM
RTR_AGMT_MBRSHP_RETIRED
);


-- Component AGMT_MBRSHP_INS_UPD, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_MBRSHP
(
MBRSHP_ID,
AGMT_ID,
AGMT_MBRSHP_STRT_DTTM,
AGMT_MBRSHP_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
FILTRANS.in_MBRSHP_ID3 as MBRSHP_ID,
FILTRANS.in_AGMT_ID3 as AGMT_ID,
FILTRANS.in_AGMT_MBRSHP_STRT_DT3 as AGMT_MBRSHP_STRT_DTTM,
FILTRANS.in_AGMT_MBRSHP_END_DT3 as AGMT_MBRSHP_END_DTTM,
FILTRANS.in_PRCS_ID3 as PRCS_ID,
FILTRANS.in_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
FILTRANS.in_EDW_END_DTTM3 as EDW_END_DTTM,
FILTRANS.in_TRANS_STRT_DTTM3 as TRANS_STRT_DTTM,
FILTRANS.in_TRANS_END_DTTM3 as TRANS_END_DTTM
FROM
FILTRANS;


-- Component exp_pass_through_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_tgt_ins AS
(
SELECT
upd_agmt_mbrshp.lkp_MBRSHP_ID3 as lkp_MBRSHP_ID3,
upd_agmt_mbrshp.lkp_AGMT_ID3 as lkp_AGMT_ID3,
upd_agmt_mbrshp.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
dateadd ( second, -1, upd_agmt_mbrshp.in_EDW_STRT_DTTM3 ) as o_DateExpiry,
dateadd ( second, -1, upd_agmt_mbrshp.in_TRANS_STRT_DTTM3 ) as out_trans_end_dttm3,
upd_agmt_mbrshp.source_record_id
FROM
upd_agmt_mbrshp
);


-- Component AGMT_MBRSHP_UPD, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_MBRSHP
USING exp_pass_through_tgt_ins ON (AGMT_MBRSHP.MBRSHP_ID = exp_pass_through_tgt_ins.lkp_MBRSHP_ID3 AND AGMT_MBRSHP.AGMT_ID = exp_pass_through_tgt_ins.lkp_AGMT_ID3 AND AGMT_MBRSHP.EDW_STRT_DTTM = exp_pass_through_tgt_ins.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
MBRSHP_ID = exp_pass_through_tgt_ins.lkp_MBRSHP_ID3,
AGMT_ID = exp_pass_through_tgt_ins.lkp_AGMT_ID3,
EDW_STRT_DTTM = exp_pass_through_tgt_ins.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_through_tgt_ins.o_DateExpiry,
TRANS_END_DTTM = exp_pass_through_tgt_ins.out_trans_end_dttm3;


-- Component exp_pass_through_tgt_ins1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_tgt_ins1 AS
(
SELECT
upd_agmt_mbrshp_retired.lkp_MBRSHP_ID3 as lkp_MBRSHP_ID3,
upd_agmt_mbrshp_retired.lkp_AGMT_ID3 as lkp_AGMT_ID3,
upd_agmt_mbrshp_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as o_DateExpiry,
upd_agmt_mbrshp_retired.in_TRANS_STRT_DTTM4 as out_trans_end_dttm4,
upd_agmt_mbrshp_retired.source_record_id
FROM
upd_agmt_mbrshp_retired
);


-- Component AGMT_MBRSHP_UPD_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_MBRSHP
USING exp_pass_through_tgt_ins1 ON (AGMT_MBRSHP.MBRSHP_ID = exp_pass_through_tgt_ins1.lkp_MBRSHP_ID3 AND AGMT_MBRSHP.AGMT_ID = exp_pass_through_tgt_ins1.lkp_AGMT_ID3 AND AGMT_MBRSHP.EDW_STRT_DTTM = exp_pass_through_tgt_ins1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
MBRSHP_ID = exp_pass_through_tgt_ins1.lkp_MBRSHP_ID3,
AGMT_ID = exp_pass_through_tgt_ins1.lkp_AGMT_ID3,
EDW_STRT_DTTM = exp_pass_through_tgt_ins1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_through_tgt_ins1.o_DateExpiry,
TRANS_END_DTTM = exp_pass_through_tgt_ins1.out_trans_end_dttm4;


INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_agmt_mbrshp_insupd'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_agmt_mbrshp_insupd'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );


END; ';