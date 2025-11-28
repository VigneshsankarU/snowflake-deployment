-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_CHNL_TYPE_INS("WORKLET_NAME" VARCHAR)
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
    p_AGMT_TYPE_CD_POLICY_VERSION STRING;
    PRCS_ID STRING;
    v_start_time TIMESTAMP;
BEGIN
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_base_agmt_chnl_type_ins'';
    start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
    p_AGMT_TYPE_CD_POLICY_VERSION := public.func_get_scoped_param(:run_id, ''p_AGMT_TYPE_CD_POLICY_VERSION'', :workflow_name, :worklet_name, :session_name);
    PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
    v_start_time := CURRENT_TIMESTAMP();

-- Component LKP_TERADATA_ETL_REF_XLAT_CHNL_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CHNL_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CHNL_TYPE'' 

             --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_sourceofbusiness_alfa.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_job, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_job AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicId,
$2 as TYPECODE_GROUPTYPE,
$3 as editeffectivedate,
$4 as end_dt,
$5 as UpdateTime,
$6 as Retired,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	DISTINCT 

publicid, 

TYPECODE_GROUPTYPE,

EditEffectivedate,

cast(NULL AS timestamp) as END_DT,

pc_updatetime,

Policy_Retired   

FROM	

(

Select 

pc_job.Publicid_stg  as publicid,

pctl_grouptype.typecode_stg as TYPECODE_GROUPTYPE,

pc_policyperiod.EditEffectivedate_stg as EditEffectivedate,

cast(NULL AS timestamp) as END_DT,

pc_policyperiod.UpdateTime_stg as  pc_updatetime,

pc_policyperiod.Retired_stg as Policy_Retired 

from

DB_T_PROD_STAG.pc_job

LEFT OUTER JOIN    DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg

LEFT JOIN    DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

left join    DB_T_PROD_STAG.pc_policyterm on pc_policyterm.id_stg = pc_policyperiod.policytermid_stg

LEFT OUTER JOIN    DB_T_PROD_STAG.pc_user ON pc_job.CreateUserID_stg = pc_user.ID_stg

LEFT OUTER JOIN    DB_T_PROD_STAG.pc_jobpolicyperiod on pc_job.id_stg = pc_jobpolicyperiod.OwnerID_stg

LEFT OUTER JOIN    DB_T_PROD_STAG.pc_policyperiod as PCP1 on pc_jobpolicyperiod.ForeignEntityID_stg = PCP1.id_stg

LEFT OUTER JOIN    DB_T_PROD_STAG.pc_effectivedatedfields on pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg

left outer join    DB_T_PROD_STAG.pcx_holineratingfactor_alfa on pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg

left outer join    DB_T_PROD_STAG.pctl_billingperiodicity on pcx_holineratingfactor_alfa.AutoLatePayBillingPeriodicity_stg=pctl_billingperiodicity.ID_stg

left join    DB_T_PROD_STAG.pc_groupuser on pc_user.id_stg = pc_groupuser.UserID_stg 

left join    DB_T_PROD_STAG.pc_group on pc_groupuser.GroupID_stg = pc_group.id_stg left join    DB_T_PROD_STAG.pctl_grouptype on pc_group.GroupType_stg = pctl_grouptype.ID_stg

left JOIN    DB_T_PROD_STAG.pctl_cancellationsource ON pc_job.source_stg = pctl_cancellationsource.id_stg

left join ( SELECT distinct JobID_stg from    DB_T_PROD_STAG.pc_policyperiod  where QuoteMaturityLevel_stg in (2,3) ) vj ON pc_job.id_stg=vj.JobID_stg

WHERE  

pc_policyperiod.UpdateTime_stg  > (:start_dttm) and pc_policyperiod.UpdateTime_stg  <= (:end_dttm) and 

 pctl_policyperiodstatus.typecode_stg <>''Temporary'' and   pctl_policyperiodstatus.typecode_stg  =''Bound'' and pc_effectivedatedfields.expirationdate_stg  is null

and pcx_holineratingfactor_alfa.ExpirationDate_stg  is  null )a
) SRC
)
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
SQ_pc_job.PublicId as PublicId,
SQ_pc_job.UpdateTime as UpdateTime,
:P_AGMT_TYPE_CD_POLICY_VERSION as out_AGMT_TYPE_CD,
SQ_pc_job.Retired as Retired,
CASE WHEN SQ_pc_job.end_dt IS NULL THEN TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE SQ_pc_job.end_dt END as o_end_dt,
SQ_pc_job.editeffectivedate as editeffectivedate1,
SQ_pc_job.TYPECODE_GROUPTYPE as TYPECODE_GROUPTYPE,
SQ_pc_job.source_record_id
FROM
SQ_pc_job
);


-- Component LKP_AGMT_PPV, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_PPV AS
(
SELECT
LKP.AGMT_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM db_t_prod_core.AGMT QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_data_transformation.PublicId AND LKP.AGMT_TYPE_CD = exp_data_transformation.out_AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
LKP_AGMT_PPV.AGMT_ID as AGMT_ID,
exp_data_transformation.UpdateTime as UpdateTime,
exp_data_transformation.Retired as Retired,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CHNL_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CHNL_TYPE */ END as out_lkp_CHNL_TYPE,
''EXBUSCNSDR'' as out_AGMT_CHNL_ROLE_TYPE,
exp_data_transformation.o_end_dt as end_dt,
exp_data_transformation.editeffectivedate1 as editeffectivedate1,
exp_data_transformation.source_record_id,
row_number() over (partition by exp_data_transformation.source_record_id order by exp_data_transformation.source_record_id) as RNK
FROM
exp_data_transformation
INNER JOIN LKP_AGMT_PPV ON exp_data_transformation.source_record_id = LKP_AGMT_PPV.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CHNL_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_data_transformation.TYPECODE_GROUPTYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CHNL_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_data_transformation.TYPECODE_GROUPTYPE
QUALIFY RNK = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_pass_to_tgt.AGMT_ID as in_AGMT_ID,
exp_pass_to_tgt.UpdateTime as UpdateTime,
exp_pass_to_tgt.Retired as Retired,
exp_pass_to_tgt.out_lkp_CHNL_TYPE as in_CHNL_TYPE_CD,
exp_pass_to_tgt.out_AGMT_CHNL_ROLE_TYPE as in_AGMT_CHNL_ROLE_TYPE_CD,
exp_pass_to_tgt.editeffectivedate1 as in_AGMT_CHNL_TYPE_STRT_DT,
exp_pass_to_tgt.end_dt as in_AGMT_CHNL_TYPE_END_DT,
:PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_pass_to_tgt.source_record_id
FROM
exp_pass_to_tgt
);


-- Component LKP_AGMT_CHNL_TYPE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_CHNL_TYPE AS
(
SELECT
LKP.AGMT_ID,
LKP.CHNL_TYPE_CD,
LKP.AGMT_CHNL_ROLE_TYPE_CD,
LKP.AGMT_CHNL_TYPE_STRT_DTTM,
LKP.AGMT_CHNL_TYPE_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_SrcFields.in_AGMT_ID as in_AGMT_ID,
exp_SrcFields.in_CHNL_TYPE_CD as in_CHNL_TYPE_CD,
exp_SrcFields.in_AGMT_CHNL_ROLE_TYPE_CD as in_AGMT_CHNL_ROLE_TYPE_CD,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.CHNL_TYPE_CD asc,LKP.AGMT_CHNL_ROLE_TYPE_CD asc,LKP.AGMT_CHNL_TYPE_STRT_DTTM asc,LKP.AGMT_CHNL_TYPE_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT AGMT_CHNL_TYPE.AGMT_CHNL_TYPE_STRT_DTTM as AGMT_CHNL_TYPE_STRT_DTTM, AGMT_CHNL_TYPE.AGMT_CHNL_TYPE_END_DTTM as AGMT_CHNL_TYPE_END_DTTM, AGMT_CHNL_TYPE.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_CHNL_TYPE.EDW_END_DTTM as EDW_END_DTTM, AGMT_CHNL_TYPE.AGMT_ID as AGMT_ID, AGMT_CHNL_TYPE.CHNL_TYPE_CD as CHNL_TYPE_CD, AGMT_CHNL_TYPE.AGMT_CHNL_ROLE_TYPE_CD as AGMT_CHNL_ROLE_TYPE_CD 
FROM db_t_prod_core.AGMT_CHNL_TYPE 
/* where EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT_CHNL_TYPE.AGMT_ID, AGMT_CHNL_TYPE.CHNL_TYPE_CD, AGMT_CHNL_TYPE.AGMT_CHNL_ROLE_TYPE_CD ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.AGMT_ID = exp_SrcFields.in_AGMT_ID AND LKP.CHNL_TYPE_CD = exp_SrcFields.in_CHNL_TYPE_CD AND LKP.AGMT_CHNL_ROLE_TYPE_CD = exp_SrcFields.in_AGMT_CHNL_ROLE_TYPE_CD
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_AGMT_ID as in_AGMT_ID,
exp_SrcFields.Retired as Retired,
exp_SrcFields.in_CHNL_TYPE_CD as in_CHNL_TYPE_CD,
exp_SrcFields.in_AGMT_CHNL_ROLE_TYPE_CD as in_AGMT_CHNL_ROLE_TYPE_CD,
exp_SrcFields.in_AGMT_CHNL_TYPE_STRT_DT as in_AGMT_CHNL_TYPE_STRT_DT,
exp_SrcFields.in_AGMT_CHNL_TYPE_END_DT as in_AGMT_CHNL_TYPE_END_DT,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_SrcFields.UpdateTime as UpdateTime,
NULL as in_TRANS_END_DTTM,
LKP_AGMT_CHNL_TYPE.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_CHNL_TYPE.CHNL_TYPE_CD as lkp_CHNL_TYPE_CD,
LKP_AGMT_CHNL_TYPE.AGMT_CHNL_ROLE_TYPE_CD as lkp_AGMT_CHNL_ROLE_TYPE_CD,
LKP_AGMT_CHNL_TYPE.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_AGMT_CHNL_TYPE.EDW_END_DTTM as lkp_EDW_END_DTTM,
MD5 ( to_char ( exp_SrcFields.in_AGMT_CHNL_TYPE_STRT_DT ) || to_char ( exp_SrcFields.in_AGMT_CHNL_TYPE_END_DT ) ) as v_SRC_MD5,
MD5 ( to_char ( LKP_AGMT_CHNL_TYPE.AGMT_CHNL_TYPE_STRT_DTTM ) || to_char ( LKP_AGMT_CHNL_TYPE.AGMT_CHNL_TYPE_END_DTTM ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 != v_TGT_MD5 THEN ''U'' ELSE ''R'' END END as o_CDC_Check,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_AGMT_CHNL_TYPE ON exp_SrcFields.source_record_id = LKP_AGMT_CHNL_TYPE.source_record_id
);


-- Component rtr_AGMT_CHNL_TYPE_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_AGMT_CHNL_TYPE_Insert AS
(
SELECT
exp_CDC_Check.in_AGMT_ID as in_AGMT_ID,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.in_CHNL_TYPE_CD as in_CHNL_TYPE_CD,
exp_CDC_Check.in_AGMT_CHNL_ROLE_TYPE_CD as in_AGMT_CHNL_ROLE_TYPE_CD,
exp_CDC_Check.in_AGMT_CHNL_TYPE_STRT_DT as in_AGMT_CHNL_TYPE_STRT_DT,
exp_CDC_Check.in_AGMT_CHNL_TYPE_END_DT as in_AGMT_CHNL_TYPE_END_DT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.UpdateTime as UpdateTime,
exp_CDC_Check.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_CDC_Check.lkp_AGMT_ID as lkp_AGMT_ID,
exp_CDC_Check.lkp_CHNL_TYPE_CD as lkp_CHNL_TYPE_CD,
exp_CDC_Check.lkp_AGMT_CHNL_ROLE_TYPE_CD as lkp_AGMT_CHNL_ROLE_TYPE_CD,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
NULL as out_trans_end_dttm,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_CDC_Check = ''I'' and exp_CDC_Check.in_AGMT_ID IS NOT NULL 
-- and exp_CDC_Check.in_CHNL_TYPE_CD IS NOT NULL ) OR ( exp_CDC_Check.Retired = 0 AND exp_CDC_Check.lkp_EDW_END_DTTM != to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AND exp_CDC_Check.in_AGMT_ID IS NOT NULL and exp_CDC_Check.in_CHNL_TYPE_CD IS NOT NULL )
);


-- Component rtr_AGMT_CHNL_TYPE_Retired, Type ROUTER
-- -Output Group Retired
CREATE
OR REPLACE TEMPORARY TABLE rtr_AGMT_CHNL_TYPE_Retired AS
SELECT
  in_AGMT_ID,
  Retired,
  in_CHNL_TYPE_CD,
  in_AGMT_CHNL_ROLE_TYPE_CD,
  in_AGMT_CHNL_TYPE_STRT_DT,
  in_AGMT_CHNL_TYPE_END_DT,
  in_PRCS_ID,
  in_EDW_STRT_DTTM,
  in_EDW_END_DTTM,
  UpdateTime,
  in_TRANS_END_DTTM,
  lkp_AGMT_ID,
  lkp_CHNL_TYPE_CD,
  lkp_AGMT_CHNL_ROLE_TYPE_CD,
  lkp_EDW_STRT_DTTM,
  lkp_EDW_END_DTTM,
  o_CDC_Check,
  NULL AS out_trans_end_dttm,
  source_record_id
FROM
  exp_CDC_Check
WHERE
  o_CDC_Check = ''R''
  AND Retired <> 0
  AND lkp_EDW_END_DTTM = TO_TIMESTAMP (
  ''9999-12-31 23:59:59.999999'',
  ''yyyy-mm-DD hh24:mi:ss.ff6''
);

-- Component rtr_AGMT_CHNL_TYPE_Update, Type ROUTER Output Group Update
CREATE
OR REPLACE TEMPORARY TABLE rtr_AGMT_CHNL_TYPE_Update as (
SELECT
exp_CDC_Check.in_AGMT_ID as in_AGMT_ID,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.in_CHNL_TYPE_CD as in_CHNL_TYPE_CD,
exp_CDC_Check.in_AGMT_CHNL_ROLE_TYPE_CD as in_AGMT_CHNL_ROLE_TYPE_CD,
exp_CDC_Check.in_AGMT_CHNL_TYPE_STRT_DT as in_AGMT_CHNL_TYPE_STRT_DT,
exp_CDC_Check.in_AGMT_CHNL_TYPE_END_DT as in_AGMT_CHNL_TYPE_END_DT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.UpdateTime as UpdateTime,
exp_CDC_Check.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_CDC_Check.lkp_AGMT_ID as lkp_AGMT_ID,
exp_CDC_Check.lkp_CHNL_TYPE_CD as lkp_CHNL_TYPE_CD,
exp_CDC_Check.lkp_AGMT_CHNL_ROLE_TYPE_CD as lkp_AGMT_CHNL_ROLE_TYPE_CD,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
NULL as out_trans_end_dttm,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_CDC_Check = ''U'' 
-- and exp_CDC_Check.in_AGMT_ID IS NOT NULL 
-- and exp_CDC_Check.in_CHNL_TYPE_CD IS NOT NULL AND exp_CDC_Check.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ))
);


-- Component fil_insupd, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_insupd AS
(
SELECT
rtr_AGMT_CHNL_TYPE_Update.in_AGMT_ID as in_AGMT_ID3,
rtr_AGMT_CHNL_TYPE_Update.Retired as Retired,
rtr_AGMT_CHNL_TYPE_Update.in_CHNL_TYPE_CD as in_CHNL_TYPE_CD3,
rtr_AGMT_CHNL_TYPE_Update.in_AGMT_CHNL_ROLE_TYPE_CD as in_AGMT_CHNL_ROLE_TYPE_CD3,
rtr_AGMT_CHNL_TYPE_Update.in_AGMT_CHNL_TYPE_STRT_DT as in_AGMT_CHNL_TYPE_STRT_DT3,
rtr_AGMT_CHNL_TYPE_Update.in_AGMT_CHNL_TYPE_END_DT as in_AGMT_CHNL_TYPE_END_DT3,
rtr_AGMT_CHNL_TYPE_Update.in_PRCS_ID as in_PRCS_ID3,
rtr_AGMT_CHNL_TYPE_Update.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_AGMT_CHNL_TYPE_Update.in_EDW_END_DTTM as in_EDW_END_DTTM3,
rtr_AGMT_CHNL_TYPE_Update.UpdateTime as UpdateTime,
rtr_AGMT_CHNL_TYPE_Update.in_TRANS_END_DTTM as in_TRANS_END_DTTM3,
rtr_AGMT_CHNL_TYPE_Update.source_record_id
FROM
rtr_AGMT_CHNL_TYPE_Update
WHERE rtr_AGMT_CHNL_TYPE_Update.Retired = 0
);


-- Component upd_AGMT_CHNL_TYPE_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_CHNL_TYPE_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_CHNL_TYPE_Retired.lkp_AGMT_ID as lkp_AGMT_ID4,
rtr_AGMT_CHNL_TYPE_Retired.lkp_CHNL_TYPE_CD as lkp_CHNL_TYPE_CD4,
rtr_AGMT_CHNL_TYPE_Retired.lkp_AGMT_CHNL_ROLE_TYPE_CD as lkp_AGMT_CHNL_ROLE_TYPE_CD4,
rtr_AGMT_CHNL_TYPE_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM4,
rtr_AGMT_CHNL_TYPE_Retired.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM4,
rtr_AGMT_CHNL_TYPE_Retired.UpdateTime as UpdateTime,
1 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_AGMT_CHNL_TYPE_Retired
);


-- Component upd_AGMT_CHNL_TYPE_Insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_CHNL_TYPE_Insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_CHNL_TYPE_Insert.in_AGMT_ID as in_AGMT_ID1,
rtr_AGMT_CHNL_TYPE_Insert.Retired as Retired,
rtr_AGMT_CHNL_TYPE_Insert.in_CHNL_TYPE_CD as in_CHNL_TYPE_CD1,
rtr_AGMT_CHNL_TYPE_Insert.in_AGMT_CHNL_ROLE_TYPE_CD as in_AGMT_CHNL_ROLE_TYPE_CD1,
rtr_AGMT_CHNL_TYPE_Insert.in_AGMT_CHNL_TYPE_STRT_DT as in_AGMT_CHNL_TYPE_STRT_DT1,
rtr_AGMT_CHNL_TYPE_Insert.in_AGMT_CHNL_TYPE_END_DT as in_AGMT_CHNL_TYPE_END_DT1,
rtr_AGMT_CHNL_TYPE_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_AGMT_CHNL_TYPE_Insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_AGMT_CHNL_TYPE_Insert.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_AGMT_CHNL_TYPE_Insert.UpdateTime as UpdateTime,
rtr_AGMT_CHNL_TYPE_Insert.in_TRANS_END_DTTM as in_TRANS_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_AGMT_CHNL_TYPE_Insert
);


-- Component exp_CHNL_TYPE_Insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CHNL_TYPE_Insert AS
(
SELECT
upd_AGMT_CHNL_TYPE_Insert.in_AGMT_ID1 as in_AGMT_ID1,
upd_AGMT_CHNL_TYPE_Insert.in_CHNL_TYPE_CD1 as in_CHNL_TYPE_CD1,
upd_AGMT_CHNL_TYPE_Insert.in_AGMT_CHNL_ROLE_TYPE_CD1 as in_AGMT_CHNL_ROLE_TYPE_CD1,
upd_AGMT_CHNL_TYPE_Insert.in_AGMT_CHNL_TYPE_STRT_DT1 as in_AGMT_CHNL_TYPE_STRT_DT1,
upd_AGMT_CHNL_TYPE_Insert.in_AGMT_CHNL_TYPE_END_DT1 as in_AGMT_CHNL_TYPE_END_DT1,
upd_AGMT_CHNL_TYPE_Insert.in_PRCS_ID1 as in_PRCS_ID1,
upd_AGMT_CHNL_TYPE_Insert.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_AGMT_CHNL_TYPE_Insert.UpdateTime as UpdateTime,
CASE WHEN upd_AGMT_CHNL_TYPE_Insert.Retired != 0 THEN upd_AGMT_CHNL_TYPE_Insert.UpdateTime ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as in_TRANS_END_DTTM1,
CASE WHEN upd_AGMT_CHNL_TYPE_Insert.Retired = 0 THEN upd_AGMT_CHNL_TYPE_Insert.in_EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM1,
upd_AGMT_CHNL_TYPE_Insert.source_record_id
FROM
upd_AGMT_CHNL_TYPE_Insert
);


-- Component upd_AGMT_CHNL_TYPE_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_CHNL_TYPE_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_AGMT_CHNL_TYPE_Update.lkp_AGMT_ID as lkp_AGMT_ID3,
rtr_AGMT_CHNL_TYPE_Update.lkp_CHNL_TYPE_CD as lkp_CHNL_TYPE_CD3,
rtr_AGMT_CHNL_TYPE_Update.lkp_AGMT_CHNL_ROLE_TYPE_CD as lkp_AGMT_CHNL_ROLE_TYPE_CD3,
rtr_AGMT_CHNL_TYPE_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_AGMT_CHNL_TYPE_Update.UpdateTime as UpdateTime,
rtr_AGMT_CHNL_TYPE_Update.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_AGMT_CHNL_TYPE_Update
);


-- Component exp_DateExpiry_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_DateExpiry_retired AS
(
SELECT
upd_AGMT_CHNL_TYPE_upd_retired.lkp_AGMT_ID4 as lkp_AGMT_ID4,
upd_AGMT_CHNL_TYPE_upd_retired.lkp_CHNL_TYPE_CD4 as lkp_CHNL_TYPE_CD4,
upd_AGMT_CHNL_TYPE_upd_retired.lkp_AGMT_CHNL_ROLE_TYPE_CD4 as lkp_AGMT_CHNL_ROLE_TYPE_CD4,
upd_AGMT_CHNL_TYPE_upd_retired.lkp_EDW_STRT_DTTM4 as lkp_EDW_STRT_DTTM4,
upd_AGMT_CHNL_TYPE_upd_retired.in_EDW_STRT_DTTM4 as in_EDW_STRT_DTTM4,
upd_AGMT_CHNL_TYPE_upd_retired.UpdateTime as UpdateTime,
upd_AGMT_CHNL_TYPE_upd_retired.source_record_id
FROM
upd_AGMT_CHNL_TYPE_upd_retired
);


-- Component tgt_AGMT_CHNL_TYPE_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_CHNL_TYPE
(
AGMT_ID,
CHNL_TYPE_CD,
AGMT_CHNL_ROLE_TYPE_CD,
AGMT_CHNL_TYPE_STRT_DTTM,
AGMT_CHNL_TYPE_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_CHNL_TYPE_Insert.in_AGMT_ID1 as AGMT_ID,
exp_CHNL_TYPE_Insert.in_CHNL_TYPE_CD1 as CHNL_TYPE_CD,
exp_CHNL_TYPE_Insert.in_AGMT_CHNL_ROLE_TYPE_CD1 as AGMT_CHNL_ROLE_TYPE_CD,
exp_CHNL_TYPE_Insert.in_AGMT_CHNL_TYPE_STRT_DT1 as AGMT_CHNL_TYPE_STRT_DTTM,
exp_CHNL_TYPE_Insert.in_AGMT_CHNL_TYPE_END_DT1 as AGMT_CHNL_TYPE_END_DTTM,
exp_CHNL_TYPE_Insert.in_PRCS_ID1 as PRCS_ID,
exp_CHNL_TYPE_Insert.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_CHNL_TYPE_Insert.out_EDW_END_DTTM1 as EDW_END_DTTM,
exp_CHNL_TYPE_Insert.UpdateTime as TRANS_STRT_DTTM,
exp_CHNL_TYPE_Insert.in_TRANS_END_DTTM1 as TRANS_END_DTTM
FROM
exp_CHNL_TYPE_Insert;


-- Component tgt_AGMT_CHNL_TYPE_ins_upd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_CHNL_TYPE
(
AGMT_ID,
CHNL_TYPE_CD,
AGMT_CHNL_ROLE_TYPE_CD,
AGMT_CHNL_TYPE_STRT_DTTM,
AGMT_CHNL_TYPE_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
fil_insupd.in_AGMT_ID3 as AGMT_ID,
fil_insupd.in_CHNL_TYPE_CD3 as CHNL_TYPE_CD,
fil_insupd.in_AGMT_CHNL_ROLE_TYPE_CD3 as AGMT_CHNL_ROLE_TYPE_CD,
fil_insupd.in_AGMT_CHNL_TYPE_STRT_DT3 as AGMT_CHNL_TYPE_STRT_DTTM,
fil_insupd.in_AGMT_CHNL_TYPE_END_DT3 as AGMT_CHNL_TYPE_END_DTTM,
fil_insupd.in_PRCS_ID3 as PRCS_ID,
fil_insupd.in_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
fil_insupd.in_EDW_END_DTTM3 as EDW_END_DTTM,
fil_insupd.UpdateTime as TRANS_STRT_DTTM
FROM
fil_insupd;


-- Component exp_DateExpiry, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_DateExpiry AS
(
SELECT
upd_AGMT_CHNL_TYPE_update.lkp_AGMT_ID3 as lkp_AGMT_ID3,
upd_AGMT_CHNL_TYPE_update.lkp_CHNL_TYPE_CD3 as lkp_CHNL_TYPE_CD3,
upd_AGMT_CHNL_TYPE_update.lkp_AGMT_CHNL_ROLE_TYPE_CD3 as lkp_AGMT_CHNL_ROLE_TYPE_CD3,
upd_AGMT_CHNL_TYPE_update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
dateadd ( second, -1, upd_AGMT_CHNL_TYPE_update.in_EDW_STRT_DTTM3 ) as o_DateExpiry,
dateadd ( second, -1, upd_AGMT_CHNL_TYPE_update.UpdateTime ) as out_trans_end_dttm3,
upd_AGMT_CHNL_TYPE_update.source_record_id
FROM
upd_AGMT_CHNL_TYPE_update
);


-- Component tgt_AGMT_CHNL_TYPE_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_CHNL_TYPE
USING exp_DateExpiry ON (AGMT_CHNL_TYPE.AGMT_ID = exp_DateExpiry.lkp_AGMT_ID3 AND AGMT_CHNL_TYPE.CHNL_TYPE_CD = exp_DateExpiry.lkp_CHNL_TYPE_CD3 AND AGMT_CHNL_TYPE.AGMT_CHNL_ROLE_TYPE_CD = exp_DateExpiry.lkp_AGMT_CHNL_ROLE_TYPE_CD3 AND AGMT_CHNL_TYPE.EDW_STRT_DTTM = exp_DateExpiry.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_DateExpiry.lkp_AGMT_ID3,
CHNL_TYPE_CD = exp_DateExpiry.lkp_CHNL_TYPE_CD3,
AGMT_CHNL_ROLE_TYPE_CD = exp_DateExpiry.lkp_AGMT_CHNL_ROLE_TYPE_CD3,
EDW_STRT_DTTM = exp_DateExpiry.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_DateExpiry.o_DateExpiry,
TRANS_END_DTTM = exp_DateExpiry.out_trans_end_dttm3;


-- Component tgt_AGMT_CHNL_TYPE_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_CHNL_TYPE
USING exp_DateExpiry_retired ON (AGMT_CHNL_TYPE.AGMT_ID = exp_DateExpiry_retired.lkp_AGMT_ID4 AND AGMT_CHNL_TYPE.CHNL_TYPE_CD = exp_DateExpiry_retired.lkp_CHNL_TYPE_CD4 AND AGMT_CHNL_TYPE.AGMT_CHNL_ROLE_TYPE_CD = exp_DateExpiry_retired.lkp_AGMT_CHNL_ROLE_TYPE_CD4 AND AGMT_CHNL_TYPE.EDW_STRT_DTTM = exp_DateExpiry_retired.lkp_EDW_STRT_DTTM4)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_DateExpiry_retired.lkp_AGMT_ID4,
CHNL_TYPE_CD = exp_DateExpiry_retired.lkp_CHNL_TYPE_CD4,
AGMT_CHNL_ROLE_TYPE_CD = exp_DateExpiry_retired.lkp_AGMT_CHNL_ROLE_TYPE_CD4,
EDW_STRT_DTTM = exp_DateExpiry_retired.lkp_EDW_STRT_DTTM4,
EDW_END_DTTM = exp_DateExpiry_retired.in_EDW_STRT_DTTM4,
TRANS_END_DTTM = exp_DateExpiry_retired.UpdateTime;


INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_agmt_chnl_type_ins'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_agmt_chnl_type_ins'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );


END; ';