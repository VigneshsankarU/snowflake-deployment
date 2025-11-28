-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_AGMT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
    start_dttm timestamp;
    end_dttm timestamp;
    prcs_id INTEGER;
    p_agmt_type_cd_policy_version VARCHAR;

BEGIN 

 start_dttm := current_timestamp();
 end_dttm := current_timestamp();
 prcs_id := 1;
   p_agmt_type_cd_policy_version := ''PolicyVersion1'';

-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''QUOTN_AGMT'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''AGMT_QUOTN_ROLE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

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


-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as JobNumber,
$2 as BranchNumber,
$3 as PolicyNumber,
$4 as AGMT_QUOTN_ROLE_CD,
$5 as NK_PublicID,
$6 as SRC_CD,
$7 as Retired,
$8 as UpdateTime_policyperiod,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct SRC.JobNumber_stg,

SRC.BranchNumber_stg,

SRC.PolicyNumber_stg,

''Converted from'' AGMT_QUOTN_ROLE_CD, 

SRC.NK_PublicID ,

''SRC_SYS4'' as src_cd, 

case when SRC.retired_stg=0 and SRC.Policy_Retired=0 then 0 else 1 end as retired,

SRC.ev_strt as UpdateTime_policyperiod

from 

(

SELECT DISTINCT

        pc_job.NotTakenNotifDate_stg ,

        pc_job.ArchiveState_stg ,

        pc_job.ArchiveSchemaInfo_stg ,

        pc_job.UpdateTime_stg ,

        pc_job.NotificationDate_stg ,

        pc_job.ID_stg ,

        pc_job.Source_stg ,

        pc_job.ExcludeReason_stg ,

        pc_job.NextPurgeCheckDate_stg ,

        pc_job.CreateUserID_stg ,

        pc_job.ArchiveFailureID_stg ,

        pc_job.RejectReason_stg ,

        pc_job.CloseDate_stg ,

        pc_job.BeanVersion_stg ,

        pc_job.Retired_stg ,

        pc_job.CancelReasonCode_stg ,

        pc_job.ChangePolicyNumber_stg ,

        pc_job.UpdateUserID_stg ,

        pc_job.PrimaryInsuredNameDenorm_stg ,

        pc_job.NonRenewalNotifDate_stg ,

        pc_job.PrimaryInsuredName_stg ,

        pc_job.QuoteType_stg ,

        pc_job.DateQuoteNeeded_stg ,

        pc_job.PublicID_stg ,

       pc_job.SideBySide_stg ,

        pc_job.JobNumber_stg ,

        pc_job.RewriteType_stg ,

        pc_job.CreateTime_stg ,

        pc_job.AuditInformationID_stg ,

        pc_job.PolicyID_stg ,

        pc_job.ExcludedFromArchive_stg ,

         pc_job.ArchiveFailureDetailsID_stg ,

        pc_job.RescindNotificationDate_stg ,

        pc_job.PurgeStatus_stg ,

        pc_job.InitialNotificationDate_stg ,

        pc_job.LastNotifiedCancellationDate_stg ,

        pc_job.JobGroup_stg ,

        pc_job.CancelProcessDate_stg ,

        pc_job.RenewalCode_stg ,

        pc_job.EscalateAfterHoldReleased_stg ,

        pc_job.ReinstateCode_stg ,

        pc_job.RenewalNotifDate_stg ,

        pc_job.PaymentReceived_stg ,

        pc_job.ArchivePartition_stg ,

        pc_job.PaymentReceived_cur_stg ,

        pc_job.NotificationAckDate_stg ,

        pc_job.ArchiveDate_stg ,

        pc_job.BindOption_stg ,

        pc_job.NonRenewalCode_stg ,

        pc_job.Subtype_stg ,

        pc_job.SubmissionDate_stg ,

        CASE WHEN pctl_job.typecode_stg IN ( ''Submission'', ''Renewal'', ''Rewrite'', ''Issuance'' )

             THEN pc_policyperiod.periodstart_stg

             WHEN pctl_job.typecode_stg = ''Cancellation''

             THEN pc_policyperiod.cancellationdate_stg

             WHEN pctl_job.typecode_stg in (''PolicyChange'',''Reinstatement'')

             THEN pc_policyperiod.editeffectivedate_stg

        END ev_strt ,

        pctl_policyperiodstatus.TYPECODE_stg as TYPECODE_policyperiodstatus,

        pc_policyperiod.branchnumber_stg ,

        pc_policyperiod.TotalPremiumRPT_stg ,

        pc_policyperiod.TotalPremAdjRPT_alfa_stg,

        pc_policyperiod.TransactionPremiumRPT_stg ,

        pc_policyperiod.EditEffectivedate_stg ,

        pc_policyperiod.periodend_stg ,

        pc_policyperiod.PolicyNumber_stg ,

        pc_policyline.PAPolicyType_alfa_stg ,

        pc_policyline.HOPolicyType_stg ,

       pc_policyline.ClaimsFreeInd_alfa_stg,

        pc_policyperiod.Publicid_stg as NK_PublicID ,

        pc_policyperiod.RateAsOfDate_stg ,

       pc_contact.AddressBookUID_stg as userpartyid ,

       pc_policyperiod.UpdateTime_stg as pc_updatetime,

       pc_policyperiod.GeneralPlusTier_alfa_stg as GeneralPlusTier_alfa,

       pc_policyperiod.Retired_stg as Policy_Retired,

       CASE WHEN pcp1.publicid_stg=pc_policyperiod.PublicID_stg THEN ''Y'' ELSE ''N'' END as SelectVesionOfQuote,

          coalesce(pc_effectivedatedfields.OverrideCreditScore_alfa_stg,0) as RatedInsuranceScore,pc_effectivedatedfields.ContinuousServiceDate_alfa_stg,

		     pc_effectivedatedfields.prevInsurance_alfa_stg,

             pctl_billingperiodicity.TYPECODE_stg as AutoLatePayBillingPeriodicity,

             pctl_cancellationsource.typecode_stg AS Bill_payment_Src,

       pc_policyperiod.TotalCostRPT_stg,

       pc_policyperiod.TransactionCostRPT_stg,

       pc_policyperiod.TotalDiscountPremRPT_alfa_stg,

       pc_policyperiod.TotalSurchargePremRPT_alfa_stg,

       pctl_sourceofbusiness_alfa.TYPECODE_stg AS SRC_OF_BUSN_CD,

case when pc_policyperiod.QuoteMaturityLevel_stg=1 and  vj.JobID_stg > 0  then 0 else 1 end as ValidQuote,

/* EIM-36450 Changed as part of GW changes */
pc_policyterm.Updatetime_stg as pc_policyterm_Updatetime

, pc_policyline.BP7PolicyType_alfa_stg

,pc_effectivedatedfields.OverrideCreditScoreDate_alfa_stg

,pc_policyperiod.IsQuoteOnline_alfa_stg /*Added as part of  EIM-20110*/

,pc_effectivedatedfields.RetentionScore_alfa_stg /*Added as part of EIM-18360*/



FROM    DB_T_PROD_STAG.pc_job

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg

        LEFT JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reasoncode AS rejectreason ON rejectreason.id_stg = pc_job.RejectReason_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reasoncode AS cancelreason ON cancelreason.id_stg = pc_job.CancelReasonCode_stg

             left join DB_T_PROD_STAG.pc_policyterm on pc_policyterm.id_stg = pc_policyperiod.policytermid_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reinstatecode ON pctl_reinstatecode.id_stg = pc_job.ReinstateCode_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nonrenewalcode ON pctl_nonrenewalcode.id_stg = pc_policyterm.NonRenewReason_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_renewalcode ON pctl_renewalcode.id_stg = pc_job.RenewalCode_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_PolicyLine ON pc_policyline.BranchID_stg = pc_policyperiod.id_stg

        INNER JOIN DB_T_PROD_STAG.pctl_job ON pctl_job.id_stg = pc_job.Subtype_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_user ON pc_job.CreateUserID_stg = pc_user.ID_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_contact ON pc_user.ContactID_stg = pc_contact.id_stg

       LEFT OUTER  JOIN DB_T_PROD_STAG.pctl_quotetype on pctl_quotetype.id_stg=pc_job.QuoteType_stg

       LEFT OUTER JOIN DB_T_PROD_STAG.pctl_riskstatus_alfa on pctl_riskstatus_alfa.id_stg=pc_job.Risk_alfa_stg

       LEFT OUTER JOIN DB_T_PROD_STAG.pc_jobpolicyperiod on pc_job.id_stg = pc_jobpolicyperiod.OwnerID_stg

    LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod as PCP1 on pc_jobpolicyperiod.ForeignEntityID_stg = PCP1.id_stg

       LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields on pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg

          LEFT OUTER JOIN DB_T_PROD_STAG.pctl_sourceofbusiness_alfa ON pc_effectivedatedfields.sourceofbusiness_alfa_stg = pctl_sourceofbusiness_alfa.id_stg

left outer join DB_T_PROD_STAG.pcx_holineratingfactor_alfa on pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg

left outer join DB_T_PROD_STAG.pctl_billingperiodicity on pcx_holineratingfactor_alfa.AutoLatePayBillingPeriodicity_stg=pctl_billingperiodicity.ID_stg



left join DB_T_PROD_STAG.pc_groupuser on pc_user.id_stg = pc_groupuser.UserID_stg 

left join DB_T_PROD_STAG.pc_group on pc_groupuser.GroupID_stg = pc_group.id_stg 

left join DB_T_PROD_STAG.pctl_grouptype on pc_group.GroupType_stg = pctl_grouptype.ID_stg

left JOIN DB_T_PROD_STAG.pctl_cancellationsource ON pc_job.source_stg = pctl_cancellationsource.id_stg

left join ( SELECT distinct JobID_stg from DB_T_PROD_STAG.pc_policyperiod  where QuoteMaturityLevel_stg in (2,3) ) vj ON pc_job.id_stg=vj.JobID_stg

/* EIM-36450 Changed as part of GW changes */


WHERE  

pc_policyperiod.UpdateTime_stg > (:start_dttm) and pc_policyperiod.UpdateTime_stg <= (:end_dttm) and 

 pctl_policyperiodstatus.typecode_stg <> ''Temporary'' and pc_effectivedatedfields.expirationdate_stg is null

and pcx_holineratingfactor_alfa.ExpirationDate_stg is  null

) SRC



inner join DB_T_PROD_STAG.pctl_job pctl_job on pctl_job.id_stg=SRC.Subtype_stg

where SRC.TYPECODE_policyperiodstatus=''Bound''   

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'')

and SRC.PolicyNumber_stg is not null
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
SQ_pc_policyperiod.PolicyNumber as PolicyNumber,
SQ_pc_policyperiod.BranchNumber as BranchNumber,
SQ_pc_policyperiod.JobNumber as JobNumber,
SQ_pc_policyperiod.AGMT_QUOTN_ROLE_CD as AGMT_QUOTN_ROLE_CD,
SQ_pc_policyperiod.NK_PublicID as NK_PublicID,
SQ_pc_policyperiod.SRC_CD as SRC_CD,
SQ_pc_policyperiod.Retired as Retired,
SQ_pc_policyperiod.UpdateTime_policyperiod as UpdateTime_policyperiod,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component exp_data_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_trans AS
(
SELECT
exp_pass_from_source.BranchNumber as BranchNumber,
exp_pass_from_source.JobNumber as JobNumber,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as out_AGMT_QUOTN_ROLE_CD,
:p_agmt_type_cd_policy_version as out_AGMT_TYPE_CD,
exp_pass_from_source.NK_PublicID as NK_PublicID,
:PRCS_ID as out_PRCS_ID,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as OUT_SRC_CD,
exp_pass_from_source.Retired as Retired,
exp_pass_from_source.UpdateTime_policyperiod as UpdateTime_policyperiod,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK
FROM
exp_pass_from_source
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.AGMT_QUOTN_ROLE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_from_source.AGMT_QUOTN_ROLE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_pass_from_source.SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_AGMT_NEW, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_NEW AS
(
SELECT
LKP.AGMT_ID,
exp_data_trans.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_data_trans
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM db_t_prod_core.AGMT QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_data_trans.NK_PublicID AND LKP.AGMT_TYPE_CD = exp_data_trans.out_AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_data_trans.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_data_trans
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM db_t_prod_core.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_data_trans.JobNumber AND LKP.VERS_NBR = exp_data_trans.BranchNumber
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_AGMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_AGMT AS
(
SELECT
LKP.AGMT_ID,
LKP.QUOTN_ID,
LKP.AGMT_QUOTN_ROLE_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_trans.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.QUOTN_ID asc,LKP.AGMT_QUOTN_ROLE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_data_trans
INNER JOIN LKP_AGMT_NEW ON exp_data_trans.source_record_id = LKP_AGMT_NEW.source_record_id
INNER JOIN LKP_INSRNC_QUOTN ON LKP_AGMT_NEW.source_record_id = LKP_INSRNC_QUOTN.source_record_id
LEFT JOIN (
SELECT QUOTN_AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, QUOTN_AGMT.EDW_END_DTTM as EDW_END_DTTM, QUOTN_AGMT.AGMT_ID as AGMT_ID, QUOTN_AGMT.QUOTN_ID as QUOTN_ID, QUOTN_AGMT.AGMT_QUOTN_ROLE_CD as AGMT_QUOTN_ROLE_CD 
FROM db_t_prod_core.QUOTN_AGMT 
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT_ID,QUOTN_ID,AGMT_QUOTN_ROLE_CD ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.AGMT_ID = LKP_AGMT_NEW.AGMT_ID AND LKP.QUOTN_ID = LKP_INSRNC_QUOTN.QUOTN_ID AND LKP.AGMT_QUOTN_ROLE_CD = exp_data_trans.out_AGMT_QUOTN_ROLE_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.QUOTN_ID asc,LKP.AGMT_QUOTN_ROLE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) 
= 1
);


-- Component exp_ins_rej, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_rej AS
(
SELECT
LKP_QUOTN_AGMT.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
LKP_QUOTN_AGMT.EDW_END_DTTM as LKP_EDW_END_DTTM,
LKP_AGMT_NEW.AGMT_ID as AGMT_ID,
LKP_INSRNC_QUOTN.QUOTN_ID as QUOTN_ID,
exp_data_trans.out_AGMT_QUOTN_ROLE_CD as out_AGMT_QUOTN_ROLE_CD,
exp_data_trans.out_PRCS_ID as PRCS_ID,
exp_data_trans.Retired as Retired,
CASE WHEN LKP_QUOTN_AGMT.AGMT_ID IS NULL and LKP_QUOTN_AGMT.QUOTN_ID IS NULL and LKP_QUOTN_AGMT.AGMT_QUOTN_ROLE_CD IS NULL THEN ''I'' ELSE ''R'' END as out_ins_rej,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_data_trans.UpdateTime_policyperiod as UpdateTime_policyperiod,
exp_data_trans.source_record_id
FROM
exp_data_trans
INNER JOIN LKP_AGMT_NEW ON exp_data_trans.source_record_id = LKP_AGMT_NEW.source_record_id
INNER JOIN LKP_INSRNC_QUOTN ON LKP_AGMT_NEW.source_record_id = LKP_INSRNC_QUOTN.source_record_id
INNER JOIN LKP_QUOTN_AGMT ON LKP_INSRNC_QUOTN.source_record_id = LKP_QUOTN_AGMT.source_record_id
);


-- Component rtr_quotn_agmt_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_quotn_agmt_INSERT as
SELECT
exp_ins_rej.AGMT_ID as AGMT_ID,
exp_ins_rej.QUOTN_ID as QUOTN_ID,
exp_ins_rej.out_AGMT_QUOTN_ROLE_CD as out_AGMT_QUOTN_ROLE_CD,
exp_ins_rej.PRCS_ID as PRCS_ID,
exp_ins_rej.Retired as Retired,
exp_ins_rej.out_ins_rej as out_ins_rej,
exp_ins_rej.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_rej.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_rej.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_rej.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_rej.UpdateTime_policyperiod as UpdateTime_policyperiod,
exp_ins_rej.source_record_id
FROM
exp_ins_rej
WHERE exp_ins_rej.out_ins_rej = ''I'' AND exp_ins_rej.AGMT_ID IS NOT NULL and exp_ins_rej.QUOTN_ID IS NOT NULL OR ( exp_ins_rej.Retired = 0 AND exp_ins_rej.LKP_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_ins_rej.AGMT_ID IS NOT NULL and exp_ins_rej.QUOTN_ID IS NOT NULL );


-- Component rtr_quotn_agmt_RETIRED, Type ROUTER Output Group RETIRED
create or replace temporary table rtr_quotn_agmt_RETIRED as
SELECT
exp_ins_rej.AGMT_ID as AGMT_ID,
exp_ins_rej.QUOTN_ID as QUOTN_ID,
exp_ins_rej.out_AGMT_QUOTN_ROLE_CD as out_AGMT_QUOTN_ROLE_CD,
exp_ins_rej.PRCS_ID as PRCS_ID,
exp_ins_rej.Retired as Retired,
exp_ins_rej.out_ins_rej as out_ins_rej,
exp_ins_rej.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_rej.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_rej.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_rej.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_rej.UpdateTime_policyperiod as UpdateTime_policyperiod,
exp_ins_rej.source_record_id
FROM
exp_ins_rej
WHERE exp_ins_rej.out_ins_rej = ''R'' and exp_ins_rej.Retired != 0 and exp_ins_rej.LKP_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_ins_rej.AGMT_ID IS NOT NULL and exp_ins_rej.QUOTN_ID IS NOT NULL;


-- Component upd_quotn_agmt_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_agmt_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_agmt_RETIRED.AGMT_ID as AGMT_ID3,
rtr_quotn_agmt_RETIRED.QUOTN_ID as QUOTN_ID3,
rtr_quotn_agmt_RETIRED.out_AGMT_QUOTN_ROLE_CD as out_AGMT_QUOTN_ROLE_CD3,
rtr_quotn_agmt_RETIRED.PRCS_ID as PRCS_ID3,
rtr_quotn_agmt_RETIRED.Retired as Retired3,
rtr_quotn_agmt_RETIRED.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_quotn_agmt_RETIRED.EDW_STRT_DTTM as EDW_STRT_DTTM3,
NULL as EDW_END_DTTM3,
rtr_quotn_agmt_RETIRED.UpdateTime_policyperiod as UpdateTime_policyperiod3,
1 as UPDATE_STRATEGY_ACTION,
rtr_quotn_agmt_RETIRED.source_record_id
FROM
rtr_quotn_agmt_RETIRED
);


-- Component exp_pass_to_tgt_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_retired AS
(
SELECT
upd_quotn_agmt_upd_retired.AGMT_ID3 as AGMT_ID,
upd_quotn_agmt_upd_retired.QUOTN_ID3 as QUOTN_ID,
upd_quotn_agmt_upd_retired.out_AGMT_QUOTN_ROLE_CD3 as out_AGMT_QUOTN_ROLE_CD,
upd_quotn_agmt_upd_retired.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_quotn_agmt_upd_retired.UpdateTime_policyperiod3 as TRANS_START_DTTM,
upd_quotn_agmt_upd_retired.source_record_id
FROM
upd_quotn_agmt_upd_retired
);


-- Component QUOTN_AGMT_Update_Retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.QUOTN_AGMT
USING exp_pass_to_tgt_upd_retired ON (QUOTN_AGMT.AGMT_ID = exp_pass_to_tgt_upd_retired.AGMT_ID AND QUOTN_AGMT.QUOTN_ID = exp_pass_to_tgt_upd_retired.QUOTN_ID AND QUOTN_AGMT.AGMT_QUOTN_ROLE_CD = exp_pass_to_tgt_upd_retired.out_AGMT_QUOTN_ROLE_CD)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_tgt_upd_retired.AGMT_ID,
QUOTN_ID = exp_pass_to_tgt_upd_retired.QUOTN_ID,
AGMT_QUOTN_ROLE_CD = exp_pass_to_tgt_upd_retired.out_AGMT_QUOTN_ROLE_CD,
EDW_STRT_DTTM = exp_pass_to_tgt_upd_retired.LKP_EDW_STRT_DTTM,
EDW_END_DTTM = exp_pass_to_tgt_upd_retired.EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_tgt_upd_retired.TRANS_START_DTTM;


-- Component upd_quotn_agmt_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_quotn_agmt_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_quotn_agmt_INSERT.AGMT_ID as AGMT_ID1,
rtr_quotn_agmt_INSERT.QUOTN_ID as QUOTN_ID1,
rtr_quotn_agmt_INSERT.out_AGMT_QUOTN_ROLE_CD as out_AGMT_QUOTN_ROLE_CD1,
rtr_quotn_agmt_INSERT.PRCS_ID as PRCS_ID1,
rtr_quotn_agmt_INSERT.Retired as Retired1,
rtr_quotn_agmt_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_quotn_agmt_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_quotn_agmt_INSERT.UpdateTime_policyperiod as UpdateTime_policyperiod1,
0 as UPDATE_STRATEGY_ACTION,
rtr_quotn_agmt_INSERT.source_record_id
FROM
rtr_quotn_agmt_INSERT
);


-- Component exp_pass_to_tgt_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_insert AS
(
SELECT
upd_quotn_agmt_insert.AGMT_ID1 as AGMT_ID,
upd_quotn_agmt_insert.QUOTN_ID1 as QUOTN_ID,
upd_quotn_agmt_insert.out_AGMT_QUOTN_ROLE_CD1 as out_AGMT_QUOTN_ROLE_CD,
upd_quotn_agmt_insert.PRCS_ID1 as PRCS_ID,
upd_quotn_agmt_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
CASE WHEN upd_quotn_agmt_insert.Retired1 = 0 THEN upd_quotn_agmt_insert.EDW_END_DTTM1 ELSE upd_quotn_agmt_insert.EDW_STRT_DTTM1 END as out_EDW_END_DTTM,
upd_quotn_agmt_insert.UpdateTime_policyperiod1 as TRANS_START_DTTM,
CASE WHEN upd_quotn_agmt_insert.Retired1 <> 0 THEN upd_quotn_agmt_insert.UpdateTime_policyperiod1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
upd_quotn_agmt_insert.source_record_id
FROM
upd_quotn_agmt_insert
);


-- Component QUOTN_AGMT_Insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_AGMT
(
AGMT_ID,
QUOTN_ID,
AGMT_QUOTN_ROLE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_insert.AGMT_ID as AGMT_ID,
exp_pass_to_tgt_insert.QUOTN_ID as QUOTN_ID,
exp_pass_to_tgt_insert.out_AGMT_QUOTN_ROLE_CD as AGMT_QUOTN_ROLE_CD,
exp_pass_to_tgt_insert.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_insert.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_insert.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_insert.TRANS_START_DTTM as TRANS_STRT_DTTM,
exp_pass_to_tgt_insert.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_insert;


END; ';