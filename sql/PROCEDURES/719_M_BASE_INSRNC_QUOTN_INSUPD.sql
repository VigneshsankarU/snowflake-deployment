-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INSRNC_QUOTN_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 declare
	
var_prev_jobnumber1 int;
VAR_PREV_BRANCHNUMBER1 int;
VAR_PREV_QUOTN_ID int;

run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;

BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
set var_prev_jobnumber1=1;  -- unused
set VAR_PREV_BRANCHNUMBER1=1;  -- unused
set VAR_PREV_QUOTN_ID=1;  -- unused

-- Component LKP_TERADATA_ETL_REF_SRC_BUSN_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_SRC_BUSN_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_OF_BUSN'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_sourceofbusiness_alfa.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_STS_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_STS_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''AGMT_STS_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_policyperiodstatus.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_STS_RSN_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_STS_RSN_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''QUOTN_CLS_RSN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_reasoncode.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''APLCTN_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_job.Typecode''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''GW''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_QUOTN_ORIGN_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_QUOTN_ORIGN_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''QUOTN_ORIGN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

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

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_TIER_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_TIER_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''TIER_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pc_policyperiod.GeneralPlusTier_alfa'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_XREF_APLCNT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_APLCNT AS
(
SELECT 
DIR_APLCTN.APLCTN_ID as APLCTN_ID, 
ltrim(rtrim(DIR_APLCTN.HOST_APLCTN_ID)) as HOST_APLCTN_ID, 
DIR_APLCTN.VERS_NBR as VERS_NBR, 
ltrim(rtrim(DIR_APLCTN.DIR_TYPE_VAL)) as DIR_TYPE_VAL, 
ltrim(rtrim(DIR_APLCTN.APLCTN_TYPE_CD)) as APLCTN_TYPE_CD,
 ltrim(rtrim(DIR_APLCTN.SRC_SYS_CD)) as SRC_SYS_CD 
FROM 
DB_T_PROD_CORE.DIR_APLCTN
);


-- Component sq_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as JobNumber,
$2 as TotalPremAdjRPT_alfa,
$3 as TotalPremiumRPT,
$4 as quotn_sbtype_cd,
$5 as quotn_sts_type_cd,
$6 as quotn_cls_rsn_cd,
$7 as createtime,
$8 as closedate,
$9 as Editeffectivedate,
$10 as periodend,
$11 as branchnumber,
$12 as quotetype,
$13 as RateAsOfDate,
$14 as UpdateTime,
$15 as GeneralPlusTier_alfa,
$16 as SYS_SRC_CD,
$17 as Retired,
$18 as SelectVesionOfQuote,
$19 as OverrideCreditScore_alfa,
$20 as ContinuousServiceDate_alfa,
$21 as Quotn_Orgin_Cd,
$22 as ClaimsFreeInd_alfa,
$23 as prevInsurance_alfa,
$24 as AutoLatePayBillingPeriodicity,
$25 as SRC_OF_BUSN_CD,
$26 as PeriodEnd_busn,
$27 as updatetime_trans,
$28 as pc_job_id,
$29 as OverrideCreditScoreDate_alfa,
$30 as RetentionScore_alfa,
$31 as rnk,
$32 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	

a.*

,row_number() over (partition by jobnumber_stg,branchnumber_stg  

order	by updatetime_trans,createtime,ClaimsFreeInd_alfa_stg) rnk

FROM	

(

SELECT	DISTINCT pc_job.jobnumber_stg,

pc_job.TotalPremAdjRPT_alfa_stg,

pc_job.TotalPremiumRPT_stg,

pctl_job.typecode_stg AS QUOTN_SBTYPE_CD,

pc_job.Typecode_policyperiodstatus_stg AS QUOTN_STS_TYPE_CD,

pctl_reasoncode.Typecode_stg AS QUOTN_CLS_RSN_CD,

pc_job.pc_createtime_stg as createtime,

pc_job.closedate_stg,

pc_job.EditEffectiveDate_stg,

pc_job.periodend_stg,

pc_job.branchnumber_stg,

pc_job.quotetype_stg,

pc_job.RateAsOfDate_stg,

pc_job.pc_updatetime,

pc_job.GeneralPlusTier_alfa,

''SRC_SYS4'' AS SYS_SRC_CD,

pc_job.Retired_stg AS Retired,

pc_job.SelectVesionOfQuote as SelectVesionOfQuote,

pc_job.RatedInsuranceScore,

pc_job.ContinuousServiceDate_alfa_stg,

/* CAST(''QUOTN_ORIGN_TYPE2'' AS VARCHAR(50)) AS QUOTN_ORGIN_CD, */
/*This has been added through EIM-24630 start */

case when pc_job.PolicyPeriodSource_stg=10004 then ''T'' when pc_job.PolicyPeriodSource_stg is null then null else ''F'' end AS  QUOTN_ORGIN_CD,

/*This has been added through EIM-24630 ends */

ClaimsFreeInd_alfa_stg,

prevInsurance_alfa_stg,

pc_job.AutoLatePayBillingPeriodicity,

SRC_OF_BUSN_CD,

pc_job.periodend_stg as PeriodEnd_busn,

pc_job.pc_updatetime as updatetime_trans

,pc_job.id_stg as pc_job_id  /*  added for EIM-17878 - Legacy DB_T_CORE_DM_PROD.Discount */
,pc_job.OverrideCreditScoreDate_alfa_stg as OverrideCreditScoreDate_alfa

,pc_job.RetentionScore_alfa_stg /* EIM-18360 */


FROM	(

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

        pctl_policyperiodstatus.TYPECODE_stg as Typecode_policyperiodstatus_stg,

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

        pc_policyperiod.Publicid_stg ,

        pc_policyperiod.RateAsOfDate_stg ,

        pc_contact.AddressBookUID_stg userpartyid ,

       pc_policyperiod.UpdateTime_stg as pc_updatetime,

case when pc_policyperiod.GeneralPlusTier_alfa_stg=1 then ''T'' when pc_policyperiod.GeneralPlusTier_alfa_stg=0 then ''F'' ELSE cast(pc_policyperiod.GeneralPlusTier_alfa_stg as string) end as GeneralPlusTier_alfa,/* EIM-32763 Replicated bit conversion issue */
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

case when pc_policyperiod. QuoteMaturityLevel_stg = 1 and  vj.JobID_stg>0  then 0 else 1 end as ValidQuote,

pc_policyperiod.createtime_stg as pc_createtime_stg,

pc_policyterm.Updatetime_stg as pc_policyterm_Updatetime,

pc_policyline.BP7PolicyType_alfa_stg,

pc_effectivedatedfields.OverrideCreditScoreDate_alfa_stg,

/* ,pc_policyperiod.IsQuoteOnline_alfa_stg Added as part of  EIM-20110; commented through EIM-24630 */
/*Added as part of EIM-24630 starts*/

pc_policyperiod.PolicyPeriodSource_stg, 

/*Added as part of EIM-24630 ends*/

pc_effectivedatedfields.RetentionScore_alfa_stg /*Added as part of EIM-18360*/



FROM    DB_T_PROD_STAG.pc_job

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_job.id_stg = pc_policyperiod.JobID_stg

        LEFT JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reasoncode AS rejectreason ON rejectreason.id_stg = pc_job.RejectReason_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reasoncode AS cancelreason ON cancelreason.id_stg = pc_job.CancelReasonCode_stg

             left join DB_T_PROD_STAG.pc_policyterm on pc_policyterm.id_stg = pc_policyperiod.policytermid_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reinstatecode ON pctl_reinstatecode.id_stg = pc_job.ReinstateCode_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_nonrenewalcode ON pctl_nonrenewalcode.id_stg = pc_policyterm.NonRenewReason_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pctl_renewalcode ON pctl_renewalcode.id_stg = pc_job.RenewalCode_stg

        LEFT OUTER JOIN DB_T_PROD_STAG.pc_PolicyLine ON pc_policyline.BranchID_stg= pc_policyperiod.id_stg

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

left join DB_T_PROD_STAG.pc_group on pc_groupuser.GroupID_stg = pc_group.id_stg left join DB_T_PROD_STAG.pctl_grouptype on pc_group.GroupType_stg = pctl_grouptype.ID_stg

left JOIN DB_T_PROD_STAG.pctl_cancellationsource ON pc_job.source_stg = pctl_cancellationsource.id_stg

left join ( SELECT distinct JobID_stg from DB_T_PROD_STAG.pc_policyperiod  where QuoteMaturityLevel_stg in (2,3)) vj ON pc_job.id_stg=vj.JobID_stg



WHERE  

pc_policyperiod.UpdateTime_stg > (:start_dttm) and pc_policyperiod.UpdateTime_stg <= (:end_dttm) and 

 pctl_policyperiodstatus.typecode_stg<>''Temporary'' and pc_effectivedatedfields.expirationdate_stg is null

and pcx_holineratingfactor_alfa.ExpirationDate_stg is  null



) pc_job 

INNER JOIN DB_T_PROD_STAG.pctl_job 

	ON	pctl_job.id_stg=pc_job.Subtype_stg  

LEFT OUTER JOIN DB_T_PROD_STAG.pctl_reasoncode 

	ON	pctl_reasoncode.id_stg=pc_job.RejectReason_stg

WHERE	pctl_job.TYPECODE_stg  IN (''Submission'',''PolicyChange'',''Renewal'') 

	AND	pc_job.Typecode_policyperiodstatus_stg<>''Temporary'' 

	and	pc_job.policynumber_stg is not null 

) a 

ORDER	BY jobnumber_stg,branchnumber_stg,rnk
) SRC
)
);


-- Component exp_default_values, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_default_values AS
(
SELECT
sq_pc_policyperiod.TotalPremiumRPT as TotalPremiumRPT,
DECODE ( TRUE , LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE */ IS NULL , ''UNK'' , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE */ ) as o_quotn_sbtype_cd,
sq_pc_policyperiod.JobNumber as JobNumber,
sq_pc_policyperiod.JobNumber as out_JobNumber,
sq_pc_policyperiod.createtime as createtime,
sq_pc_policyperiod.branchnumber as branchnumber,
DATE_TRUNC(DAY, sq_pc_policyperiod.Editeffectivedate) as o_Editeffectivedate,
sq_pc_policyperiod.closedate as closedate,
DATE_TRUNC(DAY, sq_pc_policyperiod.periodend) as o_periodend,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_STS_CD */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_STS_CD */ END as out_quotn_sts_type_cd,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_STS_RSN_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_STS_RSN_TYPE */ END as out_quotn_cls_rsn_cd,
LTRIM ( RTRIM ( CASE WHEN sq_pc_policyperiod.Quotn_Orgin_Cd = ''T'' THEN ''QUOTN_ORIGN_TYPE3'' ELSE ''QUOTN_ORIGN_TYPE2'' END ) ) as var_quotn_orign_cd,
LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_QUOTN_ORIGN_CD */ as var_lkp_quotn_orgin_cd,
CASE WHEN TRIM(var_quotn_orign_cd) = '''' OR var_quotn_orign_cd IS NULL OR LENGTH ( var_quotn_orign_cd ) = 0 OR var_lkp_quotn_orgin_cd IS NULL THEN ''UNK'' ELSE var_lkp_quotn_orgin_cd END as out_quotn_orign_cd,
''UNK'' as agmt_objtv_type_cd,
CASE WHEN DATE_TRUNC(DAY, sq_pc_policyperiod.RateAsOfDate) IS NULL THEN TO_DATE ( ''01/01/1900'' , ''MM/DD/YYYY'' ) ELSE DATE_TRUNC(DAY, sq_pc_policyperiod.RateAsOfDate) END as o_RateAsOfDate,
sq_pc_policyperiod.UpdateTime as UpdateTime,
:PRCS_ID as prcs_id,
sq_pc_policyperiod.SYS_SRC_CD as SYS_SRC_CD,
LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as in_SYS_SRC_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
sq_pc_policyperiod.Retired as Retired,
LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_TIER_TYPE_CD */ as in_Tier_Type_cd,
sq_pc_policyperiod.SelectVesionOfQuote as SelectVesionOfQuote,
TO_CHAR ( sq_pc_policyperiod.OverrideCreditScore_alfa ) as RTD_INSRNC_SCR_VAL,
sq_pc_policyperiod.ContinuousServiceDate_alfa as ContinuousServiceDate_alfa,
sq_pc_policyperiod.ClaimsFreeInd_alfa as ClaimsFreeInd_alfa,
sq_pc_policyperiod.prevInsurance_alfa as prevInsurance_alfa,
sq_pc_policyperiod.AutoLatePayBillingPeriodicity as AutoLatePayBillingPeriodicity,
sq_pc_policyperiod.TotalPremAdjRPT_alfa as TotalPremAdjRPT_alfa,
sq_pc_policyperiod.PeriodEnd_busn as PeriodEnd_busn,
sq_pc_policyperiod.updatetime_trans as updatetime_trans,
sq_pc_policyperiod.rnk as rnk,
CASE WHEN LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_SRC_BUSN_CD */ IS NULL THEN ''UNK'' ELSE LKP_11.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_SRC_BUSN_CD */ END as out_SRC_OF_BUSN_CD,
sq_pc_policyperiod.OverrideCreditScoreDate_alfa as OverrideCreditScoreDate_alfa,
sq_pc_policyperiod.RetentionScore_alfa as RetentionScore_alfa,
sq_pc_policyperiod.source_record_id,
row_number() over (partition by sq_pc_policyperiod.source_record_id order by sq_pc_policyperiod.source_record_id) as RNK1
FROM
sq_pc_policyperiod
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_policyperiod.quotn_sbtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_APLCTN_APLCTN_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pc_policyperiod.quotn_sbtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_STS_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_pc_policyperiod.quotn_sts_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_STS_CD LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_pc_policyperiod.quotn_sts_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_STS_RSN_TYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_pc_policyperiod.quotn_cls_rsn_cd
LEFT JOIN LKP_TERADATA_ETL_REF_STS_RSN_TYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = sq_pc_policyperiod.quotn_cls_rsn_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_QUOTN_ORIGN_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = var_quotn_orign_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = sq_pc_policyperiod.SYS_SRC_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_TIER_TYPE_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = sq_pc_policyperiod.GeneralPlusTier_alfa
LEFT JOIN LKP_TERADATA_ETL_REF_SRC_BUSN_CD LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = sq_pc_policyperiod.SRC_OF_BUSN_CD
LEFT JOIN LKP_TERADATA_ETL_REF_SRC_BUSN_CD LKP_11 ON LKP_11.SRC_IDNTFTN_VAL = sq_pc_policyperiod.SRC_OF_BUSN_CD
QUALIFY RNK1 = 1
);


-- Component LKP_APLCTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_APLCTN AS
(
SELECT
LKP.APLCTN_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_default_values.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_default_values.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_default_values.source_record_id ORDER BY LKP.APLCTN_ID asc,LKP.APLCTN_TYPE_CD asc,LKP.HOST_APLCTN_ID asc,LKP.APLCTN_CMPLTD_DTTM asc,LKP.APLCTN_RECVD_DTTM asc,LKP.SRC_SYS_CD asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.APLCTN_QUOT_TYPE_CD asc,LKP.PROD_GRP_ID asc,LKP.PROD_ID asc,LKP.CHNL_TYPE_CD asc,LKP.HOST_APLCTN_NUM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.TRANS_END_DTTM asc) RNK
FROM
exp_default_values
LEFT JOIN (
SELECT APLCTN.APLCTN_ID as APLCTN_ID, 
APLCTN.APLCTN_CMPLTD_DTTM as APLCTN_CMPLTD_DTTM, 
APLCTN.APLCTN_RECVD_DTTM as APLCTN_RECVD_DTTM,
APLCTN.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD,
APLCTN.APLCTN_QUOT_TYPE_CD as APLCTN_QUOT_TYPE_CD, 
APLCTN.PROD_GRP_ID as PROD_GRP_ID,
APLCTN.PROD_ID as PROD_ID, 
APLCTN.CHNL_TYPE_CD as CHNL_TYPE_CD, 
APLCTN.HOST_APLCTN_NUM as HOST_APLCTN_NUM, 
APLCTN.EDW_STRT_DTTM as EDW_STRT_DTTM, 
APLCTN.EDW_END_DTTM as EDW_END_DTTM, 
APLCTN.HOST_APLCTN_ID as HOST_APLCTN_ID, 
APLCTN.SRC_SYS_CD as SRC_SYS_CD, 
APLCTN.APLCTN_TYPE_CD as APLCTN_TYPE_CD,
APLCTN.TRANS_STRT_DTTM as TRANS_STRT_DTTM, 
APLCTN.TRANS_END_DTTM as TRANS_END_DTTM
FROM DB_T_PROD_CORE.APLCTN
QUALIFY ROW_NUMBER () OVER (partition by HOST_APLCTN_ID,SRC_SYS_CD order by EDW_END_DTTM desc)=1
) LKP ON LKP.HOST_APLCTN_ID = exp_default_values.out_JobNumber AND LKP.SRC_SYS_CD = exp_default_values.in_SYS_SRC_CD AND LKP.APLCTN_TYPE_CD = exp_default_values.o_quotn_sbtype_cd
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
LKP.DSCNT_AMT,
LKP.PREM_AMT,
LKP.QUOTN_SBTYPE_CD,
LKP.QUOTN_STS_TYPE_CD,
LKP.QUOTN_CLS_RSN_CD,
LKP.QUOTN_ORIGN_CD,
LKP.APLCTN_ID,
LKP.QUOTN_OPN_DTTM,
LKP.QUOTN_CLS_DTTM,
LKP.QUOTN_PLND_AGMT_OPN_DTTM,
LKP.QUOTN_PLND_AGMT_CLS_DTTM,
LKP.AGMT_OBJTV_TYPE_CD,
LKP.NK_JOB_NBR,
LKP.VERS_NBR,
LKP.RTD_DTTM,
LKP.QUOTN_UPDT_DTTM,
LKP.QUOTN_SLCTD_IND,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.SRC_SYS_CD,
LKP.RTD_INSRNC_SCR_VAL,
LKP.TRANS_STRT_DTTM,
LKP.CNTNUS_SRVC_DT,
LKP.SRC_OF_BUSN_CD,
LKP.PRIOR_CLM_FREE_IND,
LKP.RTD_INSRNC_SCR_DTTM,
LKP.RETN_SCR_VAL,
exp_default_values.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_default_values.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.DSCNT_AMT asc,LKP.PREM_AMT asc,LKP.QUOTN_SBTYPE_CD asc,LKP.QUOTN_STS_TYPE_CD asc,LKP.QUOTN_CLS_RSN_CD asc,LKP.QUOTN_ORIGN_CD asc,LKP.APLCTN_ID asc,LKP.QUOTN_OPN_DTTM asc,LKP.QUOTN_CLS_DTTM asc,LKP.QUOTN_PLND_AGMT_OPN_DTTM asc,LKP.QUOTN_PLND_AGMT_CLS_DTTM asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.NK_JOB_NBR asc,LKP.VERS_NBR asc,LKP.RTD_DTTM asc,LKP.QUOTN_UPDT_DTTM asc,LKP.QUOTN_SLCTD_IND asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc,LKP.RTD_INSRNC_SCR_VAL asc,LKP.TRANS_STRT_DTTM asc,LKP.CNTNUS_SRVC_DT asc,LKP.SRC_OF_BUSN_CD asc,LKP.PRIOR_CLM_FREE_IND asc,LKP.RTD_INSRNC_SCR_DTTM asc,LKP.RETN_SCR_VAL asc) RNK
FROM
exp_default_values
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID as QUOTN_ID, INSRNC_QUOTN.DSCNT_AMT as DSCNT_AMT, INSRNC_QUOTN.PREM_AMT as PREM_AMT, INSRNC_QUOTN.QUOTN_SBTYPE_CD as QUOTN_SBTYPE_CD, INSRNC_QUOTN.QUOTN_STS_TYPE_CD as QUOTN_STS_TYPE_CD, INSRNC_QUOTN.QUOTN_CLS_RSN_CD as QUOTN_CLS_RSN_CD, INSRNC_QUOTN.QUOTN_ORIGN_CD as QUOTN_ORIGN_CD, INSRNC_QUOTN.APLCTN_ID as APLCTN_ID, INSRNC_QUOTN.QUOTN_OPN_DTTM as QUOTN_OPN_DTTM, INSRNC_QUOTN.QUOTN_CLS_DTTM as QUOTN_CLS_DTTM, INSRNC_QUOTN.QUOTN_PLND_AGMT_OPN_DTTM as QUOTN_PLND_AGMT_OPN_DTTM, INSRNC_QUOTN.QUOTN_PLND_AGMT_CLS_DTTM as QUOTN_PLND_AGMT_CLS_DTTM, INSRNC_QUOTN.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, INSRNC_QUOTN.RTD_DTTM as RTD_DTTM, INSRNC_QUOTN.QUOTN_UPDT_DTTM as QUOTN_UPDT_DTTM, INSRNC_QUOTN.QUOTN_SLCTD_IND as QUOTN_SLCTD_IND, INSRNC_QUOTN.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRNC_QUOTN.EDW_END_DTTM as EDW_END_DTTM, INSRNC_QUOTN.RTD_INSRNC_SCR_VAL as RTD_INSRNC_SCR_VAL, INSRNC_QUOTN.TRANS_STRT_DTTM as TRANS_STRT_DTTM, INSRNC_QUOTN.CNTNUS_SRVC_DT as CNTNUS_SRVC_DT, INSRNC_QUOTN.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, INSRNC_QUOTN.PRIOR_CLM_FREE_IND as PRIOR_CLM_FREE_IND, INSRNC_QUOTN.RTD_INSRNC_SCR_DTTM as RTD_INSRNC_SCR_DTTM, INSRNC_QUOTN.RETN_SCR_VAL as RETN_SCR_VAL, INSRNC_QUOTN.NK_JOB_NBR as NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR as VERS_NBR, INSRNC_QUOTN.SRC_SYS_CD as SRC_SYS_CD FROM DB_T_PROD_CORE.INSRNC_QUOTN QUALIFY	ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR,
		INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  
ORDER	BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_default_values.JobNumber AND LKP.VERS_NBR = exp_default_values.branchnumber AND LKP.SRC_SYS_CD = exp_default_values.in_SYS_SRC_CD
QUALIFY RNK = 1
);


-- Component exp_compare_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare_data AS
(
SELECT
LKP_INSRNC_QUOTN.QUOTN_ID as lkp_QUOTN_ID,
LKP_INSRNC_QUOTN.NK_JOB_NBR as lkp_NK_JOB_NBR,
LKP_INSRNC_QUOTN.VERS_NBR as lkp_VERS_NBR,
LKP_INSRNC_QUOTN.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_INSRNC_QUOTN.SRC_SYS_CD as lkp_SYS_SRC_CD,
exp_default_values.TotalPremiumRPT as in_TotalPremiumRPT,
MD5 ( LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.PREM_AMT ) ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.QUOTN_SBTYPE_CD ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.QUOTN_STS_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.QUOTN_CLS_RSN_CD ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.QUOTN_ORIGN_CD ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.APLCTN_ID ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.QUOTN_OPN_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.QUOTN_CLS_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.QUOTN_PLND_AGMT_OPN_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.QUOTN_PLND_AGMT_CLS_DTTM ) ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.AGMT_OBJTV_TYPE_CD ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.RTD_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.QUOTN_UPDT_DTTM ) ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.QUOTN_SLCTD_IND ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.RTD_INSRNC_SCR_VAL ) ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.CNTNUS_SRVC_DT ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.DSCNT_AMT ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.SRC_OF_BUSN_CD ) ) || LTRIM ( RTRIM ( LKP_INSRNC_QUOTN.PRIOR_CLM_FREE_IND ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.RTD_INSRNC_SCR_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_INSRNC_QUOTN.RETN_SCR_VAL ) ) ) as v_lkp_checksum,
exp_default_values.o_quotn_sbtype_cd as in_quotn_sbtype_cd,
exp_default_values.out_quotn_sts_type_cd as in_quotn_sts_type_cd,
exp_default_values.out_quotn_cls_rsn_cd as in_quotn_cls_rsn_cd,
exp_default_values.out_quotn_orign_cd as in_quotn_orign_cd,
LKP_APLCTN.APLCTN_ID as in_APLCTN_ID,
exp_default_values.createtime as createtime,
exp_default_values.closedate as in_closedate,
exp_default_values.o_Editeffectivedate as in_Editeffectivedate,
exp_default_values.o_periodend as in_periodend,
exp_default_values.agmt_objtv_type_cd as in_agmt_objtv_type_cd,
exp_default_values.JobNumber as in_JobNumber,
exp_default_values.branchnumber as in_branchnumber,
exp_default_values.o_RateAsOfDate as in_RateAsOfDate,
exp_default_values.UpdateTime as in_UpdateTime,
exp_default_values.prcs_id as in_prcs_id,
exp_default_values.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_default_values.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_default_values.EDW_END_DTTM as in_EDW_END_DTTM,
exp_default_values.Retired as Retired,
LKP_INSRNC_QUOTN.TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
LKP_INSRNC_QUOTN.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_default_values.in_Tier_Type_cd as in_Tier_Type_cd,
exp_default_values.SelectVesionOfQuote as in_SelectVesionOfQuote,
exp_default_values.RTD_INSRNC_SCR_VAL as in_RTD_INSRNC_SCR_VAL,
exp_default_values.ContinuousServiceDate_alfa as ContinuousServiceDate_alfa,
exp_default_values.out_SRC_OF_BUSN_CD as out_SRC_OF_BUSN_CD,
exp_default_values.TotalPremAdjRPT_alfa as TotalPremAdjRPT_alfa,
MD5 ( LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.TotalPremiumRPT ) ) ) || LTRIM ( RTRIM ( exp_default_values.o_quotn_sbtype_cd ) ) || LTRIM ( RTRIM ( exp_default_values.out_quotn_sts_type_cd ) ) || LTRIM ( RTRIM ( exp_default_values.out_quotn_cls_rsn_cd ) ) || LTRIM ( RTRIM ( exp_default_values.out_quotn_orign_cd ) ) || LTRIM ( RTRIM ( LKP_APLCTN.APLCTN_ID ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.createtime ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.closedate ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.o_Editeffectivedate ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.o_periodend ) ) ) || LTRIM ( RTRIM ( exp_default_values.agmt_objtv_type_cd ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.o_RateAsOfDate ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.UpdateTime ) ) ) || LTRIM ( RTRIM ( exp_default_values.SelectVesionOfQuote ) || LTRIM ( RTRIM ( exp_default_values.RTD_INSRNC_SCR_VAL ) ) ) || LTRIM ( RTRIM ( exp_default_values.ContinuousServiceDate_alfa ) ) || LTRIM ( RTRIM ( exp_default_values.TotalPremAdjRPT_alfa ) ) || LTRIM ( RTRIM ( exp_default_values.out_SRC_OF_BUSN_CD ) ) || LTRIM ( RTRIM ( exp_default_values.ClaimsFreeInd_alfa ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.OverrideCreditScoreDate_alfa ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_default_values.RetentionScore_alfa ) ) ) as v_in_checksum,
CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_checksum != v_in_checksum THEN ''U'' ELSE ''R'' END END as calc_ins_upd,
exp_default_values.ClaimsFreeInd_alfa as ClaimsFreeInd_alfa,
exp_default_values.prevInsurance_alfa as prevInsurance_alfa,
exp_default_values.AutoLatePayBillingPeriodicity as AutoLatePayBillingPeriodicity,
exp_default_values.updatetime_trans as updatetime_trans,
exp_default_values.PeriodEnd_busn as PeriodEnd_busn,
exp_default_values.rnk as rnk,
exp_default_values.SYS_SRC_CD as SYS_SRC_CD,
NULL as in_ParentPolicy,
exp_default_values.OverrideCreditScoreDate_alfa as in_OverrideCreditScoreDate_alfa,
exp_default_values.RetentionScore_alfa as in_RetentionScore_alfa,
exp_default_values.source_record_id
FROM
exp_default_values
INNER JOIN LKP_APLCTN ON exp_default_values.source_record_id = LKP_APLCTN.source_record_id
INNER JOIN LKP_INSRNC_QUOTN ON LKP_APLCTN.source_record_id = LKP_INSRNC_QUOTN.source_record_id
);


-- Component rtr_ins_upd_condition_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_condition_Retired AS
(SELECT
exp_compare_data.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_compare_data.lkp_NK_JOB_NBR as lkp_NK_JOB_NBR,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.lkp_SYS_SRC_CD as lkp_SYS_SRC_CD,
exp_compare_data.in_TotalPremiumRPT as in_TotalPremiumRPT,
exp_compare_data.in_quotn_sbtype_cd as in_quotn_sbtype_cd,
exp_compare_data.in_quotn_sts_type_cd as in_quotn_sts_type_cd,
exp_compare_data.in_quotn_cls_rsn_cd as in_quotn_cls_rsn_cd,
exp_compare_data.in_quotn_orign_cd as in_quotn_orign_cd,
exp_compare_data.in_APLCTN_ID as in_APLCTN_ID,
exp_compare_data.createtime as createtime,
exp_compare_data.in_closedate as in_closedate,
exp_compare_data.in_Editeffectivedate as in_Editeffectivedate,
exp_compare_data.in_periodend as in_periodend,
exp_compare_data.in_agmt_objtv_type_cd as in_agmt_objtv_type_cd,
exp_compare_data.in_JobNumber as in_JobNumber,
exp_compare_data.in_branchnumber as in_branchnumber,
exp_compare_data.in_RateAsOfDate as in_RateAsOfDate,
exp_compare_data.in_UpdateTime as in_UpdateTime,
exp_compare_data.in_prcs_id as in_prcs_id,
exp_compare_data.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.lkp_VERS_NBR as lkp_VERS_NBR,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.in_Tier_Type_cd as in_Tier_Type_cd,
exp_compare_data.in_SelectVesionOfQuote as in_SelectVesionOfQuote,
exp_compare_data.in_RTD_INSRNC_SCR_VAL as in_RTD_INSRNC_SCR_VAL,
exp_compare_data.ContinuousServiceDate_alfa as ContinuousServiceDate_alfa,
exp_compare_data.ClaimsFreeInd_alfa as ClaimsFreeInd_alfa,
exp_compare_data.prevInsurance_alfa as prevInsurance_alfa,
exp_compare_data.AutoLatePayBillingPeriodicity as AutoLatePayBillingPeriodicity,
exp_compare_data.TotalPremAdjRPT_alfa as TotalPremAdjRPT_alfa,
exp_compare_data.updatetime_trans as updatetime_trans,
exp_compare_data.PeriodEnd_busn as PeriodEnd_busn,
exp_compare_data.rnk as rnk,
exp_compare_data.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_compare_data.out_SRC_OF_BUSN_CD as out_SRC_OF_BUSN_CD,
exp_compare_data.SYS_SRC_CD as SYS_SRC_CD,
exp_compare_data.in_ParentPolicy as in_ParentPolicy,
exp_compare_data.in_OverrideCreditScoreDate_alfa as in_OverrideCreditScoreDate_alfa,
exp_compare_data.in_RetentionScore_alfa as RetentionScore_alfa,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE exp_compare_data.calc_ins_upd = ''R'' and exp_compare_data.Retired != 0 and exp_compare_data.lkp_EDW_END_DTTM = TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_ins_upd_condition_insert, Type ROUTER Output Group insert
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_condition_insert AS
(SELECT
exp_compare_data.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_compare_data.lkp_NK_JOB_NBR as lkp_NK_JOB_NBR,
exp_compare_data.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_compare_data.lkp_SYS_SRC_CD as lkp_SYS_SRC_CD,
exp_compare_data.in_TotalPremiumRPT as in_TotalPremiumRPT,
exp_compare_data.in_quotn_sbtype_cd as in_quotn_sbtype_cd,
exp_compare_data.in_quotn_sts_type_cd as in_quotn_sts_type_cd,
exp_compare_data.in_quotn_cls_rsn_cd as in_quotn_cls_rsn_cd,
exp_compare_data.in_quotn_orign_cd as in_quotn_orign_cd,
exp_compare_data.in_APLCTN_ID as in_APLCTN_ID,
exp_compare_data.createtime as createtime,
exp_compare_data.in_closedate as in_closedate,
exp_compare_data.in_Editeffectivedate as in_Editeffectivedate,
exp_compare_data.in_periodend as in_periodend,
exp_compare_data.in_agmt_objtv_type_cd as in_agmt_objtv_type_cd,
exp_compare_data.in_JobNumber as in_JobNumber,
exp_compare_data.in_branchnumber as in_branchnumber,
exp_compare_data.in_RateAsOfDate as in_RateAsOfDate,
exp_compare_data.in_UpdateTime as in_UpdateTime,
exp_compare_data.in_prcs_id as in_prcs_id,
exp_compare_data.in_SYS_SRC_CD as in_SYS_SRC_CD,
exp_compare_data.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_compare_data.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_compare_data.calc_ins_upd as calc_ins_upd,
exp_compare_data.lkp_VERS_NBR as lkp_VERS_NBR,
exp_compare_data.Retired as Retired,
exp_compare_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_compare_data.in_Tier_Type_cd as in_Tier_Type_cd,
exp_compare_data.in_SelectVesionOfQuote as in_SelectVesionOfQuote,
exp_compare_data.in_RTD_INSRNC_SCR_VAL as in_RTD_INSRNC_SCR_VAL,
exp_compare_data.ContinuousServiceDate_alfa as ContinuousServiceDate_alfa,
exp_compare_data.ClaimsFreeInd_alfa as ClaimsFreeInd_alfa,
exp_compare_data.prevInsurance_alfa as prevInsurance_alfa,
exp_compare_data.AutoLatePayBillingPeriodicity as AutoLatePayBillingPeriodicity,
exp_compare_data.TotalPremAdjRPT_alfa as TotalPremAdjRPT_alfa,
exp_compare_data.updatetime_trans as updatetime_trans,
exp_compare_data.PeriodEnd_busn as PeriodEnd_busn,
exp_compare_data.rnk as rnk,
exp_compare_data.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_compare_data.out_SRC_OF_BUSN_CD as out_SRC_OF_BUSN_CD,
exp_compare_data.SYS_SRC_CD as SYS_SRC_CD,
exp_compare_data.in_ParentPolicy as in_ParentPolicy,
exp_compare_data.in_OverrideCreditScoreDate_alfa as in_OverrideCreditScoreDate_alfa,
exp_compare_data.in_RetentionScore_alfa as RetentionScore_alfa,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE ( exp_compare_data.calc_ins_upd = ''I'' or ( exp_compare_data.calc_ins_upd = ''U'' and exp_compare_data.updatetime_trans > exp_compare_data.lkp_TRANS_STRT_DTTM ) ) OR ( exp_compare_data.Retired = 0 AND exp_compare_data.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component upd_INSRNC_QUOTN_update_Retire_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_INSRNC_QUOTN_update_Retire_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_condition_Retired.lkp_QUOTN_ID as lkp_QUOTN_ID3,
rtr_ins_upd_condition_Retired.lkp_NK_JOB_NBR as lkp_NK_JOB_NBR3,
rtr_ins_upd_condition_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_condition_Retired.lkp_SYS_SRC_CD as lkp_SYS_SRC_CD3,
NULL as in_JobNumber3,
NULL as in_SYS_SRC_CD3,
NULL as in_EDW_STRT_DTTM3,
rtr_ins_upd_condition_Retired.lkp_VERS_NBR as lkp_VERS_NBR3,
NULL as Retired3,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_condition_Retired.source_record_id
FROM
rtr_ins_upd_condition_Retired
);


-- Component exp_INSRNC_QUOTN_update_Retire_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_INSRNC_QUOTN_update_Retire_rejected AS
(
SELECT
upd_INSRNC_QUOTN_update_Retire_rejected.lkp_QUOTN_ID3 as lkp_QUOTN_ID3,
upd_INSRNC_QUOTN_update_Retire_rejected.lkp_NK_JOB_NBR3 as lkp_NK_JOB_NBR3,
upd_INSRNC_QUOTN_update_Retire_rejected.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_INSRNC_QUOTN_update_Retire_rejected.lkp_SYS_SRC_CD3 as lkp_SYS_SRC_CD3,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_INSRNC_QUOTN_update_Retire_rejected.lkp_VERS_NBR3 as lkp_VERS_NBR3,
upd_INSRNC_QUOTN_update_Retire_rejected.source_record_id
FROM
upd_INSRNC_QUOTN_update_Retire_rejected
);


-- Component upd_INSRNC_QUOTN_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_INSRNC_QUOTN_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_condition_insert.lkp_QUOTN_ID as lkp_QUOTN_ID1,
rtr_ins_upd_condition_insert.in_TotalPremiumRPT as in_TotalPremiumRPT1,
rtr_ins_upd_condition_insert.in_quotn_sbtype_cd as in_quotn_sbtype_cd1,
rtr_ins_upd_condition_insert.in_quotn_sts_type_cd as in_quotn_sts_type_cd1,
rtr_ins_upd_condition_insert.in_quotn_cls_rsn_cd as in_quotn_cls_rsn_cd1,
rtr_ins_upd_condition_insert.in_quotn_orign_cd as in_quotn_orign_cd1,
rtr_ins_upd_condition_insert.in_APLCTN_ID as in_APLCTN_ID1,
rtr_ins_upd_condition_insert.createtime as createtime,
rtr_ins_upd_condition_insert.in_closedate as in_closedate1,
rtr_ins_upd_condition_insert.in_Editeffectivedate as in_Editeffectivedate1,
rtr_ins_upd_condition_insert.in_periodend as in_periodend1,
rtr_ins_upd_condition_insert.in_agmt_objtv_type_cd as in_agmt_objtv_type_cd1,
rtr_ins_upd_condition_insert.in_JobNumber as in_JobNumber1,
rtr_ins_upd_condition_insert.in_branchnumber as in_branchnumber1,
rtr_ins_upd_condition_insert.in_RateAsOfDate as in_RateAsOfDate1,
rtr_ins_upd_condition_insert.in_UpdateTime as in_UpdateTime1,
rtr_ins_upd_condition_insert.in_prcs_id as in_prcs_id1,
rtr_ins_upd_condition_insert.in_SYS_SRC_CD as in_SYS_SRC_CD1,
rtr_ins_upd_condition_insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_ins_upd_condition_insert.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_ins_upd_condition_insert.Retired as Retired1,
rtr_ins_upd_condition_insert.in_Tier_Type_cd as in_Tier_Type_cd,
rtr_ins_upd_condition_insert.in_SelectVesionOfQuote as in_SelectVesionOfQuote1,
rtr_ins_upd_condition_insert.in_RTD_INSRNC_SCR_VAL as in_RTD_INSRNC_SCR_VAL1,
rtr_ins_upd_condition_insert.ContinuousServiceDate_alfa as ContinuousServiceDate_alfa1,
rtr_ins_upd_condition_insert.ClaimsFreeInd_alfa as ClaimsFreeInd_alfa1,
rtr_ins_upd_condition_insert.prevInsurance_alfa as prevInsurance_alfa1,
rtr_ins_upd_condition_insert.AutoLatePayBillingPeriodicity as AutoLatePayBillingPeriodicity1,
rtr_ins_upd_condition_insert.TotalPremAdjRPT_alfa as TotalPremAdjRPT_alfa1,
rtr_ins_upd_condition_insert.updatetime_trans as updatetime_trans,
rtr_ins_upd_condition_insert.PeriodEnd_busn as PeriodEnd_busn1,
rtr_ins_upd_condition_insert.rnk as rnk1,
rtr_ins_upd_condition_insert.out_SRC_OF_BUSN_CD as out_SRC_OF_BUSN_CD1,
rtr_ins_upd_condition_insert.SYS_SRC_CD as SYS_SRC_CD1,
rtr_ins_upd_condition_insert.in_ParentPolicy as in_ParentPolicy1,
rtr_ins_upd_condition_insert.in_OverrideCreditScoreDate_alfa as in_OverrideCreditScoreDate_alfa1,
rtr_ins_upd_condition_insert.RetentionScore_alfa as RetentionScore_alfa1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_condition_insert.SOURCE_RECORD_ID
FROM
rtr_ins_upd_condition_insert
);


-- Component exp_INSRNC_QUOTN_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_INSRNC_QUOTN_insert AS
(
SELECT
LKP_1.APLCTN_ID /* replaced lookup LKP_XREF_APLCNT */ as var_QUOTN_ID_xref,
CASE WHEN upd_INSRNC_QUOTN_insert.lkp_QUOTN_ID1 > 0 THEN upd_INSRNC_QUOTN_insert.lkp_QUOTN_ID1 ELSE CASE WHEN upd_INSRNC_QUOTN_insert.in_JobNumber1 = LAG(upd_INSRNC_QUOTN_insert.in_JobNumber1) OVER (partition by upd_INSRNC_QUOTN_insert.source_record_id order by upd_INSRNC_QUOTN_insert.source_record_id) --:var_prev_jobnumber1 
and upd_INSRNC_QUOTN_insert.in_branchnumber1 =  LAG(upd_INSRNC_QUOTN_insert.in_branchnumber1) OVER (partition by upd_INSRNC_QUOTN_insert.source_record_id order by upd_INSRNC_QUOTN_insert.source_record_id) 
--:var_prev_branchnumber1 
THEN 
LAG(CASE WHEN upd_INSRNC_QUOTN_insert.lkp_QUOTN_ID1 > 0 THEN upd_INSRNC_QUOTN_insert.lkp_QUOTN_ID1 ELSE var_QUOTN_ID_xref END) OVER (partition by upd_INSRNC_QUOTN_insert.source_record_id order by upd_INSRNC_QUOTN_insert.source_record_id)
--:var_prev_quotn_id 
ELSE var_QUOTN_ID_xref END END as var_New_quotn_id,
var_New_quotn_id as OUT_QUOTN_ID,


upd_INSRNC_QUOTN_insert.in_TotalPremiumRPT1 as in_TotalPremiumRPT1,
upd_INSRNC_QUOTN_insert.in_quotn_sbtype_cd1 as in_quotn_sbtype_cd1,
upd_INSRNC_QUOTN_insert.in_quotn_sts_type_cd1 as in_quotn_sts_type_cd1,
upd_INSRNC_QUOTN_insert.in_quotn_cls_rsn_cd1 as in_quotn_cls_rsn_cd1,
upd_INSRNC_QUOTN_insert.in_quotn_orign_cd1 as in_quotn_orign_cd1,
upd_INSRNC_QUOTN_insert.in_APLCTN_ID1 as in_APLCTN_ID1,
upd_INSRNC_QUOTN_insert.createtime as createtime,
upd_INSRNC_QUOTN_insert.in_closedate1 as in_closedate1,
upd_INSRNC_QUOTN_insert.in_Editeffectivedate1 as in_Editeffectivedate1,
upd_INSRNC_QUOTN_insert.in_periodend1 as in_periodend1,
upd_INSRNC_QUOTN_insert.in_agmt_objtv_type_cd1 as in_agmt_objtv_type_cd1,
upd_INSRNC_QUOTN_insert.in_JobNumber1 as in_JobNumber1,
upd_INSRNC_QUOTN_insert.in_branchnumber1 as in_branchnumber1,
upd_INSRNC_QUOTN_insert.in_RateAsOfDate1 as in_RateAsOfDate1,
upd_INSRNC_QUOTN_insert.in_UpdateTime1 as in_UpdateTime1,
upd_INSRNC_QUOTN_insert.in_prcs_id1 as in_prcs_id1,
upd_INSRNC_QUOTN_insert.in_SYS_SRC_CD1 as in_SYS_SRC_CD1,
DATEADD (
  SECOND,
  2 * (upd_INSRNC_QUOTN_insert.rnk1 - 1),
  CURRENT_TIMESTAMP()
) as out_in_EDW_STRT_DTTM,
CASE WHEN upd_INSRNC_QUOTN_insert.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_INSRNC_QUOTN_insert.in_EDW_END_DTTM1 END as o_EDW_END_DTTM,
upd_INSRNC_QUOTN_insert.in_Tier_Type_cd as in_Tier_Type_cd,
upd_INSRNC_QUOTN_insert.in_SelectVesionOfQuote1 as in_SelectVesionOfQuote1,
upd_INSRNC_QUOTN_insert.in_RTD_INSRNC_SCR_VAL1 as in_RTD_INSRNC_SCR_VAL1,
upd_INSRNC_QUOTN_insert.ContinuousServiceDate_alfa1 as ContinuousServiceDate_alfa1,
upd_INSRNC_QUOTN_insert.ClaimsFreeInd_alfa1 as ClaimsFreeInd_alfa1,
upd_INSRNC_QUOTN_insert.prevInsurance_alfa1 as prevInsurance_alfa1,
upd_INSRNC_QUOTN_insert.AutoLatePayBillingPeriodicity1 as AutoLatePayBillingPeriodicity1,
upd_INSRNC_QUOTN_insert.TotalPremAdjRPT_alfa1 as TotalPremAdjRPT_alfa1,
upd_INSRNC_QUOTN_insert.updatetime_trans as updatetime_trans,
upd_INSRNC_QUOTN_insert.PeriodEnd_busn1 as PeriodEnd_busn1,
upd_INSRNC_QUOTN_insert.in_JobNumber1 as var_prev_jobnumber,
upd_INSRNC_QUOTN_insert.in_branchnumber1 as var_prev_branchnumber,
var_New_quotn_id as var_prev_quotn_id,
upd_INSRNC_QUOTN_insert.out_SRC_OF_BUSN_CD1 as out_SRC_OF_BUSN_CD1,
upd_INSRNC_QUOTN_insert.in_OverrideCreditScoreDate_alfa1 as in_OverrideCreditScoreDate_alfa1,
TO_CHAR ( upd_INSRNC_QUOTN_insert.RetentionScore_alfa1 ) as RETN_SCR_VAL,
upd_INSRNC_QUOTN_insert.source_record_id,
row_number() over (partition by upd_INSRNC_QUOTN_insert.source_record_id order by upd_INSRNC_QUOTN_insert.source_record_id) as RNK
FROM
upd_INSRNC_QUOTN_insert
LEFT JOIN LKP_XREF_APLCNT LKP_1 ON LKP_1.HOST_APLCTN_ID = upd_INSRNC_QUOTN_insert.in_JobNumber1 AND LKP_1.VERS_NBR = upd_INSRNC_QUOTN_insert.in_branchnumber1 AND LKP_1.DIR_TYPE_VAL = ''QUOTN'' AND LKP_1.APLCTN_TYPE_CD = NULL AND LKP_1.SRC_SYS_CD = upd_INSRNC_QUOTN_insert.in_SYS_SRC_CD1
QUALIFY RNK = 1
);


-- Component insrnc_quotn_update_Retire_rejected, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INSRNC_QUOTN
USING exp_INSRNC_QUOTN_update_Retire_rejected ON (INSRNC_QUOTN.QUOTN_ID = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_QUOTN_ID3 AND INSRNC_QUOTN.NK_JOB_NBR = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_NK_JOB_NBR3 AND INSRNC_QUOTN.VERS_NBR = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_VERS_NBR3 AND INSRNC_QUOTN.SRC_SYS_CD = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_SYS_SRC_CD3 AND INSRNC_QUOTN.EDW_STRT_DTTM = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = nvl(exp_INSRNC_QUOTN_update_Retire_rejected.lkp_QUOTN_ID3,-1),
NK_JOB_NBR = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_NK_JOB_NBR3,
VERS_NBR = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_VERS_NBR3,
SRC_SYS_CD = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_SYS_SRC_CD3,
EDW_STRT_DTTM = exp_INSRNC_QUOTN_update_Retire_rejected.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_INSRNC_QUOTN_update_Retire_rejected.EDW_END_DTTM;


-- Component insrnc_quotn_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INSRNC_QUOTN
(
QUOTN_ID,
DSCNT_AMT,
PREM_AMT,
QUOTN_SBTYPE_CD,
QUOTN_STS_TYPE_CD,
QUOTN_CLS_RSN_CD,
QUOTN_ORIGN_CD,
APLCTN_ID,
QUOTN_OPN_DTTM,
QUOTN_EXPN_DT,
QUOTN_CLS_DTTM,
QUOTN_PLND_AGMT_OPN_DTTM,
QUOTN_PLND_AGMT_CLS_DTTM,
AGMT_OBJTV_TYPE_CD,
NK_JOB_NBR,
VERS_NBR,
RTD_DTTM,
QUOTN_UPDT_DTTM,
TIER_TYPE_CD,
QUOTN_SLCTD_IND,
RTD_INSRNC_SCR_VAL,
CNTNUS_SRVC_DT,
PRIOR_CLM_FREE_IND,
PRIOR_INSRNC_IND,
STMT_CYCL_CD,
SRC_SYS_CD,
SRC_OF_BUSN_CD,
RETN_SCR_VAL,
RTD_INSRNC_SCR_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
nvl(exp_INSRNC_QUOTN_insert.OUT_QUOTN_ID,-1) as QUOTN_ID,
exp_INSRNC_QUOTN_insert.TotalPremAdjRPT_alfa1 as DSCNT_AMT,
exp_INSRNC_QUOTN_insert.in_TotalPremiumRPT1 as PREM_AMT,
exp_INSRNC_QUOTN_insert.in_quotn_sbtype_cd1 as QUOTN_SBTYPE_CD,
exp_INSRNC_QUOTN_insert.in_quotn_sts_type_cd1 as QUOTN_STS_TYPE_CD,
exp_INSRNC_QUOTN_insert.in_quotn_cls_rsn_cd1 as QUOTN_CLS_RSN_CD,
exp_INSRNC_QUOTN_insert.in_quotn_orign_cd1 as QUOTN_ORIGN_CD,
exp_INSRNC_QUOTN_insert.in_APLCTN_ID1 as APLCTN_ID,
exp_INSRNC_QUOTN_insert.createtime as QUOTN_OPN_DTTM,
exp_INSRNC_QUOTN_insert.PeriodEnd_busn1 as QUOTN_EXPN_DT,
exp_INSRNC_QUOTN_insert.in_closedate1 as QUOTN_CLS_DTTM,
exp_INSRNC_QUOTN_insert.in_Editeffectivedate1 as QUOTN_PLND_AGMT_OPN_DTTM,
exp_INSRNC_QUOTN_insert.in_periodend1 as QUOTN_PLND_AGMT_CLS_DTTM,
exp_INSRNC_QUOTN_insert.in_agmt_objtv_type_cd1 as AGMT_OBJTV_TYPE_CD,
exp_INSRNC_QUOTN_insert.in_JobNumber1 as NK_JOB_NBR,
exp_INSRNC_QUOTN_insert.in_branchnumber1 as VERS_NBR,
exp_INSRNC_QUOTN_insert.in_RateAsOfDate1 as RTD_DTTM,
exp_INSRNC_QUOTN_insert.in_UpdateTime1 as QUOTN_UPDT_DTTM,
exp_INSRNC_QUOTN_insert.in_Tier_Type_cd as TIER_TYPE_CD,
exp_INSRNC_QUOTN_insert.in_SelectVesionOfQuote1 as QUOTN_SLCTD_IND,
exp_INSRNC_QUOTN_insert.in_RTD_INSRNC_SCR_VAL1 as RTD_INSRNC_SCR_VAL,
exp_INSRNC_QUOTN_insert.ContinuousServiceDate_alfa1 as CNTNUS_SRVC_DT,
exp_INSRNC_QUOTN_insert.ClaimsFreeInd_alfa1 as PRIOR_CLM_FREE_IND,
exp_INSRNC_QUOTN_insert.prevInsurance_alfa1 as PRIOR_INSRNC_IND,
exp_INSRNC_QUOTN_insert.AutoLatePayBillingPeriodicity1 as STMT_CYCL_CD,
exp_INSRNC_QUOTN_insert.in_SYS_SRC_CD1 as SRC_SYS_CD,
exp_INSRNC_QUOTN_insert.out_SRC_OF_BUSN_CD1 as SRC_OF_BUSN_CD,
exp_INSRNC_QUOTN_insert.RETN_SCR_VAL as RETN_SCR_VAL,
exp_INSRNC_QUOTN_insert.in_OverrideCreditScoreDate_alfa1 as RTD_INSRNC_SCR_DTTM,
exp_INSRNC_QUOTN_insert.in_prcs_id1 as PRCS_ID,
exp_INSRNC_QUOTN_insert.out_in_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_INSRNC_QUOTN_insert.o_EDW_END_DTTM as EDW_END_DTTM,
exp_INSRNC_QUOTN_insert.updatetime_trans as TRANS_STRT_DTTM
FROM
exp_INSRNC_QUOTN_insert;


-- Component insrnc_quotn_insert, Type Post SQL 
UPDATE  DB_T_PROD_CORE.INSRNC_QUOTN  FROM  

(

SELECT	distinct NK_JOB_NBR,VERS_NBR,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by NK_JOB_NBR,VERS_NBR ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1, 

max(TRANS_STRT_DTTM) over (partition by NK_JOB_NBR,VERS_NBR ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

FROM DB_T_PROD_CORE.INSRNC_QUOTN 

)  A

set TRANS_END_DTTM=  A.lead, 

EDW_END_DTTM=A.lead1

where  INSRNC_QUOTN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and INSRNC_QUOTN.NK_JOB_NBR=A.NK_JOB_NBR

and INSRNC_QUOTN.VERS_NBR=A.VERS_NBR

and INSRNC_QUOTN.TRANS_STRT_DTTM <>INSRNC_QUOTN.TRANS_END_DTTM

and lead is not null;


END; 
';