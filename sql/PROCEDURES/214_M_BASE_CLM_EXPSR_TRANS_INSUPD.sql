-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_TRANS_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' declare
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;
BEGIN 
set start_dttm  = current_timestamp;
set END_DTTM = current_timestamp;
set prcs_id= 1;
-- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
);


-- Component LKP_TERADATA_ETL_REF_XLAT_COSTCATEGORY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_COSTCATEGORY AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CTGY_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_costcategory.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_COSTTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_COSTTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''COST_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_costtype.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PAYMENTTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PAYMENTTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PMT_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_paymenttype.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_RCVRY_CTGY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_RCVRY_CTGY AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CTGY_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_recoverycategory.TYPECODE'' 

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

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_TRANSACTIONTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_TRANSACTIONTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_EXPSR_TRANS_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_transaction.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_XREF_CLM, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_CLM AS
(
SELECT
CLM_ID,
NK_SRC_KEY,
DIR_CLM_VAL
FROM DB_T_PROD_CORE.DIR_CLM
);


-- Component sq_cc_transaction, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_transaction AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ExposurePublicID,
$2 as ID,
$3 as CreateTime,
$4 as Comments,
$5 as DoesNotErodeReserves,
$6 as Subtype,
$7 as CostType,
$8 as CostCategory,
$9 as paymenttypecd,
$10 as RecoveryCategory,
$11 as User_id_nk,
$12 as SRC_CD,
$13 as Issuedate,
$14 as ScheduledSendDate,
$15 as Retired,
$16 as GLMonth,
$17 as GLYear,
$18 as TreatyCode,
$19 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT 

clm_expsr_nk,transactionid,createtime,comments_stg,DoesnotErodeReserves_stg,transtypecd,costtypecd,

ctgytypecd,paymenttypecd,recoverycatcd,creater_user_id,SRC_CD,Issuedate,ScheduledSendDate,

Retired_stg,GLMONTH_stg,GLYEAR_stg,TreatyCode

FROM 

(

SELECT 

cast(cc_exposure.publicid_stg as varchar(64))AS clm_expsr_nk,

a.id_stg AS transactionid,

case when a.subtype_stg in (2,3) then a.CreateTime_stg when (a.subtype_stg=1 and  cc_check.IssueDate_stg is not null) then cc_check.IssueDate_stg

when (a.subtype_stg=1 and  cc_check.IssueDate_stg is  null) then cc_check.ScheduledSendDate_stg end as createtime ,

a.comments_stg,

case when a.DoesnotErodeReserves_stg=0 then ''F'' when a.DoesnotErodeReserves_stg=1 then ''T'' else null end as DoesnotErodeReserves_stg,

c.typecode_stg AS transtypecd,

d.typecode_stg AS costtypecd,

e.typecode_stg AS ctgytypecd,

f.typecode_stg AS paymenttypecd,

x.typecode_stg AS recoverycatcd,

cast(cc_contact.PublicID_stg as varchar(64)) AS creater_user_id,

''SRC_SYS6'' AS SRC_CD,

cc_check.IssueDate_stg as Issuedate,

cc_check.ScheduledSendDate_stg as ScheduledSendDate,

a.Retired_stg,

CC.GLMONTH_stg,

CC.GLYEAR_stg,

trty.TreatyCode_stg as TreatyCode,

case when cctl_transactionstatus.TYPECODE_stg = ''voided'' and cc.payload_new_stg=''voided_11'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''voided'' and cc.payload_new_stg= ''voided_15'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''transferred'' and cc.payload_new_stg= ''transferred_11''then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''transferred'' and cc.payload_new_stg= ''transferred_13'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg ='' transferred'' and cc.payload_new_stg=''cleared_13'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg=''recoded_11'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg = ''recoded_14'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg=''issued_14'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg= ''cleared_14'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg= ''requested_14'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg= ''voided_14'' then ''N'' 

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg= ''transferred_14'' then ''N'' /* EIM-41121 */
else ''Y'' end as eligible



from

 (

SELECT ExposureID_stg,id_stg,createtime_stg,comments_stg,DoesNotErodeReserves_stg,costtype_stg,CostCategory_stg,PaymentType_stg,CreateUserID_stg,RecoveryCategory_stg,

Retired_stg,CheckID_stg,UpdateTime_stg,subtype_stg,status_stg,ClaimID_stg,cast(Publicid_stg as varchar(64)) as Publicid_stg , Treaty_alfa_stg

FROM DB_T_PROD_STAG.cc_transaction WHERE subtype_stg = 2 



 UNION

SELECT ExposureID_stg,id_stg,createtime_stg,comments_stg,DoesNotErodeReserves_stg,costtype_stg,CostCategory_stg,PaymentType_stg,CreateUserID_stg,RecoveryCategory_stg,

Retired_stg,CheckID_stg,UpdateTime_stg,subtype_stg,status_stg,ClaimID_stg,cast(Publicid_stg as varchar(64)) as Publicid_stg, Treaty_alfa_stg

 FROM DB_T_PROD_STAG.cc_transaction WHERE subtype_stg = 1

 UNION

 

 SELECT ExposureID_stg,id_stg,createtime_stg,comments_stg,DoesNotErodeReserves_stg,costtype_stg,CostCategory_stg,PaymentType_stg,CreateUserID_stg,RecoveryCategory_stg,

Retired_stg,CheckID_stg,UpdateTime_stg,subtype_stg,status_stg,ClaimID_stg,cast(Publicid_stg as varchar(64)) as Publicid_stg,Treaty_alfa_stg



 FROM DB_T_PROD_STAG.cc_transaction WHERE subtype_stg = 3 

 ) a

 JOIN  DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS

 ON A.STATUS_stg= CCTL_TRANSACTIONSTATUS.ID_stg

JOIN (SELECT  CC_CLAIM.* FROM DB_T_PROD_STAG.CC_CLAIM  INNER JOIN DB_T_PROD_STAG.CCTL_CLAIMSTATE ON CC_CLAIM.STATE_stg= CCTL_CLAIMSTATE.ID_stg WHERE CCTL_CLAIMSTATE.NAME_stg <> ''DRAFT'') CC_CLAIM ON CC_CLAIM.ID_stg=A.CLAIMID_stg

JOIN DB_T_PROD_STAG.cc_policy ON cc_claim.PolicyID_stg=cc_policy.ID_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cc_exposure ON cc_exposure.id_stg=a.exposureid_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cc_check ON cc_check.id_stg = a.CheckID_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON a.CreateUserID_stg = cc_user.id_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cc_contact ON cc_user.ContactID_stg = cc_contact.id_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.GL_EventStaging_CC CC ON CC.Publicid_stg=a.Publicid_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cctl_transaction c ON c.ID_stg = a.subtype_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cctl_costtype d ON a.costtype_stg = d.id_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cctl_costcategory e ON a.costcategory_stg = e.id_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cctl_paymenttype f ON a.paymenttype_stg = f.id_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.cctl_recoverycategory x ON a.recoverycategory_stg = x.id_stg

 LEFT OUTER JOIN DB_T_PROD_STAG.ccx_treaty_alfa trty on trty.ID_stg = a.Treaty_alfa_stg

 where cc_exposure.publicid_stg is not null

 and 

((a.UpdateTime_stg >(:start_dttm)

    and a.UpdateTime_stg <= (:end_dttm))

or

(cc_check.UpdateTime_stg >(:start_dttm)

    and cc_check.UpdateTime_stg <= (:end_dttm)))

)trans

where eligible=''Y''
) SRC
)
);


-- Component exp_all_source_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source_data AS
(
SELECT
sq_cc_transaction.ExposurePublicID as ExposurePublicID,
sq_cc_transaction.ID as ID,
sq_cc_transaction.CreateTime as CreateTime,
sq_cc_transaction.Comments as Comments,
sq_cc_transaction.DoesNotErodeReserves as DoesNotErodeReserves,
sq_cc_transaction.Subtype as Subtype,
sq_cc_transaction.CostType as CostType,
sq_cc_transaction.CostCategory as CostCategory,
sq_cc_transaction.paymenttypecd as paymenttypecd,
sq_cc_transaction.RecoveryCategory as RecoveryCategory,
sq_cc_transaction.User_id_nk as User_id_nk,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_SRC_CD,
CASE WHEN sq_cc_transaction.Subtype <> ''Payment'' THEN sq_cc_transaction.CreateTime ELSE CASE WHEN sq_cc_transaction.Issuedate IS NULL THEN sq_cc_transaction.ScheduledSendDate ELSE sq_cc_transaction.Issuedate END END as v_CLM_STRT_DT,
CASE WHEN v_CLM_STRT_DT IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE v_CLM_STRT_DT END as in_CLM_STRT_DT,
sq_cc_transaction.Retired as Retired,
sq_cc_transaction.GLMonth as GLMonth,
sq_cc_transaction.GLYear as GLYear,
sq_cc_transaction.TreatyCode as TreatyCode,
sq_cc_transaction.source_record_id,
row_number() over (partition by sq_cc_transaction.source_record_id order by sq_cc_transaction.source_record_id) as RNK
FROM
sq_cc_transaction
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_cc_transaction.SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_CLM_EXPSR_TRANS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS AS
(
SELECT
LKP.CLM_EXPSR_TRANS_ID,
LKP.CLM_EXPSR_TRANS_SBTYPE_CD,
LKP.CLM_EXPSR_ID,
LKP.EXPSR_COST_TYPE_CD,
LKP.EXPSR_COST_CTGY_TYPE_CD,
LKP.PMT_TYPE_CD,
LKP.CLM_EXPSR_TRANS_DTTM,
LKP.CLM_EXPSR_TRANS_TXT,
LKP.RCVRY_CTGY_TYPE_CD,
LKP.DOES_NOT_ERODE_RSERV_IND,
LKP.CRTD_BY_PRTY_ID,
LKP.NK_CLM_EXPSR_TRANS_ID,
LKP.GL_MTH_NUM,
LKP.GL_YR_NUM,
LKP.TRTY_CD,
LKP.CLM_EXPSR_TRANS_STRT_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID desc,LKP.CLM_EXPSR_TRANS_SBTYPE_CD desc,LKP.CLM_EXPSR_ID desc,LKP.EXPSR_COST_TYPE_CD desc,LKP.EXPSR_COST_CTGY_TYPE_CD desc,LKP.PMT_TYPE_CD desc,LKP.CLM_EXPSR_TRANS_DTTM desc,LKP.CLM_EXPSR_TRANS_TXT desc,LKP.RCVRY_CTGY_TYPE_CD desc,LKP.DOES_NOT_ERODE_RSERV_IND desc,LKP.CRTD_BY_PRTY_ID desc,LKP.NK_CLM_EXPSR_TRANS_ID desc,LKP.GL_MTH_NUM desc,LKP.GL_YR_NUM desc,LKP.TRTY_CD desc,LKP.PRCS_ID desc,LKP.CLM_EXPSR_TRANS_STRT_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT	CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
		CLM_EXPSR_TRANS.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR_TRANS.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
		CLM_EXPSR_TRANS.EXPSR_COST_CTGY_TYPE_CD as EXPSR_COST_CTGY_TYPE_CD,
		CLM_EXPSR_TRANS.PMT_TYPE_CD as PMT_TYPE_CD,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT, CLM_EXPSR_TRANS.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
		CLM_EXPSR_TRANS.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
		CLM_EXPSR_TRANS.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID, 
		CLM_EXPSR_TRANS.GL_MTH_NUM as GL_MTH_NUM, CLM_EXPSR_TRANS.GL_YR_NUM as GL_YR_NUM,
		CLM_EXPSR_TRANS.TRTY_CD as TRTY_CD, CLM_EXPSR_TRANS.PRCS_ID as PRCS_ID,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_STRT_DTTM as CLM_EXPSR_TRANS_STRT_DTTM,
		CLM_EXPSR_TRANS.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR_TRANS.EDW_END_DTTM as EDW_END_DTTM,
		CLM_EXPSR_TRANS.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID 
FROM	DB_T_PROD_CORE.CLM_EXPSR_TRANS
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY  CLM_EXPSR_TRANS.NK_CLM_EXPSR_TRANS_ID  
ORDER BY CLM_EXPSR_TRANS.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_CLM_EXPSR_TRANS_ID = exp_all_source_data.ID
QUALIFY RNK = 1
);


-- Component LKP_CLM_EXPSR_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_ID AS
(
SELECT
LKP.CLM_EXPSR_ID,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_EXPSR_ID desc,LKP.CLMNT_PRTY_ID desc,LKP.CLM_EXPSR_NAME desc,LKP.CLM_EXPSR_RPTD_DTTM desc,LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND desc,LKP.CLM_ID desc,LKP.CVGE_FEAT_ID desc,LKP.INSRBL_INT_ID desc,LKP.PRCS_ID desc,LKP.COTTER_CLM_IND desc,LKP.LOSS_PRTY_TYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.HOLDBACK_IND desc,LKP.HOLDBACK_AMT desc,LKP.HOLDBACK_REIMBURSED_IND desc,LKP.ROOF_RPLACEMT_IND desc,LKP.CLM_EXPSR_TYPE_CD desc,LKP.CLM_EXPSR_STRT_DTTM desc,LKP.CLM_EXPSR_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT CLM_EXPSR.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR.CLMNT_PRTY_ID as CLMNT_PRTY_ID, CLM_EXPSR.CLM_EXPSR_NAME as CLM_EXPSR_NAME, CLM_EXPSR.CLM_EXPSR_RPTD_DTTM as CLM_EXPSR_RPTD_DTTM, CLM_EXPSR.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND, CLM_EXPSR.CLM_ID as CLM_ID, CLM_EXPSR.CVGE_FEAT_ID as CVGE_FEAT_ID, CLM_EXPSR.INSRBL_INT_ID as INSRBL_INT_ID, CLM_EXPSR.PRCS_ID as PRCS_ID, CLM_EXPSR.COTTER_CLM_IND as COTTER_CLM_IND, CLM_EXPSR.LOSS_PRTY_TYPE_CD as LOSS_PRTY_TYPE_CD, CLM_EXPSR.HOLDBACK_IND as HOLDBACK_IND , CLM_EXPSR.HOLDBACK_AMT as HOLDBACK_AMT, CLM_EXPSR.HOLDBACK_REIMBURSED_IND as HOLDBACK_REIMBURSED_IND, CLM_EXPSR.ROOF_RPLACEMT_IND as ROOF_RPLACEMT_IND, CLM_EXPSR.CLM_EXPSR_TYPE_CD AS CLM_EXPSR_TYPE_CD,CLM_EXPSR.CLM_EXPSR_STRT_DTTM as CLM_EXPSR_STRT_DTTM, CLM_EXPSR.CLM_EXPSR_END_DTTM as CLM_EXPSR_END_DTTM, CLM_EXPSR.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR.EDW_END_DTTM as EDW_END_DTTM, CLM_EXPSR.NK_SRC_KEY as NK_SRC_KEY FROM DB_T_PROD_CORE.CLM_EXPSR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_EXPSR.NK_SRC_KEY  ORDER BY CLM_EXPSR.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_SRC_KEY = exp_all_source_data.ExposurePublicID
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_TRTY_CD_TGT_VAL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_TRTY_CD_TGT_VAL AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK1
FROM
exp_all_source_data
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
 DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 
WHERE 
	 TGT_IDNTFTN_NM = ''TRTY_TYPE''
AND SRC_IDNTFTN_NM = ''ccx_treaty_alfa.treatycode''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_all_source_data.TreatyCode
qualify RNK1 = 1
) ;


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_all_source_data.ID as NK_CLM_EXPSR_TRANS_ID,
exp_all_source_data.CreateTime as CLM_EXPSR_TRANS_DTTM,
exp_all_source_data.Comments as CLM_EXPSR_TRANS_TXT,
exp_all_source_data.DoesNotErodeReserves as DOES_NOT_ERODE_RSERV_IND,
exp_all_source_data.User_id_nk as User_id_nk,
LKP_CLM_EXPSR_ID.CLM_EXPSR_ID as out_CLM_EXPSR_ID,
LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as out_CLM_EXPSR_TRANS_ID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_TRANSACTIONTYPE */ as out_CLM_EXPSR_TRANS_SBTYPE_CD,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_TRANSACTIONTYPE */ as var_CLM_EXPSR_TRANS_SBTYPE_CD,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_COSTTYPE */ as out_EXPSR_COST_TYPE_CD,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_COSTTYPE */ as var_EXPSR_COST_TYPE_CD,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_COSTCATEGORY */ as out_EXPSR_COST_CTGY_TYPE,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_COSTCATEGORY */ as var_EXPSR_COST_CTGY_TYPE,
LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RCVRY_CTGY */ as out_RCVRY_CTGY_TYPE_CD,
LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RCVRY_CTGY */ as var_RCVRY_CTGY_TYPE_CD,
LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PAYMENTTYPE */ as out_PMT_TYPE_CD,
LKP_10.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PAYMENTTYPE */ as var_PMT_TYPE_CD,
LKP_11.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ as out_CRTD_BY_PRTY_ID,
LKP_12.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ as var_CRTD_BY_PRTY_ID,
:PRCS_ID as out_PRCS_ID,
exp_all_source_data.in_CLM_STRT_DT as TRANS_STRT_DT,
LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_STRT_DTTM as lkp_CLM_EXPSR_TRANS_STRT_DT,
MD5 ( var_CLM_EXPSR_TRANS_SBTYPE_CD || to_char ( LKP_CLM_EXPSR_ID.CLM_EXPSR_ID ) || var_EXPSR_COST_TYPE_CD || var_EXPSR_COST_CTGY_TYPE || var_PMT_TYPE_CD || to_char ( exp_all_source_data.CreateTime ) || rtrim ( ltrim ( exp_all_source_data.Comments ) ) || var_RCVRY_CTGY_TYPE_CD || rtrim ( ltrim ( exp_all_source_data.DoesNotErodeReserves ) ) || to_char ( var_CRTD_BY_PRTY_ID ) || rtrim ( ltrim ( exp_all_source_data.GLMonth ) ) || rtrim ( ltrim ( exp_all_source_data.GLYear ) ) || rtrim ( ltrim ( LKP_TERADATA_ETL_REF_XLAT_TRTY_CD_TGT_VAL.TGT_IDNTFTN_VAL ) ) ) as chksum_input,
var_CLM_EXPSR_TRANS_SBTYPE_CD || to_char ( LKP_CLM_EXPSR_ID.CLM_EXPSR_ID ) || var_EXPSR_COST_TYPE_CD || var_EXPSR_COST_CTGY_TYPE || var_PMT_TYPE_CD || to_char ( exp_all_source_data.CreateTime ) || exp_all_source_data.Comments || var_RCVRY_CTGY_TYPE_CD || to_char ( exp_all_source_data.DoesNotErodeReserves ) || to_char ( var_CRTD_BY_PRTY_ID ) as chksum_input_string,
MD5 ( LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_SBTYPE_CD || to_char ( LKP_CLM_EXPSR_TRANS.CLM_EXPSR_ID ) || LKP_CLM_EXPSR_TRANS.EXPSR_COST_TYPE_CD || LKP_CLM_EXPSR_TRANS.EXPSR_COST_CTGY_TYPE_CD || LKP_CLM_EXPSR_TRANS.PMT_TYPE_CD || to_char ( LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_DTTM ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_TXT ) ) || LKP_CLM_EXPSR_TRANS.RCVRY_CTGY_TYPE_CD || rtrim ( ltrim ( LKP_CLM_EXPSR_TRANS.DOES_NOT_ERODE_RSERV_IND ) ) || to_char ( LKP_CLM_EXPSR_TRANS.CRTD_BY_PRTY_ID ) || rtrim ( ltrim ( LKP_CLM_EXPSR_TRANS.GL_MTH_NUM ) ) || rtrim ( ltrim ( LKP_CLM_EXPSR_TRANS.GL_YR_NUM ) ) || rtrim ( ltrim ( LKP_CLM_EXPSR_TRANS.TRTY_CD ) ) ) as chksum_lkp,
LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_SBTYPE_CD || to_char ( LKP_CLM_EXPSR_TRANS.CLM_EXPSR_ID ) || LKP_CLM_EXPSR_TRANS.EXPSR_COST_TYPE_CD || LKP_CLM_EXPSR_TRANS.EXPSR_COST_CTGY_TYPE_CD || LKP_CLM_EXPSR_TRANS.PMT_TYPE_CD || to_char ( LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_DTTM ) || LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_TXT || LKP_CLM_EXPSR_TRANS.RCVRY_CTGY_TYPE_CD || to_char ( LKP_CLM_EXPSR_TRANS.DOES_NOT_ERODE_RSERV_IND ) || to_char ( LKP_CLM_EXPSR_TRANS.CRTD_BY_PRTY_ID ) as chksum_lkp_string,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_input THEN ''U'' ELSE ''R'' END END as o_flag,
LKP_CLM_EXPSR_TRANS.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
LKP_CLM_EXPSR_TRANS.EDW_END_DTTM as lkp_EDW_END_DTTM,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP( ''12/31/9999 23:59:59.999999'' , ''mm/DD/yyyy HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
DATEADD (SECOND, -1, CURRENT_TIMESTAMP()) as EDW_END_DTTM_exp,
exp_all_source_data.o_SRC_CD as o_SRC_CD,
NULL as TRANS_END_DT,
exp_all_source_data.Retired as Retired,
exp_all_source_data.GLMonth as GLMonth,
exp_all_source_data.GLYear as GLYear,
exp_all_source_data.ID as src_NK_CLM_EXPSR_TRANS_ID,
LKP_TERADATA_ETL_REF_XLAT_TRTY_CD_TGT_VAL.TGT_IDNTFTN_VAL as TreatyCode,
exp_all_source_data.source_record_id,
row_number() over (partition by exp_all_source_data.source_record_id order by exp_all_source_data.source_record_id) as RNK1
FROM
exp_all_source_data
INNER JOIN LKP_CLM_EXPSR_TRANS ON exp_all_source_data.source_record_id = LKP_CLM_EXPSR_TRANS.source_record_id
INNER JOIN LKP_CLM_EXPSR_ID ON LKP_CLM_EXPSR_TRANS.source_record_id = LKP_CLM_EXPSR_ID.source_record_id
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_TRTY_CD_TGT_VAL ON LKP_CLM_EXPSR_ID.source_record_id = LKP_TERADATA_ETL_REF_XLAT_TRTY_CD_TGT_VAL.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_TRANSACTIONTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_all_source_data.Subtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_TRANSACTIONTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_all_source_data.Subtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_COSTTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_all_source_data.CostType
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_COSTTYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = exp_all_source_data.CostType
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_COSTCATEGORY LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = exp_all_source_data.CostCategory
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_COSTCATEGORY LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = exp_all_source_data.CostCategory
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_RCVRY_CTGY LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = exp_all_source_data.RecoveryCategory
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_RCVRY_CTGY LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = exp_all_source_data.RecoveryCategory
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PAYMENTTYPE LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = exp_all_source_data.paymenttypecd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PAYMENTTYPE LKP_10 ON LKP_10.SRC_IDNTFTN_VAL = exp_all_source_data.paymenttypecd
LEFT JOIN LKP_INDIV_CLM_CTR LKP_11 ON LKP_11.NK_PUBLC_ID = exp_all_source_data.User_id_nk
LEFT JOIN LKP_INDIV_CLM_CTR LKP_12 ON LKP_12.NK_PUBLC_ID = exp_all_source_data.User_id_nk
qualify RNK1 = 1
);


-- Component rtr_clm_expsr_trans_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_trans_INSERT AS
(
SELECT
exp_data_transformation.out_CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_CLM_EXPSR_ID as CLM_EXPSR_ID,
exp_data_transformation.out_CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
exp_data_transformation.out_EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
exp_data_transformation.out_EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
exp_data_transformation.out_RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
exp_data_transformation.out_PMT_TYPE_CD as PMT_TYPE_CD,
NULL as RCVRY_PRTY_ID,
exp_data_transformation.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
exp_data_transformation.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
exp_data_transformation.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
exp_data_transformation.out_CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
exp_data_transformation.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
exp_data_transformation.o_flag as o_flag,
exp_data_transformation.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_data_transformation.lkp_CLM_EXPSR_TRANS_STRT_DT as LKP_TRANS_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformation.o_SRC_CD as o_SRC_CD,
exp_data_transformation.TRANS_STRT_DT as TRANS_STRT_DT,
exp_data_transformation.TRANS_END_DT as TRANS_END_DT,
exp_data_transformation.Retired as Retired,
exp_data_transformation.GLMonth as GLMonth,
exp_data_transformation.GLYear as GLYear,
exp_data_transformation.src_NK_CLM_EXPSR_TRANS_ID as src_NK_CLM_EXPSR_TRANS_ID,
exp_data_transformation.TreatyCode as TreatyCode,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.out_CLM_EXPSR_ID IS NOT NULL and ( exp_data_transformation.o_flag = ''I'' ) OR ( exp_data_transformation.Retired = 0 AND exp_data_transformation.lkp_EDW_END_DTTM != TO_TIMESTAMP( ''12/31/9999 23:59:59.999999'' , ''mm/DD/yyyy HH24:MI:SS.FF6'' ) ));


-- Component rtr_clm_expsr_trans_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_trans_Retired AS
(
SELECT
exp_data_transformation.out_CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_CLM_EXPSR_ID as CLM_EXPSR_ID,
exp_data_transformation.out_CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
exp_data_transformation.out_EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
exp_data_transformation.out_EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
exp_data_transformation.out_RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
exp_data_transformation.out_PMT_TYPE_CD as PMT_TYPE_CD,
NULL as RCVRY_PRTY_ID,
exp_data_transformation.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
exp_data_transformation.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
exp_data_transformation.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
exp_data_transformation.out_CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
exp_data_transformation.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
exp_data_transformation.o_flag as o_flag,
exp_data_transformation.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_data_transformation.lkp_CLM_EXPSR_TRANS_STRT_DT as LKP_TRANS_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformation.o_SRC_CD as o_SRC_CD,
exp_data_transformation.TRANS_STRT_DT as TRANS_STRT_DT,
exp_data_transformation.TRANS_END_DT as TRANS_END_DT,
exp_data_transformation.Retired as Retired,
exp_data_transformation.GLMonth as GLMonth,
exp_data_transformation.GLYear as GLYear,
exp_data_transformation.src_NK_CLM_EXPSR_TRANS_ID as src_NK_CLM_EXPSR_TRANS_ID,
exp_data_transformation.TreatyCode as TreatyCode,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.o_flag = ''R'' and exp_data_transformation.Retired != 0 and exp_data_transformation.lkp_EDW_END_DTTM = TO_TIMESTAMP( ''12/31/9999 23:59:59.999999'' , ''mm/DD/yyyy HH24:MI:SS.FF6'' ));


-- Component rtr_clm_expsr_trans_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_trans_UPDATE AS
(
SELECT
exp_data_transformation.out_CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_CLM_EXPSR_ID as CLM_EXPSR_ID,
exp_data_transformation.out_CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
exp_data_transformation.out_EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
exp_data_transformation.out_EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
exp_data_transformation.out_RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
exp_data_transformation.out_PMT_TYPE_CD as PMT_TYPE_CD,
NULL as RCVRY_PRTY_ID,
exp_data_transformation.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
exp_data_transformation.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
exp_data_transformation.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
exp_data_transformation.out_CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
exp_data_transformation.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
exp_data_transformation.o_flag as o_flag,
exp_data_transformation.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_data_transformation.lkp_CLM_EXPSR_TRANS_STRT_DT as LKP_TRANS_STRT_DTTM,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformation.o_SRC_CD as o_SRC_CD,
exp_data_transformation.TRANS_STRT_DT as TRANS_STRT_DT,
exp_data_transformation.TRANS_END_DT as TRANS_END_DT,
exp_data_transformation.Retired as Retired,
exp_data_transformation.GLMonth as GLMonth,
exp_data_transformation.GLYear as GLYear,
exp_data_transformation.src_NK_CLM_EXPSR_TRANS_ID as src_NK_CLM_EXPSR_TRANS_ID,
exp_data_transformation.TreatyCode as TreatyCode,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.o_flag = ''U'' AND exp_data_transformation.lkp_EDW_END_DTTM = TO_TIMESTAMP( ''12/31/9999 23:59:59.999999'' , ''mm/DD/yyyy HH24:MI:SS.FF6'' ) AND exp_data_transformation.out_CLM_EXPSR_ID IS NOT NULL 
-- CASE WHEN 
-- exp_data_transformation.out_CLM_EXPSR_TRANS_ID IS NOT NULL 
-- AND 
-- exp_data_transformation.out_CLM_EXPSR_ID IS NOT NULL THEN 
-- TRUE ELSE FALSE 
-- END
);


-- Component updstr_clm_expsr_trans_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_trans_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_INSERT.CLM_EXPSR_ID as CLM_EXPSR_ID,
rtr_clm_expsr_trans_INSERT.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
rtr_clm_expsr_trans_INSERT.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
rtr_clm_expsr_trans_INSERT.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
rtr_clm_expsr_trans_INSERT.PMT_TYPE_CD as PMT_TYPE_CD,
rtr_clm_expsr_trans_INSERT.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
rtr_clm_expsr_trans_INSERT.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
rtr_clm_expsr_trans_INSERT.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
rtr_clm_expsr_trans_INSERT.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
rtr_clm_expsr_trans_INSERT.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
rtr_clm_expsr_trans_INSERT.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
rtr_clm_expsr_trans_INSERT.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_INSERT.PRCS_ID as PRCS_ID,
rtr_clm_expsr_trans_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM2,
rtr_clm_expsr_trans_INSERT.EDW_END_DTTM as EDW_END_DTTM2,
rtr_clm_expsr_trans_INSERT.o_SRC_CD as o_SRC_CD1,
rtr_clm_expsr_trans_INSERT.TRANS_STRT_DT as TRANS_STRT_DT1,
rtr_clm_expsr_trans_INSERT.TRANS_END_DT as TRANS_END_DT1,
rtr_clm_expsr_trans_INSERT.Retired as Retired1,
rtr_clm_expsr_trans_INSERT.GLMonth as GLMonth1,
rtr_clm_expsr_trans_INSERT.GLYear as GLYear1,
rtr_clm_expsr_trans_INSERT.src_NK_CLM_EXPSR_TRANS_ID as src_NK_CLM_EXPSR_TRANS_ID1,
rtr_clm_expsr_trans_INSERT.TreatyCode as TreatyCode1,
0 as UPDATE_STRATEGY_ACTION,
rtr_clm_expsr_trans_INSERT.SOURCE_RECORD_ID
FROM
rtr_clm_expsr_trans_INSERT
);


-- Component updstr_clm_expsr_trans_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_trans_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_SEQ_NUM,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_ID as CLM_EXPSR_ID,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
rtr_clm_expsr_trans_UPDATE.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
rtr_clm_expsr_trans_UPDATE.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
rtr_clm_expsr_trans_UPDATE.PMT_TYPE_CD as PMT_TYPE_CD,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
rtr_clm_expsr_trans_UPDATE.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
rtr_clm_expsr_trans_UPDATE.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
rtr_clm_expsr_trans_UPDATE.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
rtr_clm_expsr_trans_UPDATE.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
rtr_clm_expsr_trans_UPDATE.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_UPDATE.PRCS_ID as PRCS_ID,
rtr_clm_expsr_trans_UPDATE.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_clm_expsr_trans_UPDATE.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_clm_expsr_trans_UPDATE.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM3,
rtr_clm_expsr_trans_UPDATE.o_SRC_CD as o_SRC_CD3,
rtr_clm_expsr_trans_UPDATE.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
rtr_clm_expsr_trans_UPDATE.TRANS_STRT_DT as TRANS_STRT_DT3,
rtr_clm_expsr_trans_UPDATE.TRANS_END_DT as TRANS_END_DT3,
rtr_clm_expsr_trans_UPDATE.Retired as Retired3,
rtr_clm_expsr_trans_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_clm_expsr_trans_UPDATE.source_record_id
FROM
rtr_clm_expsr_trans_UPDATE
);


-- Component fil_retired_existing, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_retired_existing AS
(
SELECT
updstr_clm_expsr_trans_upd.CLM_EXPSR_TRANS_SEQ_NUM as CLM_EXPSR_TRANS_SEQ_NUM,
updstr_clm_expsr_trans_upd.CLM_EXPSR_ID as CLM_EXPSR_ID,
updstr_clm_expsr_trans_upd.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
updstr_clm_expsr_trans_upd.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
updstr_clm_expsr_trans_upd.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
updstr_clm_expsr_trans_upd.PMT_TYPE_CD as PMT_TYPE_CD,
updstr_clm_expsr_trans_upd.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
updstr_clm_expsr_trans_upd.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
updstr_clm_expsr_trans_upd.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
updstr_clm_expsr_trans_upd.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
updstr_clm_expsr_trans_upd.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
updstr_clm_expsr_trans_upd.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
updstr_clm_expsr_trans_upd.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_upd.PRCS_ID as PRCS_ID,
updstr_clm_expsr_trans_upd.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
updstr_clm_expsr_trans_upd.EDW_END_DTTM_exp3 as EDW_END_DTTM_exp3,
updstr_clm_expsr_trans_upd.o_SRC_CD3 as o_SRC_CD3,
updstr_clm_expsr_trans_upd.TRANS_STRT_DT3 as TRANS_STRT_DT3,
updstr_clm_expsr_trans_upd.TRANS_END_DT3 as TRANS_END_DT3,
updstr_clm_expsr_trans_upd.Retired3 as Retired3,
updstr_clm_expsr_trans_upd.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
updstr_clm_expsr_trans_upd.LKP_TRANS_STRT_DTTM3 as LKP_TRANS_STRT_DTTM3,
updstr_clm_expsr_trans_upd.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
updstr_clm_expsr_trans_upd.source_record_id
FROM
updstr_clm_expsr_trans_upd
WHERE updstr_clm_expsr_trans_upd.lkp_EDW_END_DTTM3 = TO_TIMESTAMP (
  ''12/31/9999 23:59:59.999999'',
  ''mm/DD/yyyy hh24:mi:ss.ff6'')
);


-- Component updstr_clm_expsr_trans_upd_retired_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_trans_upd_retired_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_Retired.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID4,
rtr_clm_expsr_trans_Retired.CLM_EXPSR_ID as CLM_EXPSR_ID4,
rtr_clm_expsr_trans_Retired.PRCS_ID as PRCS_ID4,
rtr_clm_expsr_trans_Retired.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM4,
rtr_clm_expsr_trans_Retired.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_clm_expsr_trans_Retired.SOURCE_RECORD_ID
FROM
rtr_clm_expsr_trans_Retired
);


-- Component updstr_clm_expsr_trans_insupd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_trans_insupd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_ID as CLM_EXPSR_ID,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
rtr_clm_expsr_trans_UPDATE.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
rtr_clm_expsr_trans_UPDATE.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
rtr_clm_expsr_trans_UPDATE.PMT_TYPE_CD as PMT_TYPE_CD,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
rtr_clm_expsr_trans_UPDATE.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
rtr_clm_expsr_trans_UPDATE.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
rtr_clm_expsr_trans_UPDATE.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
rtr_clm_expsr_trans_UPDATE.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
rtr_clm_expsr_trans_UPDATE.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
rtr_clm_expsr_trans_UPDATE.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_UPDATE.PRCS_ID as PRCS_ID,
rtr_clm_expsr_trans_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM2,
rtr_clm_expsr_trans_UPDATE.EDW_END_DTTM as EDW_END_DTTM2,
rtr_clm_expsr_trans_UPDATE.o_SRC_CD as o_SRC_CD3,
rtr_clm_expsr_trans_UPDATE.TRANS_STRT_DT as TRANS_STRT_DT3,
rtr_clm_expsr_trans_UPDATE.TRANS_END_DT as TRANS_END_DT3,
rtr_clm_expsr_trans_UPDATE.Retired as Retired3,
rtr_clm_expsr_trans_UPDATE.GLMonth as GLMonth3,
rtr_clm_expsr_trans_UPDATE.GLYear as GLYear3,
rtr_clm_expsr_trans_UPDATE.TreatyCode as TreatyCode3,
0 as UPDATE_STRATEGY_ACTION,
rtr_clm_expsr_trans_UPDATE.SOURCE_RECORD_ID
FROM
rtr_clm_expsr_trans_UPDATE
);


-- Component fil_retired, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_retired AS
(
SELECT
updstr_clm_expsr_trans_insupd.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_insupd.CLM_EXPSR_ID as CLM_EXPSR_ID,
updstr_clm_expsr_trans_insupd.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
updstr_clm_expsr_trans_insupd.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
updstr_clm_expsr_trans_insupd.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
updstr_clm_expsr_trans_insupd.PMT_TYPE_CD as PMT_TYPE_CD,
updstr_clm_expsr_trans_insupd.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
updstr_clm_expsr_trans_insupd.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
updstr_clm_expsr_trans_insupd.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
updstr_clm_expsr_trans_insupd.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
updstr_clm_expsr_trans_insupd.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
updstr_clm_expsr_trans_insupd.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
updstr_clm_expsr_trans_insupd.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_insupd.PRCS_ID as PRCS_ID,
updstr_clm_expsr_trans_insupd.EDW_STRT_DTTM2 as EDW_STRT_DTTM2,
updstr_clm_expsr_trans_insupd.EDW_END_DTTM2 as EDW_END_DTTM2,
updstr_clm_expsr_trans_insupd.o_SRC_CD3 as o_SRC_CD3,
updstr_clm_expsr_trans_insupd.TRANS_STRT_DT3 as TRANS_STRT_DT3,
updstr_clm_expsr_trans_insupd.Retired3 as Retired3,
updstr_clm_expsr_trans_insupd.GLMonth3 as GLMonth3,
updstr_clm_expsr_trans_insupd.GLYear3 as GLYear3,
updstr_clm_expsr_trans_insupd.TreatyCode3 as TreatyCode3,
updstr_clm_expsr_trans_insupd.source_record_id
FROM
updstr_clm_expsr_trans_insupd
WHERE updstr_clm_expsr_trans_insupd.Retired3 = 0
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
LKP_1.CLM_ID /* replaced lookup LKP_XREF_CLM */ as CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_ins.CLM_EXPSR_ID as CLM_EXPSR_ID,
updstr_clm_expsr_trans_ins.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
updstr_clm_expsr_trans_ins.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
updstr_clm_expsr_trans_ins.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
updstr_clm_expsr_trans_ins.PMT_TYPE_CD as PMT_TYPE_CD,
updstr_clm_expsr_trans_ins.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
updstr_clm_expsr_trans_ins.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
updstr_clm_expsr_trans_ins.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
updstr_clm_expsr_trans_ins.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
updstr_clm_expsr_trans_ins.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
updstr_clm_expsr_trans_ins.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
updstr_clm_expsr_trans_ins.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_ins.PRCS_ID as PRCS_ID,
updstr_clm_expsr_trans_ins.EDW_STRT_DTTM2 as EDW_STRT_DTTM2,
CASE WHEN updstr_clm_expsr_trans_ins.Retired1 != 0 THEN updstr_clm_expsr_trans_ins.EDW_STRT_DTTM2 ELSE updstr_clm_expsr_trans_ins.EDW_END_DTTM2 END as o_EDW_END_DTTM2,
updstr_clm_expsr_trans_ins.o_SRC_CD1 as o_SRC_CD1,
updstr_clm_expsr_trans_ins.TRANS_STRT_DT1 as TRANS_STRT_DT1,
CASE WHEN updstr_clm_expsr_trans_ins.Retired1 != 0 THEN updstr_clm_expsr_trans_ins.TRANS_STRT_DT1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DT1,
updstr_clm_expsr_trans_ins.GLMonth1 as GLMonth1,
updstr_clm_expsr_trans_ins.GLYear1 as GLYear1,
updstr_clm_expsr_trans_ins.TreatyCode1 as TreatyCode1,
updstr_clm_expsr_trans_ins.source_record_id,
row_number() over (partition by updstr_clm_expsr_trans_ins.source_record_id order by updstr_clm_expsr_trans_ins.source_record_id) as RNK
FROM
updstr_clm_expsr_trans_ins
LEFT JOIN LKP_XREF_CLM LKP_1 ON LKP_1.NK_SRC_KEY = RTRIM ( LTRIM ( updstr_clm_expsr_trans_ins.src_NK_CLM_EXPSR_TRANS_ID1 ) ) AND LKP_1.DIR_CLM_VAL = ''CLMEXPSRTRANS''
QUALIFY RNK = 1
);


-- Component exp_pass_to_target_upd_retired_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_retired_rejected AS
(
SELECT
updstr_clm_expsr_trans_upd_retired_rejected.CLM_EXPSR_TRANS_ID4 as CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_upd_retired_rejected.CLM_EXPSR_ID4 as CLM_EXPSR_ID,
updstr_clm_expsr_trans_upd_retired_rejected.LKP_EDW_STRT_DTTM4 as LKP_EDW_STRT_DTTM3,
updstr_clm_expsr_trans_upd_retired_rejected.LKP_EDW_STRT_DTTM4 as EDW_END_DTTM_ret,
updstr_clm_expsr_trans_upd_retired_rejected.LKP_TRANS_STRT_DTTM4 as TRANS_END_DTTM_ret1,
updstr_clm_expsr_trans_upd_retired_rejected.source_record_id
FROM
updstr_clm_expsr_trans_upd_retired_rejected
);


-- Component exp_pass_to_target_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd AS
(
SELECT
fil_retired_existing.CLM_EXPSR_TRANS_SEQ_NUM as CLM_EXPSR_TRANS_SEQ_NUM,
fil_retired_existing.CLM_EXPSR_ID as CLM_EXPSR_ID,
fil_retired_existing.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
CASE WHEN fil_retired_existing.Retired3 != 0 THEN fil_retired_existing.LKP_EDW_STRT_DTTM3 ELSE DATEADD (SECOND, -1, fil_retired_existing.EDW_STRT_DTTM3) END as o_EDW_END_DTTM_exp3,
CASE WHEN fil_retired_existing.Retired3 != 0 THEN fil_retired_existing.LKP_TRANS_STRT_DTTM3 ELSE DATEADD (SECOND, -1, fil_retired_existing.TRANS_STRT_DT3) END as out_TRANS_END_DT3,
fil_retired_existing.source_record_id
FROM
fil_retired_existing
);


-- Component tgt_clm_expsr_trans_upd_retired_rejected, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS
USING exp_pass_to_target_upd_retired_rejected ON (CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID = exp_pass_to_target_upd_retired_rejected.CLM_EXPSR_TRANS_ID AND CLM_EXPSR_TRANS.EDW_STRT_DTTM = exp_pass_to_target_upd_retired_rejected.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_EXPSR_TRANS_ID = exp_pass_to_target_upd_retired_rejected.CLM_EXPSR_TRANS_ID,
CLM_EXPSR_ID = exp_pass_to_target_upd_retired_rejected.CLM_EXPSR_ID,
CLM_EXPSR_TRANS_END_DTTM = exp_pass_to_target_upd_retired_rejected.TRANS_END_DTTM_ret1,
EDW_STRT_DTTM = exp_pass_to_target_upd_retired_rejected.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_retired_rejected.EDW_END_DTTM_ret;


-- Component tgt_clm_expsr_trans_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS
(
CLM_EXPSR_TRANS_ID,
CLM_EXPSR_TRANS_SBTYPE_CD,
CLM_EXPSR_ID,
EXPSR_COST_TYPE_CD,
EXPSR_COST_CTGY_TYPE_CD,
PMT_TYPE_CD,
RCVRY_PRTY_ID,
CLM_EXPSR_TRANS_DTTM,
CLM_EXPSR_TRANS_TXT,
RCVRY_CTGY_TYPE_CD,
DOES_NOT_ERODE_RSERV_IND,
CRTD_BY_PRTY_ID,
NK_CLM_EXPSR_TRANS_ID,
GL_MTH_NUM,
GL_YR_NUM,
TRTY_CD,
PRCS_ID,
CLM_EXPSR_TRANS_STRT_DTTM,
CLM_EXPSR_TRANS_END_DTTM,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_ins.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_pass_to_target_ins.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
exp_pass_to_target_ins.CLM_EXPSR_ID as CLM_EXPSR_ID,
exp_pass_to_target_ins.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
exp_pass_to_target_ins.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE_CD,
exp_pass_to_target_ins.PMT_TYPE_CD as PMT_TYPE_CD,
exp_pass_to_target_ins.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
exp_pass_to_target_ins.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
exp_pass_to_target_ins.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
exp_pass_to_target_ins.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
exp_pass_to_target_ins.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
exp_pass_to_target_ins.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
exp_pass_to_target_ins.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
exp_pass_to_target_ins.GLMonth1 as GL_MTH_NUM,
exp_pass_to_target_ins.GLYear1 as GL_YR_NUM,
exp_pass_to_target_ins.TreatyCode1 as TRTY_CD,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.TRANS_STRT_DT1 as CLM_EXPSR_TRANS_STRT_DTTM,
exp_pass_to_target_ins.TRANS_END_DT1 as CLM_EXPSR_TRANS_END_DTTM,
exp_pass_to_target_ins.o_SRC_CD1 as SRC_SYS_CD,
exp_pass_to_target_ins.EDW_STRT_DTTM2 as EDW_STRT_DTTM,
exp_pass_to_target_ins.o_EDW_END_DTTM2 as EDW_END_DTTM
FROM
exp_pass_to_target_ins;


-- Component exp_pass_to_target_insupd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insupd AS
(
SELECT
fil_retired.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
fil_retired.CLM_EXPSR_ID as CLM_EXPSR_ID,
fil_retired.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
fil_retired.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
fil_retired.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE,
fil_retired.PMT_TYPE_CD as PMT_TYPE_CD,
fil_retired.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
fil_retired.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
fil_retired.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
fil_retired.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
fil_retired.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
fil_retired.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
fil_retired.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
fil_retired.PRCS_ID as PRCS_ID,
fil_retired.EDW_STRT_DTTM2 as EDW_STRT_DTTM2,
fil_retired.EDW_END_DTTM2 as EDW_END_DTTM2,
fil_retired.o_SRC_CD3 as o_SRC_CD3,
fil_retired.TRANS_STRT_DT3 as TRANS_STRT_DT3,
TO_TIMESTAMP (
  ''9999-12-31 23:59:59.999999'',
  ''yyyy-mm-dd hh24:mi:ss.ff6''
) as TRANS_END_DT3,
fil_retired.GLMonth3 as GLMonth3,
fil_retired.GLYear3 as GLYear3,
fil_retired.TreatyCode3 as TreatyCode3,
fil_retired.source_record_id
FROM
fil_retired
);


-- Component tgt_clm_expsr_trans_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS
USING exp_pass_to_target_upd ON (CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID = exp_pass_to_target_upd.CLM_EXPSR_TRANS_SEQ_NUM AND CLM_EXPSR_TRANS.EDW_STRT_DTTM = exp_pass_to_target_upd.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_EXPSR_TRANS_ID = exp_pass_to_target_upd.CLM_EXPSR_TRANS_SEQ_NUM,
CLM_EXPSR_ID = exp_pass_to_target_upd.CLM_EXPSR_ID,
CLM_EXPSR_TRANS_END_DTTM = exp_pass_to_target_upd.out_TRANS_END_DT3,
EDW_STRT_DTTM = exp_pass_to_target_upd.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd.o_EDW_END_DTTM_exp3;


-- Component tgt_clm_expsr_trans_insupd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS
(
CLM_EXPSR_TRANS_ID,
CLM_EXPSR_TRANS_SBTYPE_CD,
CLM_EXPSR_ID,
EXPSR_COST_TYPE_CD,
EXPSR_COST_CTGY_TYPE_CD,
PMT_TYPE_CD,
RCVRY_PRTY_ID,
CLM_EXPSR_TRANS_DTTM,
CLM_EXPSR_TRANS_TXT,
RCVRY_CTGY_TYPE_CD,
DOES_NOT_ERODE_RSERV_IND,
CRTD_BY_PRTY_ID,
NK_CLM_EXPSR_TRANS_ID,
GL_MTH_NUM,
GL_YR_NUM,
TRTY_CD,
PRCS_ID,
CLM_EXPSR_TRANS_STRT_DTTM,
CLM_EXPSR_TRANS_END_DTTM,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_insupd.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_pass_to_target_insupd.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
exp_pass_to_target_insupd.CLM_EXPSR_ID as CLM_EXPSR_ID,
exp_pass_to_target_insupd.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
exp_pass_to_target_insupd.EXPSR_COST_CTGY_TYPE as EXPSR_COST_CTGY_TYPE_CD,
exp_pass_to_target_insupd.PMT_TYPE_CD as PMT_TYPE_CD,
exp_pass_to_target_insupd.RCVRY_PRTY_ID as RCVRY_PRTY_ID,
exp_pass_to_target_insupd.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
exp_pass_to_target_insupd.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT,
exp_pass_to_target_insupd.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
exp_pass_to_target_insupd.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
exp_pass_to_target_insupd.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID,
exp_pass_to_target_insupd.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID,
exp_pass_to_target_insupd.GLMonth3 as GL_MTH_NUM,
exp_pass_to_target_insupd.GLYear3 as GL_YR_NUM,
exp_pass_to_target_insupd.TreatyCode3 as TRTY_CD,
exp_pass_to_target_insupd.PRCS_ID as PRCS_ID,
exp_pass_to_target_insupd.TRANS_STRT_DT3 as CLM_EXPSR_TRANS_STRT_DTTM,
exp_pass_to_target_insupd.TRANS_END_DT3 as CLM_EXPSR_TRANS_END_DTTM,
exp_pass_to_target_insupd.o_SRC_CD3 as SRC_SYS_CD,
exp_pass_to_target_insupd.EDW_STRT_DTTM2 as EDW_STRT_DTTM,
exp_pass_to_target_insupd.EDW_END_DTTM2 as EDW_END_DTTM
FROM
exp_pass_to_target_insupd;


END; ';