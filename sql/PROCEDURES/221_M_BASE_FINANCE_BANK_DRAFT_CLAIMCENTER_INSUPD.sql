-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_BANK_DRAFT_CLAIMCENTER_INSUPD("RUN_ID" VARCHAR)
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

-- Component LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL ,row_number() over (order by 1) AS source_record_id

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_CTGY_TYPE''

    		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);
-- Component LKP_TERADATA_ETL_REF_XLAT
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT * from LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE);
-- Component LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_TYPE''

    		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component src_sq_cc_check, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cc_check AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as CheckNumber,
$3 as ClearedDate_alfa,
$4 as transactionamount,
$5 as CreateTime,
$6 as DOC_TYPE,
$7 as DOC_CATEGORY,
$8 as Retired,
$9 as Typecode,
$10 as trans_acct_id,
$11 as RNK,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select * From

(

select publicid, 

checknumber, 

cleareddate_alfa, 

reportableamount, 

createtime,

DOC_TYPE, 

DOC_CATEGORY, 

retired, 

typecode,

trans_acct_id,

Rank()  OVER(PARTITION BY checknumber, DOC_TYPE, DOC_CATEGORY  ORDER BY cleareddate_alfa )  as rnk 

from 

(

select publicid, 

checknumber, 

cleareddate_alfa, 

reportableamount, 

createtime, 

DOC_TYPE, 

DOC_CATEGORY, 

retired,

typecode,

publicid as trans_acct_id

from (

SELECT distinct 

cc_check.PublicID_stg as publicid,

cc_check.CheckNumber_stg as checknumber,

cc_check.ClearedDate_alfa_stg as cleareddate_alfa,

cc_check.ReportableAmount_stg as reportableamount,

cc_check.CreateTime_stg as createtime,

 CAST(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,

CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY, 

cc_check.paymentmethod_stg as paymentmethod,

cctl_paymentmethod.typecode_stg as typecode,

 case when cc_check.Retired_stg=0 and cc_transactionlineitem.Retired_stg =0 then 0  else 1 end retired

FROM

(

	select	cc_claim.id_stg,cc_claim.State_stg from	DB_T_PROD_STAG.cc_claim 

	inner join DB_T_PROD_STAG.cctl_claimstate 

		on cc_claim.State_stg= cctl_claimstate.id_stg 

	where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK 

	on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction 

	on cc_check.id_stg =cc_transaction.checkid_stg

join DB_T_PROD_STAG.cc_transactionlineitem 

	on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS 

	on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod 

	on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg



where	cc_check.UpdateTime_stg>(:START_DTTM) 

	AND cc_check.UpdateTime_stg <= (:END_DTTM)

	and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

)cc

inner join DB_T_PROD_STAG.cctl_paymentmethod on cc.paymentmethod=cctl_paymentmethod.id_stg



UNION 

select  publicid, 

checknumber,

cleareddate_alfa, 

reportableamount, 

createtime, 

DOC_TYPE, 

DOC_CATEGORY, 

retired,

typecode,

publicid as trans_acct_id

from (

SELECT distinct 

CC_CHECK.CombinedCheckNumber_alfa_stg  as publicid,

CC_CHECK.CombinedCheckNumber_alfa_stg as checknumber,

 cc_check.ClearedDate_alfa_stg as cleareddate_alfa,

cast(null as integer) as reportableamount,

cc_check.CreateTime_stg as createtime,

CAST(''DOC_TYPE6'' AS VARCHAR(50)) AS DOC_TYPE,

CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

  case when cc_check.Retired_stg=0 and cc_transactionlineitem.Retired_stg =0 then 0  else 1 end retired,

  cctl_paymentmethod.typecode_stg as typecode,

 cc_check.paymentmethod_stg as paymentmethod,

  CC_CHECK.STATUS_stg as STATUS,

  cc_claim.CLAIMNUMBER_stg as CLAIMNUMBER

FROM(

select	cc_claim.id_stg,cc_claim.State_stg,CLAIMNUMBER_stg from	DB_T_PROD_STAG.cc_claim 

	inner join DB_T_PROD_STAG.cctl_claimstate 

		on cc_claim.State_stg= cctl_claimstate.id_stg 

	where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK 

	on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction 

	on cc_check.id_stg =cc_transaction.checkid_stg

join DB_T_PROD_STAG.cc_transactionlineitem 

	on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS 

	on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod 

	on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

where	cc_check.UpdateTime_stg>(:START_DTTM) 

	AND cc_check.UpdateTime_stg <= (:END_DTTM)

	and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

)

dd

inner join DB_T_PROD_STAG.cctl_paymentmethod on dd.paymentmethod=cctl_paymentmethod.id_stg

inner join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on dd.STATUS=CCTL_TRANSACTIONSTATUS.ID_stg

WHERE CCTL_TRANSACTIONSTATUS.TYPECODE_stg <> ''voided'' and dd.checknumber  IS  NOT NULL AND CLAIMNUMBER IS NOT NULL

 QUALIFY ROW_NUMBER() OVER (PARTITION BY dd.checknumber 

 ORDER BY dd.checknumber,dd.CreateTime)=1

 )a

 )temp
) SRC
)
);


-- Component exp_src_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_pass AS
(
SELECT
src_sq_cc_check.PublicID as RefNumber,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */ as o_doc_Type,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */ as o_doc_category,
src_sq_cc_check.Typecode as Typecode,
src_sq_cc_check.transactionamount as Amount,
src_sq_cc_check.ClearedDate_alfa as ClearedDate_alfa,
CASE WHEN src_sq_cc_check.ClearedDate_alfa IS NULL THEN to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' ) ELSE src_sq_cc_check.ClearedDate_alfa END as out_ClearedDate_alfa,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
src_sq_cc_check.CreateTime as CreateTime,
NULL as UPDATETIME,
src_sq_cc_check.Retired as Retired,
src_sq_cc_check.CheckNumber as CheckNumber,
src_sq_cc_check.RNK as RNK1,
src_sq_cc_check.trans_acct_id as trans_acct_id,
to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' ) as in_updatetime,
src_sq_cc_check.source_record_id,
row_number() over (partition by src_sq_cc_check.source_record_id order by src_sq_cc_check.source_record_id) as RNK
FROM
src_sq_cc_check
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = src_sq_cc_check.DOC_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = src_sq_cc_check.DOC_CATEGORY
QUALIFY RNK = 1
);


-- Component LKP_DOC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DOC AS
(
SELECT
LKP.DOC_ID,
exp_src_pass.RefNumber as RefNumber,
exp_src_pass.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src_pass.source_record_id ORDER BY LKP.DOC_ID asc,LKP.TM_PRD_CD asc,LKP.DOC_CRTN_DTTM asc,LKP.DOC_RECPT_DT asc,LKP.DOC_PRD_STRT_DTTM asc,LKP.DOC_PRD_END_DTTM asc,LKP.DOC_ISSUR_NUM asc,LKP.DATA_SRC_TYPE_CD asc,LKP.DOC_DESC_TXT asc,LKP.DOC_NAME asc,LKP.DOC_HOST_NUM asc,LKP.DOC_HOST_VERS_NUM asc,LKP.DOC_CYCL_CD asc,LKP.DOC_TYPE_CD asc,LKP.MM_OBJT_ID asc,LKP.DOC_CTGY_TYPE_CD asc,LKP.LANG_TYPE_CD asc,LKP.PRCS_ID asc,LKP.DOC_STS_CD asc) RNK1
FROM
exp_src_pass
LEFT JOIN (
SELECT
DOC_ID,
TM_PRD_CD,
DOC_CRTN_DTTM,
DOC_RECPT_DT,
DOC_PRD_STRT_DTTM,
DOC_PRD_END_DTTM,
DOC_ISSUR_NUM,
DATA_SRC_TYPE_CD,
DOC_DESC_TXT,
DOC_NAME,
DOC_HOST_NUM,
DOC_HOST_VERS_NUM,
DOC_CYCL_CD,
DOC_TYPE_CD,
MM_OBJT_ID,
DOC_CTGY_TYPE_CD,
LANG_TYPE_CD,
PRCS_ID,
DOC_STS_CD
FROM DB_T_PROD_CORE.DOC
) LKP ON LKP.DOC_ISSUR_NUM = exp_src_pass.RefNumber AND LKP.DOC_TYPE_CD = exp_src_pass.o_doc_Type AND LKP.DOC_CTGY_TYPE_CD = exp_src_pass.o_doc_category
QUALIFY RNK1 = 1
);


-- Component LKP_BNK_DRFT_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_BNK_DRFT_TGT AS
(
SELECT
LKP.BNK_DRFT_DOC_ID,
LKP.DRFT_TYPE_CD,
LKP.BNK_DRFT_NUM,
LKP.BNK_DRFT_AMT,
LKP.BNK_DRFT_CLRD_DTTM,
LKP.BNK_DRFT_VOID_DTTM,
LKP.BNK_DRFT_STRT_DTTM,
LKP.TRANS_ACCT_ID,
 null as EDW_STRT_DTTM,
null as EDW_END_DTTM,
LKP_DOC.DOC_ID as DOC_ID,
LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as in_Typecode,
exp_src_pass.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src_pass.source_record_id ORDER BY --LKP.NewLookupRow asc,
LKP.BNK_DRFT_DOC_ID asc,LKP.DRFT_TYPE_CD asc,LKP.BNK_DRFT_NUM asc,LKP.BNK_DRFT_AMT asc,LKP.BNK_DRFT_CLRD_DTTM asc,LKP.BNK_DRFT_VOID_DTTM asc,LKP.BNK_DRFT_STRT_DTTM asc,LKP.TRANS_ACCT_ID asc) RNK2
FROM
exp_src_pass
INNER JOIN LKP_TERADATA_ETL_REF_XLAT ON exp_src_pass.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
INNER JOIN LKP_DOC ON LKP_TERADATA_ETL_REF_XLAT.source_record_id = LKP_DOC.source_record_id
LEFT JOIN (
SELECT BNK_DRFT.DRFT_TYPE_CD as DRFT_TYPE_CD, BNK_DRFT.BNK_DRFT_NUM as BNK_DRFT_NUM, BNK_DRFT.BNK_DRFT_AMT as BNK_DRFT_AMT,BNK_DRFT.BNK_DRFT_CLRD_DTTM as BNK_DRFT_CLRD_DTTM, BNK_DRFT.BNK_DRFT_VOID_DTTM as BNK_DRFT_VOID_DTTM, BNK_DRFT.BNK_DRFT_STRT_DTTM as BNK_DRFT_STRT_DTTM, BNK_DRFT.BNK_DRFT_DOC_ID as BNK_DRFT_DOC_ID ,BNK_DRFT.TRANS_ACCT_ID as TRANS_ACCT_ID
FROM DB_T_PROD_CORE.BNK_DRFT 
 QUALIFY ROW_NUMBER() OVER(PARTITION BY BNK_DRFT.BNK_DRFT_DOC_ID,TRANS_ACCT_ID ORDER BY BNK_DRFT.EDW_END_DTTM desc) = 1
) LKP ON LKP.BNK_DRFT_DOC_ID = LKP_DOC.DOC_ID AND LKP.TRANS_ACCT_ID = exp_src_pass.trans_acct_id
QUALIFY RNK2 = 1
);


-- Component exp_data_transformation1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation1 AS
(
SELECT
LKP_BNK_DRFT_TGT.BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
LKP_BNK_DRFT_TGT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_BNK_DRFT_TGT.BNK_DRFT_STRT_DTTM as lkp_BNK_DRFT_STRT_DT,
MD5 ( LTRIM ( RTRIM ( LKP_BNK_DRFT_TGT.DRFT_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_BNK_DRFT_TGT.BNK_DRFT_NUM ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_BNK_DRFT_TGT.BNK_DRFT_AMT ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_BNK_DRFT_TGT.BNK_DRFT_CLRD_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_BNK_DRFT_TGT.BNK_DRFT_VOID_DTTM ) ) ) || LTRIM ( RTRIM ( LKP_BNK_DRFT_TGT.BNK_DRFT_STRT_DTTM ) ) ) as lkp_Checksum,
LKP_BNK_DRFT_TGT.in_Typecode as in_Typecode,
exp_src_pass.CheckNumber as in_RefNumber,
exp_src_pass.Amount as in_Amount,
exp_src_pass.out_ClearedDate_alfa as in_PaidDate,
exp_src_pass.in_updatetime as in_UpdateTime,
LKP_DOC.DOC_ID as in_DOC_ID,
exp_src_pass.CreateTime as in_BNK_DRFT_STRT_DT,
exp_src_pass.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_src_pass.EDW_END_DTTM as in_EDW_END_DTTM,
MD5 ( LTRIM ( RTRIM ( LKP_BNK_DRFT_TGT.in_Typecode ) ) || LTRIM ( RTRIM ( exp_src_pass.CheckNumber ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_src_pass.Amount ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_src_pass.out_ClearedDate_alfa ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_src_pass.in_updatetime ) ) ) || ltrim ( rtrim ( exp_src_pass.CreateTime ) ) ) as in_Checksum,
CASE WHEN lkp_Checksum IS NULL THEN ''I'' ELSE CASE WHEN lkp_Checksum != in_Checksum THEN ''U'' ELSE ''R'' END END as Calc_ins_upd,
:PRCS_ID as o_process_id,
to_date ( ''01-01-1000'' , ''mm-dd-yyyy'' ) as BNK_DRFT_STRT_DT,
exp_src_pass.Retired as Retired,
LKP_BNK_DRFT_TGT.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_src_pass.RNK as RNK,
exp_src_pass.trans_acct_id as trans_acct_id,
exp_src_pass.source_record_id
FROM
exp_src_pass
INNER JOIN LKP_DOC ON exp_src_pass.source_record_id = LKP_DOC.source_record_id
INNER JOIN LKP_BNK_DRFT_TGT ON LKP_DOC.source_record_id = LKP_BNK_DRFT_TGT.source_record_id
);


-- Component RTR_Insert_Update1_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update1_INSERT AS
(
SELECT
exp_data_transformation1.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
exp_data_transformation1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation1.in_Typecode as in_Typecode,
exp_data_transformation1.in_RefNumber as in_RefNumber,
exp_data_transformation1.in_Amount as in_Amount,
exp_data_transformation1.in_PaidDate as in_PaidDate,
exp_data_transformation1.in_UpdateTime as in_UpdateTime,
exp_data_transformation1.in_DOC_ID as in_DOC_ID,
exp_data_transformation1.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation1.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation1.Calc_ins_upd as Calc_ins_upd,
exp_data_transformation1.o_process_id as o_process_id,
exp_data_transformation1.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT,
exp_data_transformation1.lkp_BNK_DRFT_STRT_DT as lkp_BNK_DRFT_STRT_DT,
exp_data_transformation1.in_BNK_DRFT_STRT_DT as in_BNK_DRFT_STRT_DT,
exp_data_transformation1.Retired as Retired,
exp_data_transformation1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation1.RNK as RNK,
exp_data_transformation1.trans_acct_id as trans_acct_id,
exp_data_transformation1.source_record_id
FROM
exp_data_transformation1
WHERE exp_data_transformation1.in_DOC_ID IS NOT NULL and exp_data_transformation1.Calc_ins_upd = ''I'' OR exp_data_transformation1.Calc_ins_upd = ''U'' OR ( exp_data_transformation1.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_data_transformation1.Retired = 0 ));


-- Component RTR_Insert_Update1_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update1_RETIRED AS
(
SELECT
exp_data_transformation1.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
exp_data_transformation1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation1.in_Typecode as in_Typecode,
exp_data_transformation1.in_RefNumber as in_RefNumber,
exp_data_transformation1.in_Amount as in_Amount,
exp_data_transformation1.in_PaidDate as in_PaidDate,
exp_data_transformation1.in_UpdateTime as in_UpdateTime,
exp_data_transformation1.in_DOC_ID as in_DOC_ID,
exp_data_transformation1.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation1.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation1.Calc_ins_upd as Calc_ins_upd,
exp_data_transformation1.o_process_id as o_process_id,
exp_data_transformation1.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT,
exp_data_transformation1.lkp_BNK_DRFT_STRT_DT as lkp_BNK_DRFT_STRT_DT,
exp_data_transformation1.in_BNK_DRFT_STRT_DT as in_BNK_DRFT_STRT_DT,
exp_data_transformation1.Retired as Retired,
exp_data_transformation1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation1.RNK as RNK,
exp_data_transformation1.trans_acct_id as trans_acct_id,
exp_data_transformation1.source_record_id
FROM
exp_data_transformation1
WHERE exp_data_transformation1.Calc_ins_upd = ''R'' and exp_data_transformation1.Retired != 0 and exp_data_transformation1.lkp_EDW_END_DTTM = TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component RTR_Insert_Update1_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update1_UPDATE AS
(
SELECT
exp_data_transformation1.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
exp_data_transformation1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation1.in_Typecode as in_Typecode,
exp_data_transformation1.in_RefNumber as in_RefNumber,
exp_data_transformation1.in_Amount as in_Amount,
exp_data_transformation1.in_PaidDate as in_PaidDate,
exp_data_transformation1.in_UpdateTime as in_UpdateTime,
exp_data_transformation1.in_DOC_ID as in_DOC_ID,
exp_data_transformation1.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation1.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation1.Calc_ins_upd as Calc_ins_upd,
exp_data_transformation1.o_process_id as o_process_id,
exp_data_transformation1.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT,
exp_data_transformation1.lkp_BNK_DRFT_STRT_DT as lkp_BNK_DRFT_STRT_DT,
exp_data_transformation1.in_BNK_DRFT_STRT_DT as in_BNK_DRFT_STRT_DT,
exp_data_transformation1.Retired as Retired,
exp_data_transformation1.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation1.RNK as RNK,
exp_data_transformation1.trans_acct_id as trans_acct_id,
exp_data_transformation1.source_record_id
FROM
exp_data_transformation1
WHERE FALSE 
-- exp_data_transformation1.Calc_ins_upd = ''U'' AND exp_data_transformation1.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_bank_draft_Update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_bank_draft_Update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update1_UPDATE.in_Typecode as in_Typecode1,
RTR_Insert_Update1_UPDATE.in_RefNumber as in_RefNumber1,
RTR_Insert_Update1_UPDATE.in_Amount as in_Amount1,
RTR_Insert_Update1_UPDATE.in_PaidDate as in_PaidDate1,
RTR_Insert_Update1_UPDATE.in_UpdateTime as in_UpdateTime1,
RTR_Insert_Update1_UPDATE.in_DOC_ID as in_DOC_ID1,
RTR_Insert_Update1_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
RTR_Insert_Update1_UPDATE.in_EDW_END_DTTM as in_EDW_END_DTTM1,
RTR_Insert_Update1_UPDATE.o_process_id as o_process_id1,
RTR_Insert_Update1_UPDATE.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT3,
RTR_Insert_Update1_UPDATE.in_BNK_DRFT_STRT_DT as in_BNK_DRFT_STRT_DT3,
RTR_Insert_Update1_UPDATE.Retired as Retired3,
RTR_Insert_Update1_UPDATE.trans_acct_id as trans_acct_id3,
0 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update1_UPDATE.source_record_id
FROM
RTR_Insert_Update1_UPDATE
);


-- Component upd_bank_draft_ins1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_bank_draft_ins1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update1_INSERT.in_Typecode as in_Typecode1,
RTR_Insert_Update1_INSERT.in_RefNumber as in_RefNumber1,
RTR_Insert_Update1_INSERT.in_Amount as in_Amount1,
RTR_Insert_Update1_INSERT.in_PaidDate as in_PaidDate1,
RTR_Insert_Update1_INSERT.in_UpdateTime as in_UpdateTime1,
RTR_Insert_Update1_INSERT.in_DOC_ID as in_DOC_ID1,
RTR_Insert_Update1_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
RTR_Insert_Update1_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
RTR_Insert_Update1_INSERT.o_process_id as o_process_id1,
RTR_Insert_Update1_INSERT.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT1,
RTR_Insert_Update1_INSERT.in_BNK_DRFT_STRT_DT as in_BNK_DRFT_STRT_DT1,
RTR_Insert_Update1_INSERT.Retired as Retired1,
RTR_Insert_Update1_INSERT.trans_acct_id as trans_acct_id1,
RTR_Insert_Update1_INSERT.RNK as RNK1,
0 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update1_INSERT.SOURCE_RECORD_ID
FROM
RTR_Insert_Update1_INSERT
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
upd_bank_draft_Update.in_Typecode1 as in_Typecode1,
upd_bank_draft_Update.in_RefNumber1 as in_RefNumber1,
upd_bank_draft_Update.in_Amount1 as in_Amount1,
upd_bank_draft_Update.in_PaidDate1 as in_PaidDate1,
upd_bank_draft_Update.in_UpdateTime1 as in_UpdateTime1,
upd_bank_draft_Update.in_DOC_ID1 as in_DOC_ID1,
upd_bank_draft_Update.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_bank_draft_Update.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_bank_draft_Update.o_process_id1 as o_process_id1,
upd_bank_draft_Update.BNK_DRFT_STRT_DT3 as BNK_DRFT_STRT_DT3,
upd_bank_draft_Update.in_BNK_DRFT_STRT_DT3 as in_BNK_DRFT_STRT_DT3,
upd_bank_draft_Update.Retired3 as Retired3,
upd_bank_draft_Update.trans_acct_id3 as trans_acct_id3,
upd_bank_draft_Update.source_record_id
FROM
upd_bank_draft_Update
WHERE upd_bank_draft_Update.Retired3 = 0
);


-- Component exp_ins_pass_to_target1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target1 AS
(
SELECT
upd_bank_draft_ins1.in_Typecode1 as in_Typecode1,
upd_bank_draft_ins1.in_RefNumber1 as in_RefNumber1,
upd_bank_draft_ins1.in_Amount1 as out_Amount1,
upd_bank_draft_ins1.in_PaidDate1 as in_PaidDate1,
upd_bank_draft_ins1.in_UpdateTime1 as in_UpdateTime1,
upd_bank_draft_ins1.in_DOC_ID1 as in_DOC_ID1,
upd_bank_draft_ins1.o_process_id1 as o_process_id1,
upd_bank_draft_ins1.in_BNK_DRFT_STRT_DT1 as in_BNK_DRFT_STRT_DT1,
CASE WHEN upd_bank_draft_ins1.Retired1 <> 0 THEN CURRENT_TIMESTAMP ELSE upd_bank_draft_ins1.in_EDW_END_DTTM1 END as o_EDW_END_DTTM,
upd_bank_draft_ins1.trans_acct_id1 as trans_acct_id1,
DATEADD(
  SECOND,
  2 * (upd_bank_draft_ins1.RNK1 - 1),
  CURRENT_TIMESTAMP()
) as in_EDW_STRT_DTTM1,
upd_bank_draft_ins1.source_record_id
FROM
upd_bank_draft_ins1
);


-- Component upd_Bank_Draft_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Bank_Draft_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update1_RETIRED.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID3,
RTR_Insert_Update1_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
RTR_Insert_Update1_RETIRED.o_process_id as o_process_id3,
1 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update1_RETIRED.SOURCE_RECORD_ID
FROM
RTR_Insert_Update1_RETIRED
);


-- Component upd_Bank_Draft_upd1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Bank_Draft_upd1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update1_UPDATE.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID3,
RTR_Insert_Update1_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
RTR_Insert_Update1_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
RTR_Insert_Update1_UPDATE.o_process_id as o_process_id3,
1 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update1_UPDATE.SOURCE_RECORD_ID
FROM
RTR_Insert_Update1_UPDATE
);


-- Component Exp_Tgt_Upd_Pass_to_tgt_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_Tgt_Upd_Pass_to_tgt_retired AS
(
SELECT
upd_Bank_Draft_retired.lkp_BNK_DRFT_DOC_ID3 as lkp_BNK_DRFT_DOC_ID3,
upd_Bank_Draft_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as Expiry_END_DATE,
upd_Bank_Draft_retired.source_record_id
FROM
upd_Bank_Draft_retired
);


-- Component Exp_Tgt_Upd_Pass_to_tgt1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_Tgt_Upd_Pass_to_tgt1 AS
(
SELECT
upd_Bank_Draft_upd1.lkp_BNK_DRFT_DOC_ID3 as lkp_BNK_DRFT_DOC_ID3,
upd_Bank_Draft_upd1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
DATEADD (
  SECOND,
  -1,
  upd_Bank_Draft_upd1.in_EDW_STRT_DTTM3
) as Expiry_END_DATE,
upd_Bank_Draft_upd1.source_record_id
FROM
upd_Bank_Draft_upd1
);


-- Component exp_ins_pass_to_target_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target_update AS
(
SELECT
FILTRANS.in_Typecode1 as in_Typecode1,
FILTRANS.in_RefNumber1 as in_RefNumber1,
FILTRANS.in_Amount1 as out_Amount1,
FILTRANS.in_PaidDate1 as in_PaidDate1,
FILTRANS.in_UpdateTime1 as in_UpdateTime1,
FILTRANS.in_DOC_ID1 as in_DOC_ID1,
FILTRANS.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
FILTRANS.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
FILTRANS.o_process_id1 as o_process_id1,
FILTRANS.in_BNK_DRFT_STRT_DT3 as in_BNK_DRFT_STRT_DT3,
FILTRANS.trans_acct_id3 as trans_acct_id3,
FILTRANS.source_record_id
FROM
FILTRANS
);


-- Component tgt_bnk_drft_update_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.BNK_DRFT
USING Exp_Tgt_Upd_Pass_to_tgt_retired ON (BNK_DRFT.BNK_DRFT_DOC_ID = Exp_Tgt_Upd_Pass_to_tgt_retired.lkp_BNK_DRFT_DOC_ID3 AND BNK_DRFT.EDW_STRT_DTTM = Exp_Tgt_Upd_Pass_to_tgt_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
BNK_DRFT_DOC_ID = Exp_Tgt_Upd_Pass_to_tgt_retired.lkp_BNK_DRFT_DOC_ID3,
EDW_STRT_DTTM = Exp_Tgt_Upd_Pass_to_tgt_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = Exp_Tgt_Upd_Pass_to_tgt_retired.Expiry_END_DATE;


-- Component tgt_bnk_drft_insert1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.BNK_DRFT
(
BNK_DRFT_DOC_ID,
DRFT_TYPE_CD,
BNK_DRFT_NUM,
BNK_DRFT_AMT,
BNK_DRFT_CLRD_DTTM,
TRANS_ACCT_ID,
BNK_DRFT_VOID_DTTM,
PRCS_ID,
BNK_DRFT_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_ins_pass_to_target1.in_DOC_ID1 as BNK_DRFT_DOC_ID,
exp_ins_pass_to_target1.in_Typecode1 as DRFT_TYPE_CD,
exp_ins_pass_to_target1.in_RefNumber1 as BNK_DRFT_NUM,
exp_ins_pass_to_target1.out_Amount1 as BNK_DRFT_AMT,
exp_ins_pass_to_target1.in_PaidDate1 as BNK_DRFT_CLRD_DTTM,
exp_ins_pass_to_target1.trans_acct_id1 as TRANS_ACCT_ID,
exp_ins_pass_to_target1.in_UpdateTime1 as BNK_DRFT_VOID_DTTM,
exp_ins_pass_to_target1.o_process_id1 as PRCS_ID,
exp_ins_pass_to_target1.in_BNK_DRFT_STRT_DT1 as BNK_DRFT_STRT_DTTM,
exp_ins_pass_to_target1.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_pass_to_target1.o_EDW_END_DTTM as EDW_END_DTTM
FROM
exp_ins_pass_to_target1;


-- Component tgt_bnk_drft_insert1, Type Post SQL 
UPDATE  DB_T_PROD_CORE.BNK_DRFT  FROM  

(

SELECT	distinct BNK_DRFT_DOC_ID,EDW_STRT_DTTM,TRANS_ACCT_ID,

max(EDW_STRT_DTTM) over (partition by BNK_DRFT_DOC_ID,TRANS_ACCT_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1

FROM	DB_T_PROD_CORE.BNK_DRFT

)  A

SET EDW_END_DTTM= A.lead1

where BNK_DRFT.BNK_DRFT_DOC_ID = A.BNK_DRFT_DOC_ID 

and BNK_DRFT.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

AND BNK_DRFT.TRANS_ACCT_ID=A.TRANS_ACCT_ID

and A.lead1 is not null  ;


-- Component tgt_bnk_drft_update1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.BNK_DRFT
USING Exp_Tgt_Upd_Pass_to_tgt1 ON (BNK_DRFT.BNK_DRFT_DOC_ID = Exp_Tgt_Upd_Pass_to_tgt1.lkp_BNK_DRFT_DOC_ID3 AND BNK_DRFT.EDW_STRT_DTTM = Exp_Tgt_Upd_Pass_to_tgt1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
BNK_DRFT_DOC_ID = Exp_Tgt_Upd_Pass_to_tgt1.lkp_BNK_DRFT_DOC_ID3,
EDW_STRT_DTTM = Exp_Tgt_Upd_Pass_to_tgt1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = Exp_Tgt_Upd_Pass_to_tgt1.Expiry_END_DATE;


-- Component tgt_bnk_drft_ins_Update, Type TARGET 
INSERT INTO DB_T_PROD_CORE.BNK_DRFT
(
BNK_DRFT_DOC_ID,
DRFT_TYPE_CD,
BNK_DRFT_NUM,
BNK_DRFT_AMT,
BNK_DRFT_CLRD_DTTM,
TRANS_ACCT_ID,
BNK_DRFT_VOID_DTTM,
PRCS_ID,
BNK_DRFT_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_ins_pass_to_target_update.in_DOC_ID1 as BNK_DRFT_DOC_ID,
exp_ins_pass_to_target_update.in_Typecode1 as DRFT_TYPE_CD,
exp_ins_pass_to_target_update.in_RefNumber1 as BNK_DRFT_NUM,
exp_ins_pass_to_target_update.out_Amount1 as BNK_DRFT_AMT,
exp_ins_pass_to_target_update.in_PaidDate1 as BNK_DRFT_CLRD_DTTM,
exp_ins_pass_to_target_update.trans_acct_id3 as TRANS_ACCT_ID,
exp_ins_pass_to_target_update.in_UpdateTime1 as BNK_DRFT_VOID_DTTM,
exp_ins_pass_to_target_update.o_process_id1 as PRCS_ID,
exp_ins_pass_to_target_update.in_BNK_DRFT_STRT_DT3 as BNK_DRFT_STRT_DTTM,
exp_ins_pass_to_target_update.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_pass_to_target_update.in_EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_ins_pass_to_target_update;


END; ';