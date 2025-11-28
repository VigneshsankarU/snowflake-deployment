-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_CHK_PAYEE_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
 

-- Component sq_cc_checkpayee, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_checkpayee AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as DOC_ID,
$2 as INDIV_PRTY_ID,
$3 as CHK_PAYEE_TYPE_CD,
$4 as UPDATETIME,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT A.DOC_ID,A.INDIV_PRTY_ID,A.CHK_PAYEE_TYPE_CD,A.UPDATETIME FROM (

SELECT DISTINCT A.DOC_ID,A.INDIV_PRTY_ID,A.CHK_PAYEE_TYPE_CD,A.UPDATETIME,CASE WHEN CP.BNK_DRFT_DOC_ID IS NULL THEN ''I'' ELSE ''R'' END AS FLG FROM 

(SELECT DISTINCT DC.DOC_ID,COALESCE(XLAT1.TGT_IDNTFTN_VAL,''UNK'') AS CHK_PAYEE_TYPE_CD,UPDATETIME,

/* EIM-30261 case sensitve update for IN condition */

COALESCE(CASE WHEN X.TYPECODE IN (''User Contact'',''Person'',''Vendor (Person)'',''Doctor'',''Policy Person'',''Adjudicator'',''Attorney'',''Contact'') THEN IV.INDIV_PRTY_ID ELSE NULL END,BS.BUSN_PRTY_ID) AS INDIV_PRTY_ID

from	(

	SELECT	cc_checkpayee.CheckNumber as CHECKNUMBER, cc_checkpayee.PublicID as PUBLICID, cc_checkpayee.Name as NAME,

			cc_checkpayee.Subtype AS SUBTYPE,cc_checkpayee.Name as TYPECODE, 

	AddressBookUID AS ADDRESSBOOKUID,	to_date(''1900-01-01'' , ''YYYY-MM-DD'')  as UPDATETIME,cc_checkpayee.typecode as CHECKPAYEETYPECODE

	FROM

	(

	SELECT	 cast(cc_contact.PublicID_stg as varchar(64)) as publicid,

		cast(cc_check.Publicid_stg as varchar(64)) as checknumber, 

		cast(cc_contact.AddressBookUID_stg as varchar(64)) as AddressBookUID,

		cctl_contact.NAME_stg as name, 

		cctl_contactrole.typecode_stg as typecode,

		cc_contact.Subtype_stg as subtype

from	db_t_prod_stag.cc_checkpayee

JOIN db_t_prod_stag.cc_check 

	on cc_check.ID_stg=cc_checkpayee.CheckID_stg

JOIN db_t_prod_stag.cc_contact 

	on cc_checkpayee.PayeeDenormID_stg = cc_contact.ID_stg

JOIN db_t_prod_stag.cctl_contact 

	on cctl_contact.id_stg=cc_contact.Subtype_stg

join db_t_prod_stag.cctl_contactrole 

	on cctl_contactrole.id_stg=cc_checkpayee.PayeeType_stg

)

	  cc_checkpayee

	 

	 union

	 

	select	CAST(BC_OUTGOINGPAYMENT.id AS VARCHAR(64)) AS CHECKNUMBER ,

			CAST(BC_OUTGOINGPAYMENT.bc_address_publicid AS VARCHAR(64)) as PUBLICID,

			CAST(null  AS VARCHAR(50)) as NAME,cast(null as int) as  SUBTYPE,

	 typecode,AddressBookUID AS ADDRESSBOOKUID ,updatetime AS UPDATETIME,CAST(null  AS VARCHAR(50)) as CHECKPAYEETYPECODE

	 FROM	(

SELECT 

 A.UpdateTime_stg as updatetime,

 A.ID_stg as id,

 cast(bc_contact.PublicID_stg as varchar(64)) as bc_address_publicid,

 bctl_contact.typecode_stg as typecode,

cast(bc_contact.AddressBookUID_stg as varchar(64)) as addressbookuid

FROM

 (Select bc_outgoingpayment.*, bc_paymentinstrument.PaymentMethod_stg as PaymentMethod, bctl_paymentmethod.typecode_stg as fund_trnsfr_mthd_typ

 from db_t_prod_stag.bc_outgoingpayment left outer join db_t_prod_stag.bc_paymentinstrument

 on bc_outgoingpayment.PaymentInstrumentID_stg = bc_paymentinstrument.ID_stg

left outer join db_t_prod_stag.bctl_paymentmethod on bctl_paymentmethod.ID_stg = bc_paymentinstrument.PaymentMethod_stg) A,

 (Select bc_outgoingpayment.*, bc_disbursement.Status_stg as bcdisbursementstatus

 from db_t_prod_stag.bc_outgoingpayment left outer join db_t_prod_stag.bc_disbursement

 on bc_outgoingpayment.DisbursementID_stg = bc_disbursement.ID_stg) B

left join db_t_prod_stag.bc_disbursement on db_t_prod_stag.bc_disbursement.ID_stg=B.DisbursementID_stg

left join db_t_prod_stag.bc_unappliedfund on db_t_prod_stag.bc_unappliedfund.id_stg=db_t_prod_stag.bc_disbursement.UnappliedFundID_stg

left join db_t_prod_stag.bc_invoicestream on db_t_prod_stag.bc_invoicestream.UnappliedFundID_stg=db_t_prod_stag.bc_unappliedfund.id_stg

left join  db_t_prod_stag.bc_account   ON (db_t_prod_stag.bc_account.id_stg = db_t_prod_stag.bc_invoicestream.PolicyID_stg and db_t_prod_stag.bc_disbursement.AccountID_stg = db_t_prod_stag.bc_account.id_stg)

/* left Join bc_account on bc_disbursement.AccountID_stg = bc_account.id_stg */
left Join db_t_prod_stag.bc_accountcontact on db_t_prod_stag.bc_account.id_stg = db_t_prod_stag.bc_accountcontact.AccountID_stg

left Join db_t_prod_stag.bc_contact on db_t_prod_stag.bc_accountcontact.ContactID_stg = db_t_prod_stag.bc_contact.id_stg

left Join db_t_prod_stag.bc_account acc on db_t_prod_stag.bc_disbursement.AccountID_stg = acc.id_stg

LEFT OUTER JOIN db_t_prod_stag.bctl_contact ON db_t_prod_stag.bctl_contact.id_stg = db_t_prod_stag.bc_contact.subtype_stg

where A.ID_stg=B.ID_stg

And A.UpdateTime_stg > (:START_DTTM)

and A.UpdateTime_stg <= (:END_DTTM)

)BC_OUTGOINGPAYMENT )x



 LEFT OUTER JOIN

 (SELECT DISTINCT DOC_ID,DOC_ISSUR_NUM AS DOC_ISSUR_NUM 
 FROM --DB_CORE.DOC 
 DB_T_PROD_CORE.DOC 
 ) DC

 ON DC.DOC_ISSUR_NUM =X.CHECKNUMBER

 

 LEFT OUTER JOIN 

 (SELECT  DISTINCT BUSN_PRTY_ID,

/* EIM-30261 case sensitve removed UPPER case contion */

NK_BUSN_CD  FROM  
--DB_CORE.BUSN 
DB_T_PROD_CORE.BUSN
WHERE  to_date(EDW_END_DTTM) =  to_date (''31/12/9999'' , ''DD/MM/YYYY'')) BS

  ON BS.NK_BUSN_CD = X.PUBLICID

 

 LEFT OUTER JOIN 

 (SELECT DISTINCT TGT_IDNTFTN_VAL,SRC_IDNTFTN_VAL 

FROM --DB_CORE.TERADATA_ETL_REF_XLAT 
DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CHK_PAYEE_TYPE'' AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT1

 ON XLAT1.SRC_IDNTFTN_VAL=X.CHECKPAYEETYPECODE

 

 LEFT OUTER JOIN 

 (SELECT DISTINCT INDIV_PRTY_ID,NK_PUBLC_ID 
 FROM --DB_CORE.INDIV 
 DB_T_PROD_CORE.INDIV
 WHERE NK_PUBLC_ID IS NOT NULL) IV

ON IV.NK_PUBLC_ID=X.PUBLICID



 WHERE CHECKNUMBER IS NOT NULL) AS A   

 

 LEFT OUTER JOIN (
	SELECT BNK_DRFT_DOC_ID, CHK_PAYEE_PRTY_ID, CHK_PAYEE_TYPE_CD 
 FROM --DB_CORE.CHK_PAYEE
 DB_T_PROD_CORE.CHK_PAYEE
 )CP ON  CP.BNK_DRFT_DOC_ID = A.DOC_ID AND CP.CHK_PAYEE_PRTY_ID = A.INDIV_PRTY_ID AND CP.CHK_PAYEE_TYPE_CD = A.CHK_PAYEE_TYPE_CD

 

 WHERE    A.DOC_ID IS NOT NULL AND A.INDIV_PRTY_ID IS NOT NULL ) AS A WHERE FLG=''I''
) SRC
)
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
sq_cc_checkpayee.DOC_ID as BNK_DRFT_DOC_ID,
sq_cc_checkpayee.INDIV_PRTY_ID as CHK_PAYEE_PRTY_ID,
sq_cc_checkpayee.CHK_PAYEE_TYPE_CD as CHK_PAYEE_TYPE_CD,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP_NTZ ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
sq_cc_checkpayee.UPDATETIME as TRANS_STRT_DTTM,
TO_TIMESTAMP_NTZ ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
sq_cc_checkpayee.source_record_id
FROM
sq_cc_checkpayee
);


-- Component CHK_PAYEE, Type TARGET 
INSERT INTO db_t_prod_core.CHK_PAYEE
(
BNK_DRFT_DOC_ID,
CHK_PAYEE_PRTY_ID,
CHK_PAYEE_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.BNK_DRFT_DOC_ID as BNK_DRFT_DOC_ID,
exp_pass_to_target_ins.CHK_PAYEE_PRTY_ID as CHK_PAYEE_PRTY_ID,
exp_pass_to_target_ins.CHK_PAYEE_TYPE_CD as CHK_PAYEE_TYPE_CD,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_pass_to_target_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


END; ';