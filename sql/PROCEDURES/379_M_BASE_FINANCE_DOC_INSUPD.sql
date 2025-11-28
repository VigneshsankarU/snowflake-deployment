-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_DOC_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

-- Component LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL ,row_number() over (order by 1) AS source_record_id

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_CTGY_TYPE'' 

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


-- Component LKP_XREF_DOC, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_DOC AS
(
SELECT
DOC_ID,
DOC_ISSUR_NUM,
DOC_TYPE_CD,
DOC_CTGY_TYPE_CD,
SRC_SYS_CD,
LOAD_DTTM
FROM DB_T_PROD_CORE.DIR_DOC
);


-- Component sq_cc_check, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_check AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Ref_key,
$2 as doc_type,
$3 as Doc_Category,
$4 as DOC_STATUS,
$5 as SRC_CD,
$6 as DOC_CRTN_DTTM,
$7 as Retired,
$8 as UpdateTime,
$9 as rnk,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT REF_KEY ,DOC_TYPE,DOC_CATEGORY,DOC_STATUS,SRC_CD,CreateTime,Retired,drv_updatetime,RANK() OVER (PARTITION BY ref_key,doc_type,doc_category  ORDER BY drv_updatetime asc) AS R

FROM (



SELECT	DISTINCT  cast(cc_check.publicid_stg as varchar(1000)) AS REF_KEY,

		CAST(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

CCTL_TRANSACTIONSTATUS.TYPECODE_stg AS DOC_STATUS,''SRC_SYS6'' AS SRC_CD,

CC_CHECK.CreateTime_stg AS CreateTime,CC_CHECK.Retired_stg AS Retired,

cc_check.updatetime_stg  as drv_updatetime

FROM (	SELECT distinct cc_check.PublicID_stg, cc_check.CreateTime_stg,cc_check.UpdateTime_stg,cc_check.Retired_stg,cc_claim.ClaimNumber_stg,CC_CHECK.STATUS_stg  FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= ( :end_dttm)

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

/* and cc_check.CheckNumber is not null */
)

CC_CHECK, DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS 

WHERE	CC_CHECK.STATUS_stg=CCTL_TRANSACTIONSTATUS.ID_stg  

	AND   cc_check.publicid_stg  IS  NOT NULL 

	AND CLAIMNUMBER_stg IS NOT NULL  

	AND 

 UPPER(CCTL_TRANSACTIONSTATUS.TYPECODE_stg) NOT IN (''ISSUED'',''VOIDED'')



union

	

SELECT DISTINCT  cast(cc_check.publicid_stg as varchar(1000)) AS REF_KEY, CAST(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

''ISSUED'' AS DOC_STATUS,''SRC_SYS6'' AS SRC_CD,

CC_CHECK.CreateTime_stg AS CreateTime ,CC_CHECK.Retired_stg AS Retired,

cc_check.issuedate_stg as drv_updatetime

FROM

(	SELECT distinct cc_check.PublicID_stg, cc_check.CreateTime_stg,cc_check.issuedate_stg,cc_check.Retired_stg,cc_claim.ClaimNumber_stg,CC_CHECK.STATUS_stg  FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= ( :end_dttm)

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

/* and cc_check.CheckNumber is not null */
)



CC_CHECK 

WHERE  cc_check.publicid_stg  IS  NOT NULL AND CLAIMNUMBER_stg IS NOT NULL and cc_check.issuedate_stg is not null 

		

	

union

		

SELECT DISTINCT  cast(cc_check.publicid_stg as varchar(1000)) AS REF_KEY, CAST(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

''VOIDED'' AS DOC_STATUS,''SRC_SYS6'' AS SRC_CD,

CC_CHECK.CreateTime_stg AS CreateTime,CC_CHECK.Retired_stg AS Retired,

cc_check.VoidDate_Alfa_stg as drv_updatetime

FROM 

(	SELECT distinct cc_check.PublicID_stg, cc_check.CreateTime_stg,cc_check.VoidDate_Alfa_stg ,cc_check.Retired_stg,cc_claim.ClaimNumber_stg,CC_CHECK.STATUS_stg  FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= ( :end_dttm)

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

/* and cc_check.CheckNumber is not null */
)

CC_CHECK 

WHERE  cc_check.publicid_stg  IS  NOT NULL AND CLAIMNUMBER_stg IS NOT NULL and cc_check.VoidDate_Alfa_stg is not null 		



union





SELECT DISTINCT  cast(CC_CHECK.CombinedCheckNumber_alfa_stg as varchar(1000)) AS REF_KEY, 

CAST(''DOC_TYPE6'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

CCTL_TRANSACTIONSTATUS.TYPECODE_stg AS DOC_STATUS,''SRC_SYS6'' AS SRC_CD,

CC_CHECK.CreateTime_stg AS CreateTime,CC_CHECK.Retired_stg AS Retired,updatetime_stg

FROM 

(	SELECT distinct cc_check.CombinedCheckNumber_alfa_stg, cc_check.CreateTime_stg,cc_check.Retired_stg,cc_claim.ClaimNumber_stg,CC_CHECK.STATUS_stg,cc_check.updatetime_stg  FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= ( :end_dttm)

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

/* and cc_check.CheckNumber is not null */
)

CC_CHECK, DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS 

WHERE CC_CHECK.STATUS_stg=CCTL_TRANSACTIONSTATUS.ID_stg  

AND UPPER(CCTL_TRANSACTIONSTATUS.TYPECODE_stg) NOT IN (''ISSUED'',''VOIDED'',''TRANSFERRED'') /* As per EIM-33812 */
AND CC_CHECK.CombinedCheckNumber_alfa_stg  IS  NOT NULL AND CLAIMNUMBER_stg IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY ref_key,doc_type,doc_category  ORDER BY updatetime_stg DESC,Createtime_stg desc)=1	

 

 

 union 

 

 

 SELECT DISTINCT  cast(CC_CHECK.CombinedCheckNumber_alfa_stg as varchar(1000)) AS REF_KEY, CAST(''DOC_TYPE6'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

''ISSUED'' AS DOC_STATUS,''SRC_SYS6'' AS SRC_CD,

CC_CHECK.CreateTime_stg AS CreateTime_stg,CC_CHECK.Retired_stg AS Retired,ISSUEDATE_stg

FROM (	SELECT distinct cc_check.CombinedCheckNumber_alfa_stg, cc_check.CreateTime_stg,cc_check.Retired_stg,cc_claim.ClaimNumber_stg,CC_CHECK.STATUS_stg,cc_check.updatetime_stg,cc_check.ISSUEDATE_stg  FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= ( :end_dttm)

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

/* and cc_check.CheckNumber is not null */
) CC_CHECK 

WHERE   cc_check.issuedate_stg is not null  AND CC_CHECK.CombinedCheckNumber_alfa_stg  IS  NOT NULL AND CLAIMNUMBER_stg IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY ref_key,doc_type,doc_category  ORDER BY updatetime_stg DESC,Createtime_stg desc)=1

  



UNION

 SELECT DISTINCT  cast(CC_CHECK.CombinedCheckNumber_alfa_stg as varchar(1000)) AS REF_KEY, CAST(''DOC_TYPE6'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

''VOIDED'' AS DOC_STATUS,''SRC_SYS6'' AS SRC_CD,

CC_CHECK.CreateTime_stg AS CreateTime_stg,CC_CHECK.Retired_stg AS Retired,VoidDate_Alfa_stg

FROM 

(	SELECT distinct cc_check.CombinedCheckNumber_alfa_stg, cc_check.CreateTime_stg,cc_check.Retired_stg,cc_claim.ClaimNumber_stg,CC_CHECK.STATUS_stg,cc_check.updatetime_stg,cc_check.ISSUEDATE_stg,cc_check.VoidDate_Alfa_stg  FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= ( :end_dttm)

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

/* and cc_check.CheckNumber is not null */
)

CC_CHECK

WHERE   cc_check.VoidDate_Alfa_stg is not null AND  cc_check.issuedate_stg is not null  AND CC_CHECK.CombinedCheckNumber_alfa_stg  IS  NOT NULL AND CLAIMNUMBER_stg IS NOT NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY ref_key,doc_type,doc_category  ORDER BY updatetime_stg DESC,Createtime_stg desc)=1 

 )x 



union



SELECT REF_KEY ,DOC_TYPE,DOC_CATEGORY,DOC_STATUS,SRC_CD,CreateTime_stg,Retired_stg,updatetime_stg,1 as R

FROM (

SELECT cast(INVOICENUMBER_stg as varchar(1000)) AS REF_KEY, ''DOC_TYPE3'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY ,CAST('''' AS VARCHAR(50)) AS DOC_STATUS,''SRC_SYS5'' AS SRC_CD,

BC_INVOICE.CreateTime_stg AS CreateTime_stg,BC_INVOICE.Retired_stg AS Retired_stg,updatetime_stg

FROM DB_T_PROD_STAG.BC_INVOICE  WHERE INVOICENUMBER_stg IS  NOT NULL

and bc_invoice.UpdateTime_stg > (:start_dttm)	and bc_invoice.UpdateTime_stg <= ( :end_dttm)

UNION



SELECT DISTINCT 

coalesce(pc_questionsetlookup.SourceFile_stg,pc_questionlookup.SourceFile_stg) AS REF_KEY, 

''DOC_TYPE5''  AS DOC_TYPE, ''DOC_CTGY_TYPE5'' AS DOC_CATEGORY ,'''' AS DOC_STATUS,''SRC_SYS4'' AS SRC_CD,

coalesce(pc_questionsetlookup.CreateTime_stg,pc_questionlookup.CreateTime_stg) AS CreateTime_stg,

coalesce(pc_questionsetlookup.Retired_stg,pc_questionlookup.Retired_stg) AS Retired_stg,

pc_questionlookup.updatetime_stg as updatetime_stg 

FROM 

(

SELECT	pc_questionsetlookup.SourceFile_stg,pc_questionsetlookup.Retired_stg,

		pc_questionsetlookup.CreateTime_stg,pc_questionsetlookup.updateTime_stg

FROM

 DB_T_PROD_STAG.pc_questionsetlookup



where pc_questionsetlookup.UpdateTime_stg > (:start_dttm)	and pc_questionsetlookup.UpdateTime_stg <= ( :end_dttm)

)

pc_questionsetlookup full outer join 

(

SELECT	DISTINCT

 pc_questionlookup.SourceFile_stg,pc_questionlookup.Retired_stg,

		pc_questionlookup.CreateTime_stg,pc_questionlookup.updateTime_stg

FROM

DB_T_PROD_STAG.pc_questionlookup 

FULL OUTER JOIN

DB_T_PROD_STAG.pc_questionsetlookup

	ON pc_questionlookup.SourceFile_stg=pc_questionsetlookup.SourceFile_stg

LEFT JOIN

DB_T_PROD_STAG.pcx_etlquestion_alfa

	ON pc_questionlookup.QuestionCode_stg=pcx_etlquestion_alfa.QuestionCode_stg

where	pc_questionlookup.UpdateTime_stg > (:start_dttm)		and pc_questionlookup.UpdateTime_stg <= ( :end_dttm)

)

pc_questionlookup on 

pc_questionlookup.SourceFile_stg=pc_questionsetlookup.SourceFile_stg 

UNION



SELECT		CAST(BC_OUTGOINGPAYMENT.id_stg AS VARCHAR(50)) AS REF_KEY,

''DOC_TYPE1'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY,BCTL_OUTGOINGPAYMENTSTATUS.TYPECODE_stg AS DOC_STATUS,

''SRC_SYS5'' AS SRC_CD,BC_OUTGOINGPAYMENT.CreateTime_stg AS CreateTime_stg,

		BC_OUTGOINGPAYMENT.Retired_stg AS Retired_stg,updatetime_stg

FROM (

SELECT	 A.ID_stg,A.CreateTime_stg,A.Retired_stg,A.UpdateTime_stg,A.Status_stg 

FROM

 (Select bc_outgoingpayment.*, bc_paymentinstrument.PaymentMethod_stg as PaymentMethod_stg, bctl_paymentmethod.typecode_stg as fund_trnsfr_mthd_typ_stg

 from DB_T_PROD_STAG.BC_OUTGOINGPAYMENT left outer join DB_T_PROD_STAG.bc_paymentinstrument

 on bc_outgoingpayment.PaymentInstrumentID_stg = bc_paymentinstrument.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentmethod on bctl_paymentmethod.ID_stg = bc_paymentinstrument.PaymentMethod_stg) A,

 (Select bc_outgoingpayment.*, bc_disbursement.Status_stg as bcdisbursementstatus_stg

 from DB_T_PROD_STAG.BC_OUTGOINGPAYMENT left outer join DB_T_PROD_STAG.bc_disbursement

 on bc_outgoingpayment.DisbursementID_stg = bc_disbursement.ID_stg) B

left join DB_T_PROD_STAG.bc_disbursement on bc_disbursement.id_stg=B.DisbursementID_stg

left join DB_T_PROD_STAG.bc_unappliedfund on bc_unappliedfund.id_stg=bc_disbursement.UnappliedFundID_stg

left join DB_T_PROD_STAG.bc_invoicestream on bc_invoicestream.UnappliedFundID_stg=bc_unappliedfund.id_stg

left join  DB_T_PROD_STAG.bc_account   ON (bc_account.id_stg = bc_invoicestream. PolicyID_stg and bc_disbursement.AccountID_stg = bc_account.id_stg)

/* left Join DB_T_PROD_STAG.bc_account on bc_disbursement.AccountID = bc_account.id */
left Join DB_T_PROD_STAG.bc_accountcontact on bc_account.id_stg = bc_accountcontact.AccountID_stg

left Join DB_T_PROD_STAG.bc_contact on bc_accountcontact.ContactID_stg = bc_contact.id_stg

left Join DB_T_PROD_STAG.bc_account acc on bc_disbursement.AccountID_stg = acc.id_stg

LEFT OUTER JOIN DB_T_PROD_STAG.bctl_contact ON bctl_contact.id_stg = bc_contact.subtype_stg

where A.ID_stg=B.ID_stg

And A.UpdateTime_stg > (:start_dttm)

and A.UpdateTime_stg <= ( :end_dttm) 

)	BC_OUTGOINGPAYMENT

LEFT OUTER JOIN DB_T_PROD_STAG.BCTL_OUTGOINGPAYMENTSTATUS  

	ON BC_OUTGOINGPAYMENT.STATUS_stg=BCTL_OUTGOINGPAYMENTSTATUS.ID_stg

union 



SELECT REFNUMBER_stg  AS REF_KEY,''DOC_TYPE1'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY, 

case when  bc_basemoneyreceived.ReceivedDate_stg  is not null then ''DOC_STS_TYPE1''  end AS DOC_STATUS,

''SRC_SYS5'' AS SRC_CD,BC_BASEMONEYRECEIVED.CreateTime_stg  AS CreateTime_stg,BC_BASEMONEYRECEIVED.Retired_stg  AS Retired_stg,

bc_basemoneyreceived.ReceivedDate_stg as updatetime_stg 

FROM 

(

SELECT bc_basemoneyreceived.RefNumber_stg,bc_basemoneyreceived.UpdateTime_stg,

bc_basemoneyreceived.CreateTime_stg,bc_basemoneyreceived.Retired_stg,

bc_basemoneyreceived.ReceivedDate_stg, bc_basemoneyreceived.ReversalDate_stg

FROM

DB_T_PROD_STAG.BC_BASEMONEYRECEIVED

left outer join DB_T_PROD_STAG.bc_unappliedfund 

	on bc_unappliedfund.id_stg=bc_basemoneyreceived.UnappliedFundID_stg

left outer join DB_T_PROD_STAG.bc_user 

	on bc_user.id_stg=bc_unappliedfund.CreateUserID_stg

left outer join DB_T_PROD_STAG.bc_contact 

	on bc_contact.id_stg=bc_user.ContactID_stg

left outer join DB_T_PROD_STAG.bc_invoicestream 

	on bc_invoicestream.UnappliedFundID_stg=bc_unappliedfund.id_stg

left outer join  DB_T_PROD_STAG.bc_paymentinstrument 

	ON bc_basemoneyreceived.PaymentInstrumentID_stg = bc_paymentinstrument.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentmethod 

	on bctl_paymentmethod.id_stg=bc_paymentinstrument.PaymentMethod_stg

left outer join DB_T_PROD_STAG.bc_dbmoneyrcvdcontext 

	on bc_basemoneyreceived.id_stg= bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg 

left outer Join DB_T_PROD_STAG.bc_transaction 

	on bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg

left outer join DB_T_PROD_STAG.bctl_transaction 

	on bc_transaction.Subtype_stg=bctl_transaction.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentsource_alfa 

	on bc_basemoneyreceived.PaymentSource_alfa_stg=bctl_paymentsource_alfa.id_stg

left outer join DB_T_PROD_STAG.bc_account 

	on bc_basemoneyreceived.AccountID_stg = bc_account.id_stg

left outer join DB_T_PROD_STAG.bc_revtrans a 

on a.OwnerID_stg = bc_transaction.id_stg			/* ownerid is the trans that is reversing another trans(revrse)(4,2017) */
left outer join DB_T_PROD_STAG.bc_revtrans b 

on b.ForeignEntityID_stg = bc_transaction.id_stg/* recieve payment (3,2017) */
where bc_basemoneyreceived.UpdateTime_stg > (:start_dttm)	and bc_basemoneyreceived.UpdateTime_stg <= ( :end_dttm)

)

BC_BASEMONEYRECEIVED WHERE REFNUMBER_stg   IS NOT NULL  and ReceivedDate_stg  is not null

/* ------------ */
union 



SELECT REFNUMBER_stg  AS REF_KEY,''DOC_TYPE1'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY, 

case when bc_basemoneyreceived.ReversalDate_stg  is not null then ''DOC_STS_TYPE3'' end AS DOC_STATUS,

''SRC_SYS5'' AS SRC_CD,BC_BASEMONEYRECEIVED.CreateTime_stg  AS CreateTime_stg,BC_BASEMONEYRECEIVED.Retired_stg  AS Retired_stg,

bc_basemoneyreceived.ReversalDate_stg as updatetime_stg 

FROM 

(

SELECT bc_basemoneyreceived.RefNumber_stg,bc_basemoneyreceived.UpdateTime_stg,

bc_basemoneyreceived.CreateTime_stg,bc_basemoneyreceived.Retired_stg,

bc_basemoneyreceived.ReceivedDate_stg, bc_basemoneyreceived.ReversalDate_stg

FROM

DB_T_PROD_STAG.BC_BASEMONEYRECEIVED

left outer join DB_T_PROD_STAG.bc_unappliedfund 

	on bc_unappliedfund.id_stg=bc_basemoneyreceived.UnappliedFundID_stg

left outer join DB_T_PROD_STAG.bc_user 

	on bc_user.id_stg=bc_unappliedfund.CreateUserID_stg

left outer join DB_T_PROD_STAG.bc_contact 

	on bc_contact.id_stg=bc_user.ContactID_stg

left outer join DB_T_PROD_STAG.bc_invoicestream 

	on bc_invoicestream.UnappliedFundID_stg=bc_unappliedfund.id_stg

left outer join  DB_T_PROD_STAG.bc_paymentinstrument 

	ON bc_basemoneyreceived.PaymentInstrumentID_stg = bc_paymentinstrument.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentmethod 

	on bctl_paymentmethod.id_stg=bc_paymentinstrument.PaymentMethod_stg

left outer join DB_T_PROD_STAG.bc_dbmoneyrcvdcontext 

	on bc_basemoneyreceived.id_stg= bc_dbmoneyrcvdcontext.directbillmoneyrcvdid_stg 

left outer Join DB_T_PROD_STAG.bc_transaction 

	on bc_transaction.id_stg = bc_dbmoneyrcvdcontext.transactionid_stg

left outer join DB_T_PROD_STAG.bctl_transaction 

	on bc_transaction.Subtype_stg=bctl_transaction.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentsource_alfa 

	on bc_basemoneyreceived.PaymentSource_alfa_stg=bctl_paymentsource_alfa.id_stg

left outer join DB_T_PROD_STAG.bc_account 

	on bc_basemoneyreceived.AccountID_stg = bc_account.id_stg

left outer join DB_T_PROD_STAG.bc_revtrans a 

on a.OwnerID_stg = bc_transaction.id_stg			/* ownerid is the trans that is reversing another trans(revrse)(4,2017) */
left outer join DB_T_PROD_STAG.bc_revtrans b 

on b.ForeignEntityID_stg = bc_transaction.id_stg/* recieve payment (3,2017) */
where bc_basemoneyreceived.UpdateTime_stg > (:start_dttm)	and bc_basemoneyreceived.UpdateTime_stg <= ( :end_dttm)

)

BC_BASEMONEYRECEIVED WHERE REFNUMBER_stg   IS NOT NULL and ReversalDate_stg  is not null



) A  

/* ORDER BY ref_key,doc_type,doc_category,Createtime ASC nulls LAST, Retired ASC  */
QUALIFY ROW_NUMBER() OVER (PARTITION BY ref_key,doc_type,doc_category,DOC_STATUS ORDER BY updatetime_stg DESC,Createtime_stg desc,doc_status desc,retired_stg desc)=1
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_cc_check.Ref_key as Ref_key,
sq_cc_check.doc_type as doc_type,
LTRIM ( RTRIM ( sq_cc_check.Doc_Category ) ) as var_Doc_Category,
CASE WHEN TRIM(var_Doc_Category) = '''' OR var_Doc_Category IS NULL OR LENGTH ( var_Doc_Category ) = 0 THEN ''UNK'' ELSE LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD */ END as out_Doc_Category,
sq_cc_check.DOC_STATUS as DOC_STATUS,
sq_cc_check.SRC_CD as SRC_CD,
sq_cc_check.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
sq_cc_check.Retired as Retired,
sq_cc_check.UpdateTime as UpdateTime,
sq_cc_check.rnk as rnk,
sq_cc_check.source_record_id,
row_number() over (partition by sq_cc_check.source_record_id order by sq_cc_check.source_record_id) as RNK1
FROM
sq_cc_check
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_cc_check.Doc_Category
QUALIFY RNK1 = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_documenttype.typecode'',''bctl_documenttype.typecode'',''cctl_documenttype.typecode'', ''derived'')
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'', ''DS'') 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_from_source.doc_type
QUALIFY RNK = 1
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
exp_pass_from_source.Ref_key as Ref_key,
LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE.TGT_IDNTFTN_VAL as doc_type,
CASE WHEN LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL END as out_doc_status,
exp_pass_from_source.SRC_CD as SRC_CD,
exp_pass_from_source.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
exp_pass_from_source.Retired as Retired,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
LKP_1.DOC_ID /* replaced lookup LKP_XREF_DOC */ as IN_doc_id,
exp_pass_from_source.UpdateTime as UpdateTime,
exp_pass_from_source.rnk as rnk,
TO_DATE ( ''12/31/9999'' , ''MM/DD/YYYY'' ) as v_PRD_END_DTTM,
v_PRD_END_DTTM as o_PRD_END_DTTM,
CASE WHEN ( UPPER ( exp_pass_from_source.DOC_STATUS ) = ''ISSUED'' ) OR ( UPPER ( exp_pass_from_source.DOC_STATUS ) = ''VOIDED'' ) THEN exp_pass_from_source.UpdateTime ELSE v_PRD_END_DTTM END as DOC_PRD_STRT_DTTM,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK1
FROM
exp_pass_from_source
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE ON exp_pass_from_source.source_record_id = LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE.source_record_id
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_TERADATA_ETL_REF_XLAT ON LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
LEFT JOIN LKP_XREF_DOC LKP_1 ON LKP_1.DOC_ISSUR_NUM = exp_pass_from_source.Ref_key AND LKP_1.DOC_TYPE_CD = LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE.TGT_IDNTFTN_VAL AND LKP_1.DOC_CTGY_TYPE_CD = exp_pass_from_source.out_Doc_Category
QUALIFY RNK1 = 1
);


-- Component LKP_DOC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DOC AS
(
SELECT
LKP.DOC_ID,
LKP.DOC_ISSUR_NUM,
LKP.DOC_PRD_STRT_DTTM,
LKP.DOC_CRTN_DTTM,
LKP.TRANS_STRT_DTTM,
LKP.DOC_TYPE_CD,
LKP.DOC_CTGY_TYPE_CD,
LKP.DOC_STS_CD,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_pass_from_source.Ref_key as Ref_key,
EXPTRANS.doc_type as doc_type,
exp_pass_from_source.out_Doc_Category as Doc_Category,
EXPTRANS.DOC_CRTN_DTTM as DOC_CRTN_DTTM1,
EXPTRANS.EDW_STRT_DTTM as EDW_STRT_DTTM1,
EXPTRANS.EDW_END_DTTM as EDW_END_DTTM1,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.DOC_ID desc,LKP.DOC_ISSUR_NUM desc,LKP.DOC_PRD_STRT_DTTM desc,LKP.DOC_CRTN_DTTM desc,LKP.TRANS_STRT_DTTM desc,LKP.DOC_TYPE_CD desc,LKP.DOC_CTGY_TYPE_CD desc,LKP.DOC_STS_CD desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK2
FROM
exp_pass_from_source
INNER JOIN EXPTRANS ON exp_pass_from_source.source_record_id = EXPTRANS.source_record_id
LEFT JOIN (
SELECT DOC.DOC_ID as DOC_ID, DOC.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM, DOC.DOC_CRTN_DTTM as DOC_CRTN_DTTM, DOC.TRANS_STRT_DTTM as TRANS_STRT_DTTM, DOC.DOC_STS_CD as DOC_STS_CD, DOC.EDW_STRT_DTTM as EDW_STRT_DTTM, DOC.EDW_END_DTTM as EDW_END_DTTM, DOC.DOC_CTGY_TYPE_CD as DOC_CTGY_TYPE_CD, DOC.DOC_ISSUR_NUM as DOC_ISSUR_NUM, DOC.DOC_TYPE_CD as DOC_TYPE_CD FROM DB_T_PROD_CORE.DOC QUALIFY ROW_NUMBER () OVER (PARTITION by DOC_ISSUR_NUM,DOC_CTGY_TYPE_CD,DOC_TYPE_CD ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.DOC_CTGY_TYPE_CD = exp_pass_from_source.out_Doc_Category AND LKP.DOC_ISSUR_NUM = exp_pass_from_source.Ref_key AND LKP.DOC_TYPE_CD = EXPTRANS.doc_type
QUALIFY RNK2 = 1
);
-- Component exp_ins_upd, Type EXPRESSION 
CREATE
OR REPLACE TEMPORARY TABLE exp_ins_upd AS (
  WITH vars AS (
    SELECT
      CAST(NULL AS DECIMAL(38, 0)) AS V_INC
  )
  SELECT
    LKP_DOC.DOC_ID AS lkp_DOC_ID,
    LKP_DOC.DOC_PRD_STRT_DTTM AS lkp_DOC_PRD_STRT_DT,
    LKP_DOC.doc_type AS in_DOC_TYPE,
    LKP_DOC.Ref_key AS in_Ref_key,
    LKP_DOC.Doc_Category AS in_Doc_Category,
    LKP_DOC.DOC_ISSUR_NUM AS lkp_DOC_ISSUR_NUM,
    LKP_DOC.DOC_TYPE_CD AS lkp_DOC_TYPE_CD,
    LKP_DOC.DOC_CTGY_TYPE_CD AS lkp_DOC_CTGY_TYPE_CD,
    LKP_DOC.DOC_STS_CD AS lkp_DOC_STS_CD,
    LKP_DOC.DOC_CRTN_DTTM AS DOC_CRTN_DTTM,
    LKP_DOC.EDW_STRT_DTTM AS LKP_EDW_STRT_DTTM,
    LKP_DOC.EDW_END_DTTM AS LKP_EDW_END_DTTM,
    EXPTRANS.out_doc_status AS out_DOC_STATUS,
    :PRCS_ID AS o_process_id,
    LKP_DOC.EDW_STRT_DTTM AS out_EDW_STRT_DTTM,
    LKP_DOC.EDW_END_DTTM AS out_EDW_END_DTTM,
    LKP_1.TGT_IDNTFTN_VAL AS out_SRC_CD,
    TO_TIMESTAMP(
      ''12/31/9999 23:59:59.999999'',
      ''mm/DD/yyyy hh24:mi:ss.ff6''
    ) AS out_PRD_END_DTTM,
    exp_pass_from_source.Retired AS Retired,
    EXPTRANS.IN_doc_id AS IN_doc_id,
    vars.V_INC AS V_INC,
    vars.V_INC AS V_VAR,
    CASE
      WHEN LKP_DOC.Ref_key = LKP_DOC.DOC_ISSUR_NUM
      AND LKP_DOC.doc_type = LKP_DOC.DOC_TYPE_CD
      AND LKP_DOC.Doc_Category = LKP_DOC.DOC_CTGY_TYPE_CD THEN vars.V_INC
      ELSE EXPTRANS.IN_doc_id
    END AS V_DOC_ID,
    CASE
      WHEN V_INC = 0 THEN EXPTRANS.IN_doc_id
      ELSE V_DOC_ID
    END AS out_DOC_ID,
    EXPTRANS.UpdateTime AS UpdateTime,
    EXPTRANS.DOC_PRD_STRT_DTTM AS DOC_PRD_STRT_DTTM,
    MD5(
      RTRIM(LTRIM(LKP_DOC.DOC_STS_CD)) || TO_CHAR(LKP_DOC.DOC_PRD_STRT_DTTM) || TO_CHAR(LKP_DOC.DOC_CRTN_DTTM)
    ) AS ORIG_CHKSM,
    MD5(
      RTRIM(LTRIM(EXPTRANS.out_doc_status)) || TO_CHAR(EXPTRANS.DOC_PRD_STRT_DTTM) || TO_CHAR(LKP_DOC.DOC_CRTN_DTTM)
    ) AS CALC_CHKSM,
    CASE
      WHEN ORIG_CHKSM IS NULL THEN ''I''
      WHEN ORIG_CHKSM <> CALC_CHKSM THEN ''U''
      ELSE ''R''
    END AS out_updateflag,
    EXPTRANS.rnk AS rnk,
    LKP_DOC.TRANS_STRT_DTTM AS lkp_TRANS_STRT_DTTM,
    exp_pass_from_source.source_record_id,
    ROW_NUMBER() OVER (
      PARTITION BY exp_pass_from_source.source_record_id
      ORDER BY
        exp_pass_from_source.source_record_id
    ) AS RNK2
  FROM
    exp_pass_from_source
    CROSS JOIN vars
    INNER JOIN EXPTRANS ON exp_pass_from_source.source_record_id = EXPTRANS.source_record_id
    INNER JOIN LKP_DOC ON EXPTRANS.source_record_id = LKP_DOC.source_record_id
    LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD AS LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.SRC_CD QUALIFY RNK2 = 1
);


-- Component rtr_check_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_check_ins_upd_INSERT AS
(SELECT
exp_ins_upd.lkp_DOC_ID as lkp_DOC_ID,
exp_ins_upd.in_Ref_key as Ref_key,
exp_ins_upd.in_DOC_TYPE as in_DOC_TYPE,
exp_ins_upd.in_Doc_Category as Doc_Category,
exp_ins_upd.out_DOC_STATUS as o_doc_status,
exp_ins_upd.out_updateflag as o_modified,
exp_ins_upd.o_process_id as Process_id,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_ins_upd.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_upd.out_SRC_CD as out_SRC_CD,
exp_ins_upd.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
exp_ins_upd.out_PRD_END_DTTM as out_PRD_END_DTTM,
exp_ins_upd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_upd.Retired as Retired,
exp_ins_upd.IN_doc_id as IN_doc_id,
exp_ins_upd.lkp_DOC_PRD_STRT_DT as lkp_DOC_PRD_STRT_DT,
exp_ins_upd.lkp_DOC_ISSUR_NUM as lkp_DOC_ISSUR_NUM,
exp_ins_upd.lkp_DOC_TYPE_CD as lkp_DOC_TYPE_CD,
exp_ins_upd.lkp_DOC_CTGY_TYPE_CD as lkp_DOC_CTGY_TYPE_CD,
exp_ins_upd.lkp_DOC_STS_CD as lkp_DOC_STS_CD,
exp_ins_upd.UpdateTime as UpdateTime,
exp_ins_upd.rnk as rnk,
exp_ins_upd.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM,
exp_ins_upd.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.out_updateflag = ''I'' 
-- Insert flow 
---- OR 
-- exp_ins_upd.Retired flow 
-- ( exp_ins_upd.Retired = 0 AND exp_ins_upd.LKP_EDW_END_DTTM != TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) or 
-- Update flow starts. For Status not in Issued and Voided , exp_ins_upd.DOC_PRD_STRT_DTTM is default date i.e 12 / 31 / 9999 which will always be greater than exp_ins_upd.DOC_PRD_STRT_DTTM for Voided and Issue status.Hence below Cases are required to complete flow 
--- ( exp_ins_upd.out_updateflag = ''U'' AND lower ( exp_ins_upd.in_DOC_TYPE ) <> ''chck'' and lower ( exp_ins_upd.in_Doc_Category ) <> ''claim'' ) 
-- When record got modified and doc doesn''t fall in check category  
---EIM-13808 latest DOC_STATUS is not populated properly in Combined check  or   (o_modified ='' U'' AND  (lower(in_DOC_TYPE) = '' cmbndchck''  or (lower(in_DOC_TYPE) = '' chck'') ) and lower(Doc_Category) ='' claim'' and  DECODE(o_doc_status,'' ISSD'',0,'' VD'',0,1))
--when record got modfied,its of check category but Status is not Issued or Voided. Insert this record wihout checking DOC_PRD_STRT_DTTM   OR  (o_modified ='' U'' AND (lower(in_DOC_TYPE) = '' cmbndchck''  or (lower(in_DOC_TYPE) = '' chck'') ) and lower(Doc_Category) ='' claim''  and DECODE(o_doc_status,'' ISSD'',1,'' VD'',1,0)  AND DECODE(lkp_DOC_STS_CD,'' ISSD'',1,'' VD'',1,0)    and DOC_PRD_STRT_DTTM> lkp_DOC_PRD_STRT_DT ) 
--when record got modfied,its of check category and  Status is Issued or Voided. Insert this record if previous status is either issued or voided and DOC_PRD_STRT_DTTM> lkp_DOC_PRD_STRT_DT  OR  (o_modified ='' U'' AND (lower(in_DOC_TYPE) = '' cmbndchck''  or (lower(in_DOC_TYPE) = '' chck'') ) and lower(Doc_Category) ='' claim''  and DECODE(o_doc_status,'' ISSD'',1,'' VD'',1,0) AND DECODE(lkp_DOC_STS_CD,'' ISSD'',0,'' VD'',0,1)    and UpdateTime>lkp_TRANS_STRT_DTTM )  
--when record got modfied,its of check category and  Status is Issued or Voided. Insert this record if previous status is not in Issued and Voided and updatetime of current record should be greater than previous record  OR  (o_modified ='' U'' AND (lower(in_DOC_TYPE) = '' cmbndchck''  or (lower(in_DOC_TYPE) = '' chck'') ) and lower(Doc_Category) ='' claim''  and DECODE(o_doc_status,'' ISSD'',1,'' VD'',1,0) AND DECODE(lkp_DOC_STS_CD,'' AWSBMSSN'',1,'' REQSTD'',1,0) )    --when record got modfied,its of check category and  Status is Issued or Voided. Insert this record if previous status is submission and requested  or  (o_modified ='' U'' AND  (lower(in_DOC_TYPE) = '' chck''  or (lower(in_DOC_TYPE) = '' cmbndchck'') ) and lower(Doc_Category) ='' bill'' );
);

-- Component rtr_check_ins_upd_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_check_ins_upd_Retired AS
(SELECT
exp_ins_upd.lkp_DOC_ID as lkp_DOC_ID,
exp_ins_upd.in_Ref_key as Ref_key,
exp_ins_upd.in_DOC_TYPE as in_DOC_TYPE,
exp_ins_upd.in_Doc_Category as Doc_Category,
exp_ins_upd.out_DOC_STATUS as o_doc_status,
exp_ins_upd.out_updateflag as o_modified,
exp_ins_upd.o_process_id as Process_id,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_ins_upd.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_ins_upd.out_SRC_CD as out_SRC_CD,
exp_ins_upd.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
exp_ins_upd.out_PRD_END_DTTM as out_PRD_END_DTTM,
exp_ins_upd.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_ins_upd.Retired as Retired,
exp_ins_upd.IN_doc_id as IN_doc_id,
exp_ins_upd.lkp_DOC_PRD_STRT_DT as lkp_DOC_PRD_STRT_DT,
exp_ins_upd.lkp_DOC_ISSUR_NUM as lkp_DOC_ISSUR_NUM,
exp_ins_upd.lkp_DOC_TYPE_CD as lkp_DOC_TYPE_CD,
exp_ins_upd.lkp_DOC_CTGY_TYPE_CD as lkp_DOC_CTGY_TYPE_CD,
exp_ins_upd.lkp_DOC_STS_CD as lkp_DOC_STS_CD,
exp_ins_upd.UpdateTime as UpdateTime,
exp_ins_upd.rnk as rnk,
exp_ins_upd.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM,
exp_ins_upd.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.out_updateflag = ''R'' and exp_ins_upd.Retired != 0 and exp_ins_upd.LKP_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_doc_upd_Retired_Rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_doc_upd_Retired_Rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_check_ins_upd_Retired.lkp_DOC_ID as lkp_DOC_ID3,
rtr_check_ins_upd_Retired.Process_id as Process_id3,
rtr_check_ins_upd_Retired.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_check_ins_upd_Retired.Retired as Retired3,
rtr_check_ins_upd_Retired.lkp_DOC_PRD_STRT_DT as lkp_DOC_PRD_STRT_DT4,
rtr_check_ins_upd_Retired.lkp_DOC_ISSUR_NUM as lkp_DOC_ISSUR_NUM4,
rtr_check_ins_upd_Retired.lkp_DOC_TYPE_CD as lkp_DOC_TYPE_CD4,
rtr_check_ins_upd_Retired.lkp_DOC_CTGY_TYPE_CD as lkp_DOC_CTGY_TYPE_CD4,
rtr_check_ins_upd_Retired.lkp_DOC_STS_CD as lkp_DOC_STS_CD4,
rtr_check_ins_upd_Retired.UpdateTime as UpdateTime4,
1 as UPDATE_STRATEGY_ACTION,rtr_check_ins_upd_Retired.source_record_id
FROM
rtr_check_ins_upd_Retired
);


-- Component upd_doc_ins_new, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_doc_ins_new AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_check_ins_upd_INSERT.Ref_key as Ref_key1,
rtr_check_ins_upd_INSERT.in_DOC_TYPE as in_DOC_TYPE,
rtr_check_ins_upd_INSERT.Doc_Category as Doc_Category1,
rtr_check_ins_upd_INSERT.o_doc_status as o_doc_status1,
rtr_check_ins_upd_INSERT.Process_id as Process_id,
rtr_check_ins_upd_INSERT.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
rtr_check_ins_upd_INSERT.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_check_ins_upd_INSERT.out_SRC_CD as out_SRC_CD1,
rtr_check_ins_upd_INSERT.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
rtr_check_ins_upd_INSERT.out_PRD_END_DTTM as out_PRD_END_DTTM1,
rtr_check_ins_upd_INSERT.Retired as Retired1,
rtr_check_ins_upd_INSERT.IN_doc_id as IN_doc_id1,
rtr_check_ins_upd_INSERT.UpdateTime as UpdateTime1,
rtr_check_ins_upd_INSERT.rnk as rnk1,
rtr_check_ins_upd_INSERT.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM1,
0 as UPDATE_STRATEGY_ACTION,rtr_check_ins_upd_INSERT.source_record_id
FROM
rtr_check_ins_upd_INSERT
);


-- Component exp_pass_to_tgt_Retired_Rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_Retired_Rejected AS
(
SELECT
upd_doc_upd_Retired_Rejected.lkp_DOC_ID3 as lkp_DOC_ID3,
upd_doc_upd_Retired_Rejected.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_doc_upd_Retired_Rejected.lkp_DOC_PRD_STRT_DT4 as lkp_DOC_PRD_STRT_DT4,
upd_doc_upd_Retired_Rejected.lkp_DOC_ISSUR_NUM4 as lkp_DOC_ISSUR_NUM4,
upd_doc_upd_Retired_Rejected.lkp_DOC_TYPE_CD4 as lkp_DOC_TYPE_CD4,
upd_doc_upd_Retired_Rejected.lkp_DOC_CTGY_TYPE_CD4 as lkp_DOC_CTGY_TYPE_CD4,
upd_doc_upd_Retired_Rejected.lkp_DOC_STS_CD4 as lkp_DOC_STS_CD4,
upd_doc_upd_Retired_Rejected.UpdateTime4 as UpdateTime4,
upd_doc_upd_Retired_Rejected.source_record_id
FROM
upd_doc_upd_Retired_Rejected
);


-- Component exp_ins_pass_to_target_new, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target_new AS
(
SELECT
upd_doc_ins_new.Ref_key1 as Ref_key1,
upd_doc_ins_new.in_DOC_TYPE as in_DOC_TYPE,
upd_doc_ins_new.Doc_Category1 as Doc_Category1,
upd_doc_ins_new.o_doc_status1 as o_doc_status1,
upd_doc_ins_new.Process_id as Process_id,
CASE
  WHEN upd_doc_ins_new.Retired1 = 0 THEN DATEADD (
    SECOND,
    (2 * (upd_doc_ins_new.rnk1 - 1)),
    CURRENT_TIMESTAMP()
  )
  ELSE CURRENT_TIMESTAMP()
END AS out_EDW_STRT_DTTM1,
CASE WHEN upd_doc_ins_new.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_doc_ins_new.out_EDW_END_DTTM1 END as o_EDW_END_DTTM,
upd_doc_ins_new.out_SRC_CD1 as out_SRC_CD1,
upd_doc_ins_new.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
upd_doc_ins_new.out_PRD_END_DTTM1 as out_PRD_END_DTTM1,
upd_doc_ins_new.IN_doc_id1 as IN_doc_id1,
upd_doc_ins_new.UpdateTime1 as TRANS_STRT_DTTM,
CASE WHEN upd_doc_ins_new.Retired1 = 0 THEN TO_TIMESTAMP ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ELSE upd_doc_ins_new.UpdateTime1 END as TRANS_END_DTTM,
upd_doc_ins_new.DOC_PRD_STRT_DTTM1 as DOC_PRD_STRT_DTTM,
upd_doc_ins_new.source_record_id
FROM
upd_doc_ins_new
);


-- Component tgt_doc_upd_Retired_Rejected, Type TARGET 
MERGE INTO DB_T_PROD_CORE.DOC
USING exp_pass_to_tgt_Retired_Rejected ON (DOC.DOC_ID = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_ID3 AND DOC.EDW_STRT_DTTM = exp_pass_to_tgt_Retired_Rejected.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
DOC_ID = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_ID3,
DOC_PRD_STRT_DTTM = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_PRD_STRT_DT4,
DOC_ISSUR_NUM = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_ISSUR_NUM4,
DOC_TYPE_CD = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_TYPE_CD4,
DOC_CTGY_TYPE_CD = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_CTGY_TYPE_CD4,
DOC_STS_CD = exp_pass_to_tgt_Retired_Rejected.lkp_DOC_STS_CD4,
EDW_STRT_DTTM = exp_pass_to_tgt_Retired_Rejected.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_Retired_Rejected.EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_tgt_Retired_Rejected.UpdateTime4;


-- Component tgt_doc_ins_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.DOC
(
DOC_ID,
DOC_CRTN_DTTM,
DOC_PRD_STRT_DTTM,
DOC_PRD_END_DTTM,
DOC_ISSUR_NUM,
DOC_TYPE_CD,
DOC_CTGY_TYPE_CD,
DOC_STS_CD,
SRC_SYS_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_ins_pass_to_target_new.IN_doc_id1 as DOC_ID,
exp_ins_pass_to_target_new.DOC_CRTN_DTTM as DOC_CRTN_DTTM,
exp_ins_pass_to_target_new.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM,
exp_ins_pass_to_target_new.out_PRD_END_DTTM1 as DOC_PRD_END_DTTM,
exp_ins_pass_to_target_new.Ref_key1 as DOC_ISSUR_NUM,
exp_ins_pass_to_target_new.in_DOC_TYPE as DOC_TYPE_CD,
exp_ins_pass_to_target_new.Doc_Category1 as DOC_CTGY_TYPE_CD,
exp_ins_pass_to_target_new.o_doc_status1 as DOC_STS_CD,
exp_ins_pass_to_target_new.out_SRC_CD1 as SRC_SYS_CD,
exp_ins_pass_to_target_new.Process_id as PRCS_ID,
exp_ins_pass_to_target_new.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_pass_to_target_new.o_EDW_END_DTTM as EDW_END_DTTM,
exp_ins_pass_to_target_new.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_pass_to_target_new.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_ins_pass_to_target_new;


-- Component tgt_doc_ins_new, Type Post SQL 
UPDATE  DB_T_PROD_CORE.DOC FROM                                                                            

(SELECT DISTINCT DOC_CTGY_TYPE_CD,DOC_ISSUR_NUM,DOC_TYPE_CD,EDW_STRT_DTTM,DOC_STS_CD,                                                                         

MAX(TRANS_STRT_DTTM) OVER (PARTITION BY DOC_CTGY_TYPE_CD,DOC_ISSUR_NUM,DOC_TYPE_CD ORDER BY EDW_STRT_DTTM ASC,  CAST(TRANS_STRT_DTTM AS DATE) ASC ,TRANS_STRT_DTTM ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND''                                                                           

 AS LEAD,                                                                           

MAX(EDW_STRT_DTTM) OVER (PARTITION BY DOC_CTGY_TYPE_CD,DOC_ISSUR_NUM,DOC_TYPE_CD ORDER BY  EDW_STRT_DTTM ASC ,CAST(TRANS_STRT_DTTM AS DATE)  ASC ,TRANS_STRT_DTTM ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) - INTERVAL ''1 SECOND''                                                                             

 AS LEAD1                                                                           

FROM DB_T_PROD_CORE.DOC                                                                            

                                                                        

 ) A                                                                            

SET TRANS_END_DTTM=  A.LEAD,                                                                            

EDW_END_DTTM=A.LEAD1                                                                            

WHERE  DOC.EDW_STRT_DTTM = A.EDW_STRT_DTTM                                                                          

AND DOC.DOC_CTGY_TYPE_CD=A.DOC_CTGY_TYPE_CD                                                                             

AND DOC.DOC_ISSUR_NUM=A.DOC_ISSUR_NUM                                                                           

AND DOC.DOC_TYPE_CD=A.DOC_TYPE_CD 

AND DOC.DOC_STS_CD=A.DOC_STS_CD 

AND DOC.TRANS_STRT_DTTM <>DOC.TRANS_END_DTTM                                                                            

AND LEAD IS NOT NULL;


END; ';