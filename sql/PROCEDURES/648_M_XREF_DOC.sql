-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_XREF_DOC("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name  order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');


-- Component LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_CTGY_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


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

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_documenttype.typecode'',''bctl_documenttype.typecode'',''cctl_documenttype.typecode'', ''derived'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'', ''DS'') 

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


-- Component sq_doc, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_doc AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Ref_key,
$2 as doc_type,
$3 as Doc_Category,
$4 as SRC_CD,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT * 

FROM (

SELECT DISTINCT  cast(A.publicid_stg as varchar(1000)) AS REF_KEY, CAST(''DOC_TYPE1'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

''SRC_SYS6'' AS SRC_CD

/* FROM DB_T_PROD_STAG.CC_CHECK, DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS  */
/* WHERE CC_CHECK.STATUS_stg=CCTL_TRANSACTIONSTATUS.ID_stg  AND   cc_check.publicid_stg  IS  NOT NULL --AND cc_check.CLAIMNUMBER IS NOT NULL  */


FROM

(

SELECT distinct 

cc_check.PublicID_stg

FROM

(select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.CC_CHECK on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where 

cc_check.UpdateTime_stg>(:start_dttm) AND cc_check.UpdateTime_stg <= (:end_dttm) and  

cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''

AND CC_CHECK.STATUS_stg=CCTL_TRANSACTIONSTATUS.ID_stg  AND   cc_check.publicid_stg  IS  NOT NULL AND cc_claim.CLAIMNUMBER_stg IS NOT NULL 

) A

UNION

SELECT INVOICENUMBER_stg AS REF_KEY, ''DOC_TYPE3'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY ,''SRC_SYS5'' AS SRC_CD

FROM DB_T_PROD_STAG.BC_INVOICE  WHERE INVOICENUMBER_stg IS  NOT NULL

and bc_invoice.UpdateTime_stg > (:start_dttm)

and bc_invoice.UpdateTime_stg <= (:end_dttm)

UNION

SELECT DISTINCT SourceFile_stg AS REF_KEY, ''DOC_TYPE5''  AS DOC_TYPE, ''DOC_CTGY_TYPE5'' AS DOC_CATEGORY ,''SRC_SYS4'' AS SRC_CD

FROM DB_T_PROD_STAG.pc_questionlookup

WHERE pc_questionlookup.UpdateTime_stg > (:start_dttm)	and pc_questionlookup.UpdateTime_stg <= (:end_dttm)

UNION

SELECT CAST(BCO.id_stg AS VARCHAR(50)) AS REF_KEY,

''DOC_TYPE1'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY,

''SRC_SYS5'' AS SRC_CD

FROM DB_T_PROD_STAG.BC_OUTGOINGPAYMENT BCO

LEFT OUTER JOIN DB_T_PROD_STAG.BCTL_OUTGOINGPAYMENTSTATUS BCOPS 

ON BCO.STATUS_stg=BCOPS.ID_stg

WHERE 

BCO.UpdateTime_stg > (:start_dttm)

and BCO.UpdateTime_stg <= (:end_dttm)

UNION

SELECT  CAST(PCD.DocumentIdentifier_stg AS VARCHAR(50)) AS REF_KEY,

cast(null AS varchar(100)) as  DOC_TYPE,

cast(null as varchar(100))  AS DOC_CATEGORY,

''SRC_SYS4'' AS SRC_CD

FROM

DB_T_PROD_STAG.pc_document PCD

where PCD.DocumentIdentifier_stg is not null

AND PCD.UpdateTime_stg > (:start_dttm)

and PCD.UpdateTime_stg <= (:end_dttm)

qualify ROW_NUMBER() OVER  (partition by DocumentIdentifier_stg/*,CTL_ID_stg, PROCESS_ID_stg*/ order by DateCreated_stg desc)=1

union 



SELECT REFNUMBER_stg AS REF_KEY,''DOC_TYPE1'' AS DOC_TYPE,''DOC_CTGY_TYPE4'' AS DOC_CATEGORY,

''SRC_SYS5'' AS SRC_CD

FROM DB_T_PROD_STAG.BC_BASEMONEYRECEIVED WHERE REFNUMBER_stg  IS NOT NULL 

AND BC_BASEMONEYRECEIVED.UpdateTime_stg > (:start_dttm)

and BC_BASEMONEYRECEIVED.UpdateTime_stg <= (:end_dttm)



union



SELECT DISTINCT  cast(CC.CombinedCheckNumber_alfa_stg as varchar(1000)) AS REF_KEY, CAST(''DOC_TYPE6'' AS VARCHAR(50)) AS DOC_TYPE,CAST(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS DOC_CATEGORY,

''SRC_SYS6'' AS SRC_CD

FROM DB_T_PROD_STAG.CC_CHECK CC, DB_T_PROD_STAG.CCTL_TRANSACTIONSTATUS TS

WHERE CC.STATUS_stg=TS.ID_stg  

/* AND CCTL_TRANSACTIONSTATUS.TYPECODE <> ''voided''   */
AND CC.CombinedCheckNumber_alfa_stg  IS  NOT NULL /* AND CLAIMNUMBER_stg IS NOT NULL */
AND CC.UpdateTime_stg > (:start_dttm)

and CC.UpdateTime_stg <= (:end_dttm)



) B  

/* ORDER BY ref_key,doc_type,doc_category,Createtime ASC nulls LAST, Retired ASC  */
QUALIFY ROW_NUMBER() OVER (PARTITION BY ref_key,doc_type,doc_category  ORDER BY doc_type, ref_key)=1
) SRC
)
);


-- Component exp_doc_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_doc_trans AS
(
SELECT DISTINCT
sq_doc.Ref_key as Ref_key,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */ as v_doc_type,
v_doc_type as o_doc_type,
CASE WHEN TRIM(rtrim ( ltrim ( sq_doc.Doc_Category ) )) = '''' OR rtrim ( ltrim ( sq_doc.Doc_Category ) ) IS NULL OR LENGTH ( rtrim ( ltrim ( sq_doc.Doc_Category ) ) ) = 0 THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD */ END as v_Doc_Category,
v_Doc_Category as o_Doc_Category,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_SRC_CD,
CURRENT_TIMESTAMP as LOAD_DTTM,
LKP_4.DOC_ID /* replaced lookup LKP_XREF_DOC */ as v_lkp_doc_id,
CASE WHEN v_lkp_doc_id IS NULL THEN ''I'' ELSE ''R'' END as o_ins_upd/*,
sq_doc.source_record_id,
row_number() over (partition by sq_doc.source_record_id order by sq_doc.source_record_id) as RNK*/
FROM
sq_doc
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_doc.doc_type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = rtrim ( ltrim ( sq_doc.Doc_Category ) )
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_doc.SRC_CD
LEFT JOIN LKP_XREF_DOC LKP_4 ON LKP_4.DOC_ISSUR_NUM = sq_doc.Ref_key AND COALESCE(LKP_4.DOC_TYPE_CD,'') = COALESCE(v_doc_type,'') AND COALESCE(LKP_4.DOC_CTGY_TYPE_CD,'') =COALESCE( v_Doc_Category,'')
--QUALIFY RNK = 1
);


-- Component fil_doc, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_doc AS
(
SELECT
exp_doc_trans.Ref_key as Ref_key,
exp_doc_trans.o_doc_type as o_doc_type,
exp_doc_trans.o_Doc_Category as o_Doc_Category,
exp_doc_trans.o_SRC_CD as o_SRC_CD,
exp_doc_trans.LOAD_DTTM as LOAD_DTTM,
exp_doc_trans.o_ins_upd as o_ins_upd/*,
exp_doc_trans.source_record_id*/
FROM
exp_doc_trans
WHERE exp_doc_trans.o_ins_upd = ''I''
);


-- Component DIR_DOC, Type TARGET 
INSERT INTO DB_T_PROD_CORE.DIR_DOC
(
DOC_ID,
DOC_ISSUR_NUM,
DOC_TYPE_CD,
DOC_CTGY_TYPE_CD,
SRC_SYS_CD,
LOAD_DTTM
)
SELECT
public.seq_DOC.nextval as DOC_ID,
fil_doc.Ref_key as DOC_ISSUR_NUM,
fil_doc.o_doc_type as DOC_TYPE_CD,
fil_doc.o_Doc_Category as DOC_CTGY_TYPE_CD,
fil_doc.o_SRC_CD as SRC_SYS_CD,
fil_doc.LOAD_DTTM as LOAD_DTTM
FROM
fil_doc;


END; 
';