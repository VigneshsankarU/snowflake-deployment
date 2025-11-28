-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AR_INVOICE_LINE_AMOUNT_INSUPD("RUN_ID" VARCHAR)
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

-- Component LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_CTGY_TYPE''

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

    		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_bc_invoiceitem, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_bc_invoiceitem AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as InvoiceNumber,
$2 as ID,
$3 as TYPECODE,
$4 as CreateTime,
$5 as Amount,
$6 as PaidAmount,
$7 as Retired,
$8 as UpdateTime,
$9 as ChargeCode,
$10 as ChargeName,
$11 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT distinct bc_invoice.InvoiceNumber_stg as InvoiceNumber, bc_invoiceitem.ID_stg as ID, bctl_chargecategory.Typecode_stg as Typecode, 

case when bc_invoiceitem.CreateTime_stg is null 

then to_date(''1900-01-01'',''yyyy-mm-dd'')

else  bc_invoiceitem.CreateTime_stg end as CreateTime , bc_invoiceitem.Amount_stg as Amount, bc_invoiceitem.PaidAmount_stg as PaidAmount, bc_invoiceitem.retired_stg as retired, bc_invoiceitem.UpdateTime_stg as UpdateTime, bc_chargepattern.ChargeCode_stg as ChargeCode,

bc_chargepattern.ChargeName_stg as ChargeName

FROM	

DB_T_PROD_STAG.bc_invoice inner join DB_T_PROD_STAG.bc_invoiceitem 

	on	bc_invoice.id_stg=bc_invoiceitem.InvoiceID_stg

inner join DB_T_PROD_STAG.bctl_invoiceitemtype 

	on	bctl_invoiceitemtype.id_stg=bc_invoiceitem.Type_stg

inner join DB_T_PROD_STAG.bc_charge 

	on	bc_charge.id_stg=bc_invoiceitem.ChargeID_stg

inner join DB_T_PROD_STAG.bc_chargepattern 

	on	bc_chargepattern.id_stg=bc_charge.ChargePatternID_stg

inner join DB_T_PROD_STAG.bctl_chargecategory 

	on	bctl_chargecategory.id_stg=bc_chargepattern.Category_stg

where	

bc_invoiceitem.UpdateTime_stg > (:start_dttm)

	and	bc_invoiceitem.UpdateTime_stg <= (:end_dttm)
) SRC
)
);


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
SQ_bc_invoiceitem.InvoiceNumber as InvoiceNumber,
SQ_bc_invoiceitem.ID as ID,
SQ_bc_invoiceitem.TYPECODE as TYPECODE,
SQ_bc_invoiceitem.CreateTime as CreateTime,
SQ_bc_invoiceitem.Amount as Amount,
SQ_bc_invoiceitem.PaidAmount as PaidAmount,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */ as doc_type_cd,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */ as doc_ctgy_type_cd,
SQ_bc_invoiceitem.Retired as Retired,
SQ_bc_invoiceitem.UpdateTime as UpdateTime,
SQ_bc_invoiceitem.ChargeCode as ChargeCode,
SQ_bc_invoiceitem.source_record_id,
row_number() over (partition by SQ_bc_invoiceitem.source_record_id order by SQ_bc_invoiceitem.source_record_id) as RNK
FROM
SQ_bc_invoiceitem
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''DOC_TYPE3''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = ''DOC_CTGY_TYPE4''
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INVC_AMT_SBTYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM=''bc_chargepattern.chargecode''
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp.ChargeCode
QUALIFY RNK = 1
);


-- Component LKP_DOC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DOC AS
(
SELECT
LKP.DOC_ID,
exp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.DOC_ID desc,LKP.TM_PRD_CD desc,LKP.DOC_CRTN_DTTM desc,LKP.DOC_RECPT_DT desc,LKP.DOC_PRD_STRT_DTTM desc,LKP.DOC_PRD_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.DOC_ISSUR_NUM desc,LKP.DATA_SRC_TYPE_CD desc,LKP.DOC_DESC_TXT desc,LKP.DOC_NAME desc,LKP.DOC_HOST_NUM desc,LKP.DOC_HOST_VERS_NUM desc,LKP.DOC_CYCL_CD desc,LKP.DOC_TYPE_CD desc,LKP.MM_OBJT_ID desc,LKP.DOC_CTGY_TYPE_CD desc,LKP.LANG_TYPE_CD desc,LKP.PRCS_ID desc,LKP.DOC_STS_CD desc) RNK
FROM
exp
LEFT JOIN (
SELECT DOC.DOC_ID as DOC_ID, DOC.TM_PRD_CD as TM_PRD_CD, DOC.DOC_CRTN_DTTM as DOC_CRTN_DTTM, DOC.DOC_RECPT_DT as DOC_RECPT_DT, DOC.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM, DOC.DOC_PRD_END_DTTM as DOC_PRD_END_DTTM, DOC.EDW_STRT_DTTM as EDW_STRT_DTTM, DOC.DATA_SRC_TYPE_CD as DATA_SRC_TYPE_CD, DOC.DOC_DESC_TXT as DOC_DESC_TXT, DOC.DOC_NAME as DOC_NAME, DOC.DOC_HOST_NUM as DOC_HOST_NUM, DOC.DOC_HOST_VERS_NUM as DOC_HOST_VERS_NUM, DOC.DOC_CYCL_CD as DOC_CYCL_CD, DOC.MM_OBJT_ID as MM_OBJT_ID, DOC.LANG_TYPE_CD as LANG_TYPE_CD, DOC.PRCS_ID as PRCS_ID, DOC.DOC_STS_CD as DOC_STS_CD, DOC.DOC_ISSUR_NUM as DOC_ISSUR_NUM, DOC.DOC_TYPE_CD as DOC_TYPE_CD, DOC.DOC_CTGY_TYPE_CD as DOC_CTGY_TYPE_CD FROM DB_T_PROD_CORE.DOC
QUALIFY ROW_NUMBER () OVER (PARTITION BY DOC_ISSUR_NUM,DOC_CTGY_TYPE_CD,DOC_TYPE_CD ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.DOC_ISSUR_NUM = exp.InvoiceNumber AND LKP.DOC_TYPE_CD = exp.doc_type_cd AND LKP.DOC_CTGY_TYPE_CD = exp.doc_ctgy_type_cd
QUALIFY RNK = 1
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
LKP_DOC.DOC_ID as DOC_ID,
exp.ID as ID,
LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TYPECODE,
exp.CreateTime as CreateTime,
exp.Amount as Amount,
exp.PaidAmount as PaidAmount,
exp.Retired as Retired,
exp.UpdateTime as UpdateTime,
exp.source_record_id
FROM
exp
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE LKP_TERADATA_ETL_REF_XLAT ON exp.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
INNER JOIN LKP_DOC ON 
exp.source_record_id = LKP_DOC.source_record_id
);


-- Component LKP_AR_INVOICE_LINE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AR_INVOICE_LINE AS
(
SELECT
LKP.AR_INVC_LN_NUM,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.AR_INVC_ID asc,LKP.AR_INVC_LN_NUM asc,LKP.AR_INVC_LN_DESC_TXT asc,LKP.AR_INVC_LN_TYPE_CD asc,LKP.HOST_INVC_LN_NUM asc,LKP.SET_OF_BKS_CD asc,LKP.AR_INVC_LN_COA_VAL asc,LKP.AR_INVC_LN_PROD_ID asc,LKP.PLCY_AGMT_ID asc,LKP.INSTLMT_NUM asc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT	AR_INVC_LN.AR_INVC_LN_NUM as AR_INVC_LN_NUM, AR_INVC_LN.AR_INVC_LN_DESC_TXT as AR_INVC_LN_DESC_TXT,
		AR_INVC_LN.AR_INVC_LN_TYPE_CD as AR_INVC_LN_TYPE_CD, AR_INVC_LN.SET_OF_BKS_CD as SET_OF_BKS_CD,
		AR_INVC_LN.AR_INVC_LN_COA_VAL as AR_INVC_LN_COA_VAL, AR_INVC_LN.AR_INVC_LN_PROD_ID as AR_INVC_LN_PROD_ID,
		AR_INVC_LN.PLCY_TERM_AGMT_ID as PLCY_AGMT_ID, AR_INVC_LN.INSTLMT_NUM as INSTLMT_NUM,
		AR_INVC_LN.AR_INVC_ID as AR_INVC_ID, AR_INVC_LN.HOST_INVC_LN_NUM as HOST_INVC_LN_NUM 
FROM	DB_T_PROD_CORE.AR_INVC_LN
QUALIFY	ROW_NUMBER() OVER(PARTITION BY AR_INVC_LN.HOST_INVC_LN_NUM 
ORDER	BY AR_INVC_LN.EDW_END_DTTM desc) = 1
) LKP ON LKP.HOST_INVC_LN_NUM = exp_pass_through.ID
QUALIFY RNK = 1
);


-- Component LKP_AR_INVC_LN_AMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AR_INVC_LN_AMT AS
(
SELECT
LKP.AR_INVC_ID,
LKP.AR_INVC_LN_NUM,
LKP.AR_INVC_LN_AMT_TYPE_CD,
LKP.AR_INVC_LN_AMT_DTTM,
LKP.AR_INVC_LN_AMT_TRANS_AMT,
LKP.AR_INVC_LN_AMT_PD_AMT,
LKP.AR_INVC_LN_AMT_SBTYPE_CD,
LKP.AR_INVC_LN_AMT_STRT_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.AR_INVC_ID asc,LKP.AR_INVC_LN_NUM asc,LKP.AR_INVC_LN_AMT_TYPE_CD asc,LKP.AR_INVC_LN_AMT_DTTM asc,LKP.AR_INVC_LN_AMT_TRANS_AMT asc,LKP.AR_INVC_LN_AMT_PD_AMT asc,LKP.AR_INVC_LN_AMT_SBTYPE_CD asc,LKP.AR_INVC_LN_AMT_STRT_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_pass_through
INNER JOIN LKP_AR_INVOICE_LINE ON exp_pass_through.source_record_id = LKP_AR_INVOICE_LINE.source_record_id
LEFT JOIN (
SELECT	AR_INVC_LN_AMT.AR_INVC_ID as AR_INVC_ID, AR_INVC_LN_AMT.AR_INVC_LN_NUM as AR_INVC_LN_NUM, AR_INVC_LN_AMT.AR_INVC_LN_AMT_TYPE_CD as AR_INVC_LN_AMT_TYPE_CD, AR_INVC_LN_AMT.AR_INVC_LN_AMT_DTTM as AR_INVC_LN_AMT_DTTM,
		AR_INVC_LN_AMT.AR_INVC_LN_AMT_TRANS_AMT as AR_INVC_LN_AMT_TRANS_AMT, AR_INVC_LN_AMT.AR_INVC_LN_AMT_PD_AMT as AR_INVC_LN_AMT_PD_AMT,	AR_INVC_LN_AMT.AR_INVC_LN_AMT_SBTYPE_CD as  AR_INVC_LN_AMT_SBTYPE_CD,
		AR_INVC_LN_AMT.AR_INVC_LN_AMT_STRT_DTTM as AR_INVC_LN_AMT_STRT_DTTM, AR_INVC_LN_AMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AR_INVC_LN_AMT.EDW_END_DTTM as EDW_END_DTTM
FROM	DB_T_PROD_CORE.AR_INVC_LN_AMT 
QUALIFY	ROW_NUMBER() OVER(PARTITION BY AR_INVC_LN_AMT.AR_INVC_LN_AMT_TYPE_CD,
		AR_INVC_LN_AMT.AR_INVC_LN_NUM 
ORDER	BY AR_INVC_LN_AMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.AR_INVC_LN_NUM = LKP_AR_INVOICE_LINE.AR_INVC_LN_NUM AND LKP.AR_INVC_LN_AMT_TYPE_CD = exp_pass_through.TYPECODE
QUALIFY RNK = 1
);


-- Component exp_check_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check_flag AS
(
SELECT
LKP_AR_INVC_LN_AMT.AR_INVC_ID as lkp_AR_INVC_ID,
LKP_AR_INVC_LN_AMT.AR_INVC_LN_NUM as lkp_AR_INVC_LN_NUM,
LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_TYPE_CD as lkp_AR_INVC_LN_AMT_TYPE_CD,
LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_DTTM as lkp_AR_INVC_LN_AMT_DTTM,
LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_TRANS_AMT as lkp_AR_INVC_LN_AMT_TRANS_AMT,
LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_PD_AMT as lkp_AR_INVC_LN_AMT_PD_AMT,
LKP_AR_INVC_LN_AMT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_AR_INVC_LN_AMT.EDW_END_DTTM as lkp_EDW_END_DTTM,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
exp_pass_through.DOC_ID as DOC_ID,
LKP_AR_INVOICE_LINE.AR_INVC_LN_NUM as ID,
exp_pass_through.TYPECODE as TYPECODE,
exp_pass_through.CreateTime as CreateTime,
exp_pass_through.Amount as Amount,
exp_pass_through.PaidAmount as PaidAmount,
:PRCS_ID as prcsid,
to_date ( ''1000-01-01'' , ''YYYY-DD-MM'' ) as o_DefaultDate,
exp_pass_through.Retired as Retired,
exp_pass_through.UpdateTime as UpdateTime,
DATEADD (SECOND, -1, exp_pass_through.UpdateTime) AS TRANS_END_DTTM_upd,
to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM_ins,
LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE.TGT_IDNTFTN_VAL as AR_INVC_LN_AMT_SBTYPE_CD2,
MD5 ( ltrim ( rtrim ( to_char ( LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_DTTM , ''yyyy-mm-dd'' ) ) ) || ltrim ( rtrim ( to_char ( LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_TRANS_AMT ) ) ) || ltrim ( rtrim ( to_char ( LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_PD_AMT ) ) ) || ltrim ( rtrim ( to_char ( LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_STRT_DTTM , ''yyyy-mm-dd'' ) ) ) || TO_CHAR ( LKP_AR_INVC_LN_AMT.AR_INVC_ID ) || LKP_AR_INVC_LN_AMT.AR_INVC_LN_AMT_SBTYPE_CD ) as var_orig_chksm,
MD5 ( ltrim ( rtrim ( to_char ( exp_pass_through.CreateTime , ''yyyy-mm-dd'' ) ) ) || ltrim ( rtrim ( to_char ( exp_pass_through.Amount ) ) ) || ltrim ( rtrim ( to_char ( exp_pass_through.PaidAmount ) ) ) || ltrim ( rtrim ( to_char ( exp_pass_through.CreateTime , ''yyyy-mm-dd'' ) ) ) || TO_CHAR ( exp_pass_through.DOC_ID ) || LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE.TGT_IDNTFTN_VAL ) as var_calc_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd,
LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE.source_record_id
FROM
LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE
INNER JOIN exp_pass_through ON LKP_TERADATA_ETL_REF_XLAT_LN_AMT_SBTYPE.source_record_id = exp_pass_through.source_record_id
INNER JOIN LKP_AR_INVOICE_LINE ON exp_pass_through.source_record_id = LKP_AR_INVOICE_LINE.source_record_id
INNER JOIN LKP_AR_INVC_LN_AMT ON LKP_AR_INVOICE_LINE.source_record_id = LKP_AR_INVC_LN_AMT.source_record_id
);


-- Component rtr_ins_upd_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_Insert AS
(SELECT
exp_check_flag.lkp_AR_INVC_ID as lkp_AR_INVC_ID,
exp_check_flag.lkp_AR_INVC_LN_NUM as lkp_AR_INVC_LN_NUM,
exp_check_flag.lkp_AR_INVC_LN_AMT_TYPE_CD as lkp_AR_INVC_LN_AMT_TYPE_CD,
exp_check_flag.lkp_AR_INVC_LN_AMT_DTTM as lkp_AR_INVC_LN_AMT_DTTM,
exp_check_flag.lkp_AR_INVC_LN_AMT_TRANS_AMT as lkp_AR_INVC_LN_AMT_TRANS_AMT,
exp_check_flag.lkp_AR_INVC_LN_AMT_PD_AMT as lkp_AR_INVC_LN_AMT_PD_AMT,
exp_check_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_check_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_check_flag.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_check_flag.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_check_flag.DOC_ID as DOC_ID,
exp_check_flag.ID as ID,
exp_check_flag.TYPECODE as TYPECODE,
exp_check_flag.CreateTime as CreateTime,
exp_check_flag.Amount as Amount,
exp_check_flag.PaidAmount as PaidAmount,
exp_check_flag.prcsid as prcsid,
exp_check_flag.out_ins_upd as out_ins_upd,
exp_check_flag.o_DefaultDate as o_DefaultDate,
exp_check_flag.Retired as Retired,
exp_check_flag.UpdateTime as UpdateTime,
exp_check_flag.TRANS_END_DTTM_upd as TRANS_END_DTTM_upd,
exp_check_flag.TRANS_END_DTTM_ins as TRANS_END_DTTM_ins,
exp_check_flag.AR_INVC_LN_AMT_SBTYPE_CD2 as AR_INVC_LN_AMT_SBTYPE_CD,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE exp_check_flag.out_ins_upd = ''I'' OR exp_check_flag.out_ins_upd = ''U'' OR ( exp_check_flag.lkp_EDW_END_DTTM != to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_check_flag.Retired = 0 ) 
-- exp_check_flag.DOC_ID IS NOT NULL AND exp_check_flag.ID IS NOT NULL AND exp_check_flag.out_ins_upd = ''I'' OR ( exp_check_flag.lkp_EDW_END_DTTM != to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_check_flag.Retired = 0 )
);


-- Component rtr_ins_upd_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_Retired AS
(SELECT
exp_check_flag.lkp_AR_INVC_ID as lkp_AR_INVC_ID,
exp_check_flag.lkp_AR_INVC_LN_NUM as lkp_AR_INVC_LN_NUM,
exp_check_flag.lkp_AR_INVC_LN_AMT_TYPE_CD as lkp_AR_INVC_LN_AMT_TYPE_CD,
exp_check_flag.lkp_AR_INVC_LN_AMT_DTTM as lkp_AR_INVC_LN_AMT_DTTM,
exp_check_flag.lkp_AR_INVC_LN_AMT_TRANS_AMT as lkp_AR_INVC_LN_AMT_TRANS_AMT,
exp_check_flag.lkp_AR_INVC_LN_AMT_PD_AMT as lkp_AR_INVC_LN_AMT_PD_AMT,
exp_check_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_check_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_check_flag.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_check_flag.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_check_flag.DOC_ID as DOC_ID,
exp_check_flag.ID as ID,
exp_check_flag.TYPECODE as TYPECODE,
exp_check_flag.CreateTime as CreateTime,
exp_check_flag.Amount as Amount,
exp_check_flag.PaidAmount as PaidAmount,
exp_check_flag.prcsid as prcsid,
exp_check_flag.out_ins_upd as out_ins_upd,
exp_check_flag.o_DefaultDate as o_DefaultDate,
exp_check_flag.Retired as Retired,
exp_check_flag.UpdateTime as UpdateTime,
exp_check_flag.TRANS_END_DTTM_upd as TRANS_END_DTTM_upd,
exp_check_flag.TRANS_END_DTTM_ins as TRANS_END_DTTM_ins,
exp_check_flag.AR_INVC_LN_AMT_SBTYPE_CD2 as AR_INVC_LN_AMT_SBTYPE_CD,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE exp_check_flag.out_ins_upd = ''R'' and exp_check_flag.Retired != 0 and exp_check_flag.lkp_EDW_END_DTTM = to_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_ar_invc_ln_amt_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ar_invc_ln_amt_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_Insert.DOC_ID as DOC_ID1,
rtr_ins_upd_Insert.ID as ID1,
rtr_ins_upd_Insert.TYPECODE as TYPECODE1,
rtr_ins_upd_Insert.CreateTime as CreateTime1,
rtr_ins_upd_Insert.Amount as Amount1,
rtr_ins_upd_Insert.PaidAmount as PaidAmount1,
rtr_ins_upd_Insert.prcsid as prcsid1,
rtr_ins_upd_Insert.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
rtr_ins_upd_Insert.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_ins_upd_Insert.o_DefaultDate as o_DefaultDate1,
rtr_ins_upd_Insert.Retired as Retired1,
rtr_ins_upd_Insert.UpdateTime as UpdateTime1,
rtr_ins_upd_Insert.TRANS_END_DTTM_ins as TRANS_END_DTTM_ins1,
rtr_ins_upd_Insert.AR_INVC_LN_AMT_SBTYPE_CD as AR_INVC_LN_AMT_SBTYPE_CD,
0 as UPDATE_STRATEGY_ACTION,rtr_ins_upd_Insert.source_record_id
FROM
rtr_ins_upd_Insert
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_ar_invc_ln_amt_ins.DOC_ID1 as DOC_ID1,
upd_ar_invc_ln_amt_ins.ID1 as ID1,
upd_ar_invc_ln_amt_ins.TYPECODE1 as TYPECODE1,
upd_ar_invc_ln_amt_ins.CreateTime1 as CreateTime1,
upd_ar_invc_ln_amt_ins.Amount1 as Amount1,
upd_ar_invc_ln_amt_ins.PaidAmount1 as PaidAmount1,
upd_ar_invc_ln_amt_ins.prcsid1 as prcsid1,
upd_ar_invc_ln_amt_ins.out_EDW_STRT_DTTM1 as out_EDW_STRT_DTTM1,
CASE WHEN upd_ar_invc_ln_amt_ins.Retired1 <> 0 THEN CURRENT_TIMESTAMP ELSE upd_ar_invc_ln_amt_ins.out_EDW_END_DTTM1 END as out_EDW_END_DTTM11,
upd_ar_invc_ln_amt_ins.UpdateTime1 as UpdateTime1,
CASE WHEN upd_ar_invc_ln_amt_ins.Retired1 <> 0 THEN upd_ar_invc_ln_amt_ins.UpdateTime1 ELSE upd_ar_invc_ln_amt_ins.TRANS_END_DTTM_ins1 END as out_TRANS_END_DTTM_ins1,
upd_ar_invc_ln_amt_ins.AR_INVC_LN_AMT_SBTYPE_CD as AR_INVC_LN_AMT_SBTYPE_CD,
upd_ar_invc_ln_amt_ins.source_record_id
FROM
upd_ar_invc_ln_amt_ins
);


-- Component tgt_AR_INVC_LN_AMT_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AR_INVC_LN_AMT
(
AR_INVC_ID,
AR_INVC_LN_NUM,
AR_INVC_LN_AMT_TYPE_CD,
AR_INVC_LN_AMT_DTTM,
AR_INVC_LN_AMT_TRANS_AMT,
AR_INVC_LN_AMT_PD_AMT,
AR_INVC_LN_AMT_SBTYPE_CD,
PRCS_ID,
AR_INVC_LN_AMT_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.DOC_ID1 as AR_INVC_ID,
exp_pass_to_target_ins.ID1 as AR_INVC_LN_NUM,
exp_pass_to_target_ins.TYPECODE1 as AR_INVC_LN_AMT_TYPE_CD,
exp_pass_to_target_ins.CreateTime1 as AR_INVC_LN_AMT_DTTM,
exp_pass_to_target_ins.Amount1 as AR_INVC_LN_AMT_TRANS_AMT,
exp_pass_to_target_ins.PaidAmount1 as AR_INVC_LN_AMT_PD_AMT,
exp_pass_to_target_ins.AR_INVC_LN_AMT_SBTYPE_CD as AR_INVC_LN_AMT_SBTYPE_CD,
exp_pass_to_target_ins.prcsid1 as PRCS_ID,
exp_pass_to_target_ins.CreateTime1 as AR_INVC_LN_AMT_STRT_DTTM,
exp_pass_to_target_ins.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.out_EDW_END_DTTM11 as EDW_END_DTTM,
exp_pass_to_target_ins.UpdateTime1 as TRANS_STRT_DTTM,
exp_pass_to_target_ins.out_TRANS_END_DTTM_ins1 as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


-- Component tgt_AR_INVC_LN_AMT_ins, Type Post SQL 
UPDATE  DB_T_PROD_CORE.AR_INVC_LN_AMT   FROM

(SELECT                    AR_INVC_LN_NUM,AR_INVC_LN_AMT_TYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM,

max(TRANS_STRT_DTTM) over (partition by AR_INVC_LN_NUM,AR_INVC_LN_AMT_TYPE_CD ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1,

max(EDW_STRT_DTTM) over (partition by AR_INVC_LN_NUM,AR_INVC_LN_AMT_TYPE_CD ORDER BY TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2

FROM      DB_T_PROD_CORE.AR_INVC_LN_AMT

group by AR_INVC_LN_NUM,AR_INVC_LN_AMT_TYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM

) a

set 

EDW_END_DTTM=A.lead2

,TRANS_END_DTTM = a.lead1

where  AR_INVC_LN_AMT.TRANS_STRT_DTTM = A.TRANS_STRT_DTTM

AND AR_INVC_LN_AMT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND AR_INVC_LN_AMT.AR_INVC_LN_AMT_TYPE_CD=A.AR_INVC_LN_AMT_TYPE_CD

and AR_INVC_LN_AMT.AR_INVC_LN_NUM = A.AR_INVC_LN_NUM

--and AR_INVC_LN_AMT.TRANS_STRT_DTTM <>AR_INVC_LN_AMT.TRANS_END_DTTM

and CAST(AR_INVC_LN_AMT.EDW_END_DTTM AS DATE)=''9999-12-31''

and CAST(AR_INVC_LN_AMT.TRANS_END_DTTM AS DATE)=''9999-12-31'' 

and lead1 is not null

and lead2 is not null;







--UPDATE  AR_INVC_LN_AMT   FROM

--(SELECT	distinct AR_INVC_LN_AMT_TYPE_CD,AR_INVC_ID,AR_INVC_LN_AMT_TRANS_AMT,AR_INVC_LN_NUM,EDW_STRT_DTTM,

--max(EDW_STRT_DTTM) over (partition by AR_INVC_LN_AMT_TYPE_CD,AR_INVC_ID,AR_INVC_LN_AMT_TRANS_AMT,AR_INVC_LN_NUM ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

-- as lead1

--FROM	AR_INVC_LN_AMT

-- ) a

--set 

--EDW_END_DTTM=A.lead1

--where  AR_INVC_LN_AMT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

--and AR_INVC_LN_AMT.AR_INVC_ID=A.AR_INVC_ID 

--AND AR_INVC_LN_AMT.AR_INVC_LN_AMT_TRANS_AMT=A.AR_INVC_LN_AMT_TRANS_AMT

--AND AR_INVC_LN_AMT.AR_INVC_LN_AMT_TYPE_CD=A.AR_INVC_LN_AMT_TYPE_CD

--and AR_INVC_LN_AMT.AR_INVC_LN_NUM = A.AR_INVC_LN_NUM

--and lead1 is not null;


-- Component upd_retired1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retired1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_Retired.lkp_AR_INVC_ID as DOC_ID3,
rtr_ins_upd_Retired.lkp_AR_INVC_LN_NUM as ID3,
rtr_ins_upd_Retired.lkp_AR_INVC_LN_AMT_TYPE_CD as TYPECODE3,
rtr_ins_upd_Retired.prcsid as prcsid3,
rtr_ins_upd_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_Retired.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM3,
rtr_ins_upd_Retired.Retired as Retired1,
rtr_ins_upd_Retired.UpdateTime as UpdateTime4,
rtr_ins_upd_Retired.TRANS_END_DTTM_upd as TRANS_END_DTTM_upd4,
1 as UPDATE_STRATEGY_ACTION,rtr_ins_upd_Retired.source_record_id
FROM
rtr_ins_upd_Retired
);


-- Component exp_pass_to_target_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_retired AS
(
SELECT
upd_retired1.ID3 as ID3,
upd_retired1.TYPECODE3 as TYPECODE3,
upd_retired1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM3,
upd_retired1.TRANS_END_DTTM_upd4 as TRANS_END_DTTM_upd4,
upd_retired1.source_record_id
FROM
upd_retired1
);


-- Component tgt_AR_INVC_LN_AMT_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AR_INVC_LN_AMT
USING exp_pass_to_target_upd_retired ON (AR_INVC_LN_AMT.AR_INVC_LN_NUM = exp_pass_to_target_upd_retired.ID3 AND AR_INVC_LN_AMT.AR_INVC_LN_AMT_TYPE_CD = exp_pass_to_target_upd_retired.TYPECODE3 AND AR_INVC_LN_AMT.EDW_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
AR_INVC_LN_NUM = exp_pass_to_target_upd_retired.ID3,
AR_INVC_LN_AMT_TYPE_CD = exp_pass_to_target_upd_retired.TYPECODE3,
EDW_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_retired.out_EDW_STRT_DTTM3,
TRANS_END_DTTM = exp_pass_to_target_upd_retired.TRANS_END_DTTM_upd4;


END; ';