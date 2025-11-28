-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_INVOICE_BILLINGCENTER_INSUPD("RUN_ID" VARCHAR)
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

-- Component src_sq_bc_invoice, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_bc_invoice AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as InvoiceNumber,
$2 as PaymentDueDate,
$3 as Event_date,
$4 as CreateTime,
$5 as typecode,
$6 as Retired,
$7 as updatetime,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/* EIM-23072 m_base_finance_invoice_billingcenter_Insupd */


SELECT InvoiceNumber_stg , PaymentDueDate_stg ,EventDate_stg, CreateTime_stg,''INVOICE'' as typecode_stg,Retired_stg,updatetime_stg FROM DB_T_PROD_STAG.bc_invoice

where  

bc_invoice.UpdateTime_Stg > (:start_dttm)

	and bc_invoice.UpdateTime_stg <= (:end_dttm)





/*UNION

 SELECT RefNumber_stg , ReceivedDate_stg , ''CHECK'' as typecode_stg FROM DB_T_PROD_STAG.bc_basemoneyreceived

  WHERE  RefNumber_stg  IS NOT NULL AND  InvoiceNumber_stg IS NOT NULL*/
) SRC
)
);


-- Component exp_src_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_pass AS
(
SELECT
src_sq_bc_invoice.InvoiceNumber as InvoiceNumber,
src_sq_bc_invoice.typecode as Typecode,
''BILL'' as doc_category,
src_sq_bc_invoice.PaymentDueDate as PaymentDueDate,
src_sq_bc_invoice.Event_date as Event_date,
src_sq_bc_invoice.CreateTime as CreateTime,
src_sq_bc_invoice.Retired as Retired,
src_sq_bc_invoice.updatetime as updatetime,
src_sq_bc_invoice.source_record_id
FROM
src_sq_bc_invoice
);


-- Component LKP_DOC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DOC AS
(
SELECT
LKP.DOC_ID,
exp_src_pass.InvoiceNumber as InvoiceNumber,
exp_src_pass.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src_pass.source_record_id ORDER BY LKP.DOC_ID asc,LKP.TM_PRD_CD asc,LKP.DOC_CRTN_DTTM asc,LKP.DOC_RECPT_DT asc,LKP.DOC_PRD_STRT_DTTM asc,LKP.DOC_PRD_END_DTTM asc,LKP.DOC_ISSUR_NUM asc,LKP.DATA_SRC_TYPE_CD asc,LKP.DOC_DESC_TXT asc,LKP.DOC_NAME asc,LKP.DOC_HOST_NUM asc,LKP.DOC_HOST_VERS_NUM asc,LKP.DOC_CYCL_CD asc,LKP.DOC_TYPE_CD asc,LKP.MM_OBJT_ID asc,LKP.DOC_CTGY_TYPE_CD asc,LKP.LANG_TYPE_CD asc,LKP.PRCS_ID asc,LKP.DOC_STS_CD asc) RNK
FROM
exp_src_pass
LEFT JOIN (
SELECT DOC.DOC_ID as DOC_ID, DOC.TM_PRD_CD as TM_PRD_CD, DOC.DOC_CRTN_DTTM as DOC_CRTN_DTTM, DOC.DOC_RECPT_DT as DOC_RECPT_DT, DOC.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM, DOC.DOC_PRD_END_DTTM as DOC_PRD_END_DTTM, DOC.DATA_SRC_TYPE_CD as DATA_SRC_TYPE_CD, DOC.DOC_DESC_TXT as DOC_DESC_TXT, DOC.DOC_NAME as DOC_NAME, DOC.DOC_HOST_NUM as DOC_HOST_NUM, DOC.DOC_HOST_VERS_NUM as DOC_HOST_VERS_NUM, DOC.DOC_CYCL_CD as DOC_CYCL_CD, DOC.MM_OBJT_ID as MM_OBJT_ID, DOC.LANG_TYPE_CD as LANG_TYPE_CD, DOC.PRCS_ID as PRCS_ID, DOC.DOC_STS_CD as DOC_STS_CD, DOC.DOC_ISSUR_NUM as DOC_ISSUR_NUM, DOC.DOC_TYPE_CD as DOC_TYPE_CD, DOC.DOC_CTGY_TYPE_CD as DOC_CTGY_TYPE_CD 
FROM DB_T_PROD_CORE.DOC 
QUALIFY ROW_NUMBER() OVER(PARTITION BY DOC.DOC_ISSUR_NUM,DOC.DOC_TYPE_CD,DOC.DOC_CTGY_TYPE_CD  ORDER BY DOC.EDW_END_DTTM desc) = 1
) LKP ON LKP.DOC_ISSUR_NUM = exp_src_pass.InvoiceNumber AND LKP.DOC_TYPE_CD = exp_src_pass.Typecode AND LKP.DOC_CTGY_TYPE_CD = exp_src_pass.doc_category
QUALIFY RNK = 1
);


-- Component LKP_INVC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INVC AS
(
SELECT
LKP.INVC_ID,
LKP.INVC_DTTM,
LKP.HOST_INVC_NUM,
LKP.INVC_SBTYPE_CD,
LKP.INVC_CURY_CD,
LKP.INVC_DUE_DTTM,
LKP.INVC_STRT_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
LKP.TRANS_END_DTTM,
LKP_DOC.DOC_ID as DOC_ID,
LKP_DOC.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_DOC.source_record_id ORDER BY LKP.INVC_ID asc,LKP.INVC_DTTM asc,LKP.HOST_INVC_NUM asc,LKP.INVC_SBTYPE_CD asc,LKP.INVC_CURY_CD asc,LKP.INVC_DUE_DTTM asc,LKP.INVC_STRT_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.TRANS_END_DTTM asc) RNK
FROM
LKP_DOC
LEFT JOIN (
SELECT	INVC.INVC_ID as INVC_ID, INVC.INVC_DTTM as INVC_DTTM, INVC.HOST_INVC_NUM as HOST_INVC_NUM,INVC.INVC_SBTYPE_CD as INVC_SBTYPE_CD,INVC.INVC_CURY_CD as INVC_CURY_CD,INVC.INVC_DUE_DTTM as INVC_DUE_DTTM, INVC.INVC_STRT_DTTM as INVC_STRT_DTTM,INVC.EDW_STRT_DTTM as EDW_STRT_DTTM,INVC.EDW_END_DTTM as EDW_END_DTTM, INVC.TRANS_STRT_DTTM as TRANS_STRT_DTTM,INVC.TRANS_END_DTTM as TRANS_END_DTTM 
FROM	DB_T_PROD_CORE.INVC QUALIFY	ROW_NUMBER () OVER (PARTITION BY INVC.INVC_ID order by INVC.EDW_END_DTTM desc)=1
) LKP ON LKP.INVC_ID = LKP_DOC.DOC_ID
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_INVC.INVC_ID as INVC_ID,
LKP_DOC.DOC_ID as DOC_ID,
''UNK'' as o_sub_type_cd,
''UNK'' as o_cury_cd,
:PRCS_ID as o_process_id,
exp_src_pass.InvoiceNumber as InvoiceNumber,
exp_src_pass.PaymentDueDate as PaymentDueDate,
exp_src_pass.Event_date as Event_date,
exp_src_pass.CreateTime as CreateTime,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_src_pass.Retired as Retired,
exp_src_pass.updatetime as TRANS_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
exp_src_pass.source_record_id
FROM
exp_src_pass
INNER JOIN LKP_DOC ON exp_src_pass.source_record_id = LKP_DOC.source_record_id
INNER JOIN LKP_INVC ON LKP_DOC.source_record_id = LKP_INVC.source_record_id
);


-- Component exp_comp_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_comp_data AS
(
SELECT
LKP_INVC.INVC_ID as LKP_INVC_ID,
LKP_INVC.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
LKP_INVC.TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
LKP_INVC.TRANS_END_DTTM as LKP_TRANS_END_DTTM,
MD5 ( LTRIM ( RTRIM ( LKP_INVC.INVC_DTTM ) ) || LTRIM ( RTRIM ( LKP_INVC.HOST_INVC_NUM ) ) || LTRIM ( RTRIM ( LKP_INVC.INVC_SBTYPE_CD ) ) || LTRIM ( RTRIM ( LKP_INVC.INVC_CURY_CD ) ) || LTRIM ( RTRIM ( LKP_INVC.INVC_DUE_DTTM ) ) || LTRIM ( RTRIM ( LKP_INVC.INVC_STRT_DTTM ) ) ) as v_lkp_checksum,
exp_data_transformation.DOC_ID as in_DOC_ID,
exp_data_transformation.o_sub_type_cd as in_INVC_SBTYPE_CD,
exp_data_transformation.o_cury_cd as in_INVC_CURY_CD,
exp_data_transformation.o_process_id as in_PRCS_ID,
exp_data_transformation.InvoiceNumber as in_HOST_INVC_NUM,
exp_data_transformation.PaymentDueDate as in_INVC_DUE_DT,
exp_data_transformation.Event_date as in_INVC_DTTM,
exp_data_transformation.CreateTime as in_INVS_STRT_DT,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
MD5 ( LTRIM ( RTRIM ( exp_data_transformation.Event_date ) ) || LTRIM ( RTRIM ( exp_data_transformation.InvoiceNumber ) ) || LTRIM ( RTRIM ( exp_data_transformation.o_sub_type_cd ) ) || LTRIM ( RTRIM ( exp_data_transformation.o_cury_cd ) ) || LTRIM ( RTRIM ( exp_data_transformation.PaymentDueDate ) ) || LTRIM ( RTRIM ( exp_data_transformation.CreateTime ) ) ) as v_in_checksum,
exp_data_transformation.Retired as Retired,
LKP_INVC.EDW_END_DTTM as lkp_EDW_END_DTTM,
CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_checksum != v_in_checksum THEN ''U'' ELSE ''R'' END END as Calc_ins_upd,
exp_data_transformation.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_data_transformation.TRANS_END_DTTM as TRANS_END_DTTM,
LKP_INVC.source_record_id
FROM
LKP_INVC
INNER JOIN exp_data_transformation ON LKP_INVC.source_record_id = exp_data_transformation.source_record_id
);


-- Component RTR_Insert_Update_Grp_Insert, Type ROUTER Output Group Grp_Insert
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update_Grp_Insert AS
(SELECT
exp_comp_data.LKP_INVC_ID as LKP_INVC_ID,
exp_comp_data.in_DOC_ID as in_DOC_ID,
exp_comp_data.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
exp_comp_data.in_INVC_CURY_CD as in_INVC_CURY_CD,
NULL as IsNewRecord,
NULL as IsModified,
exp_comp_data.in_PRCS_ID as in_PRCS_ID,
exp_comp_data.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
exp_comp_data.in_INVC_DUE_DT as in_INVC_DUE_DT,
exp_comp_data.in_INVC_DTTM as in_INVC_DTTM,
exp_comp_data.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_comp_data.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
exp_comp_data.Calc_ins_upd as Calc_ins_upd,
exp_comp_data.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_comp_data.EDW_END_DTTM as EDW_END_DTTM,
exp_comp_data.in_INVS_STRT_DT as o_DefaultDate,
exp_comp_data.Retired as Retired,
exp_comp_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_comp_data.LKP_TRANS_END_DTTM as lkp_TRANS_END_DTTM,
exp_comp_data.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_comp_data.TRANS_END_DTTM as TRANS_END_DTTM,
exp_comp_data.source_record_id
FROM
exp_comp_data
WHERE exp_comp_data.in_DOC_ID IS NOT NULL and exp_comp_data.Calc_ins_upd = ''I'' OR ( exp_comp_data.Retired = 0 and exp_comp_data.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component RTR_Insert_Update_Grp_Retired, Type ROUTER Output Group Grp_Retired
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update_Grp_Retired AS
(
SELECT
exp_comp_data.LKP_INVC_ID as LKP_INVC_ID,
exp_comp_data.in_DOC_ID as in_DOC_ID,
exp_comp_data.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
exp_comp_data.in_INVC_CURY_CD as in_INVC_CURY_CD,
NULL as IsNewRecord,
NULL as IsModified,
exp_comp_data.in_PRCS_ID as in_PRCS_ID,
exp_comp_data.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
exp_comp_data.in_INVC_DUE_DT as in_INVC_DUE_DT,
exp_comp_data.in_INVC_DTTM as in_INVC_DTTM,
exp_comp_data.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_comp_data.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
exp_comp_data.Calc_ins_upd as Calc_ins_upd,
exp_comp_data.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_comp_data.EDW_END_DTTM as EDW_END_DTTM,
exp_comp_data.in_INVS_STRT_DT as o_DefaultDate,
exp_comp_data.Retired as Retired,
exp_comp_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_comp_data.LKP_TRANS_END_DTTM as lkp_TRANS_END_DTTM,
exp_comp_data.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_comp_data.TRANS_END_DTTM as TRANS_END_DTTM,
exp_comp_data.source_record_id
FROM
exp_comp_data
WHERE exp_comp_data.Calc_ins_upd = ''R'' and exp_comp_data.Retired != 0 and exp_comp_data.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component RTR_Insert_Update_Grp_update, Type ROUTER Output Group Grp_update
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update_Grp_update AS
(
SELECT
exp_comp_data.LKP_INVC_ID as LKP_INVC_ID,
exp_comp_data.in_DOC_ID as in_DOC_ID,
exp_comp_data.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
exp_comp_data.in_INVC_CURY_CD as in_INVC_CURY_CD,
NULL as IsNewRecord,
NULL as IsModified,
exp_comp_data.in_PRCS_ID as in_PRCS_ID,
exp_comp_data.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
exp_comp_data.in_INVC_DUE_DT as in_INVC_DUE_DT,
exp_comp_data.in_INVC_DTTM as in_INVC_DTTM,
exp_comp_data.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_comp_data.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM,
exp_comp_data.Calc_ins_upd as Calc_ins_upd,
exp_comp_data.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_comp_data.EDW_END_DTTM as EDW_END_DTTM,
exp_comp_data.in_INVS_STRT_DT as o_DefaultDate,
exp_comp_data.Retired as Retired,
exp_comp_data.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_comp_data.LKP_TRANS_END_DTTM as lkp_TRANS_END_DTTM,
exp_comp_data.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_comp_data.TRANS_END_DTTM as TRANS_END_DTTM,
exp_comp_data.source_record_id
FROM
exp_comp_data
WHERE exp_comp_data.Calc_ins_upd = ''U'' AND exp_comp_data.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_invoice_upd_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_invoice_upd_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_Grp_update.in_DOC_ID as in_DOC_ID,
RTR_Insert_Update_Grp_update.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
RTR_Insert_Update_Grp_update.in_INVC_CURY_CD as in_INVC_CURY_CD,
RTR_Insert_Update_Grp_update.in_PRCS_ID as in_PRCS_ID,
RTR_Insert_Update_Grp_update.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
RTR_Insert_Update_Grp_update.in_INVC_DUE_DT as in_INVC_DUE_DT,
RTR_Insert_Update_Grp_update.in_INVC_DTTM as in_INVC_DTTM,
RTR_Insert_Update_Grp_update.EDW_STRT_DTTM as EDW_STRT_DTTM2,
RTR_Insert_Update_Grp_update.EDW_END_DTTM as EDW_END_DTTM2,
RTR_Insert_Update_Grp_update.o_DefaultDate as o_DefaultDate3,
RTR_Insert_Update_Grp_update.Retired as Retired3,
RTR_Insert_Update_Grp_update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
RTR_Insert_Update_Grp_update.TRANS_END_DTTM as TRANS_END_DTTM3,
0 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update_Grp_update.source_record_id
FROM
RTR_Insert_Update_Grp_update
);


-- Component upd_invoice_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_invoice_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_Grp_update.LKP_INVC_ID as LKP_INVC_ID1,
RTR_Insert_Update_Grp_update.EDW_STRT_DTTM as EDW_STRT_DTTM1,
RTR_Insert_Update_Grp_update.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
RTR_Insert_Update_Grp_update.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
RTR_Insert_Update_Grp_update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
RTR_Insert_Update_Grp_update.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM3,
RTR_Insert_Update_Grp_update.lkp_TRANS_END_DTTM as lkp_TRANS_END_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update_Grp_update.source_record_id
FROM
RTR_Insert_Update_Grp_update
);


-- Component upd_invoice_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_invoice_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_Grp_Retired.LKP_INVC_ID as LKP_INVC_ID1,
RTR_Insert_Update_Grp_Retired.EDW_STRT_DTTM as EDW_STRT_DTTM1,
RTR_Insert_Update_Grp_Retired.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
RTR_Insert_Update_Grp_Retired.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
RTR_Insert_Update_Grp_Retired.LKP_TRANS_STRT_DTTM as LKP_TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update_Grp_Retired.source_record_id
FROM
RTR_Insert_Update_Grp_Retired
);


-- Component fil_active_recs, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_active_recs AS
(
SELECT
upd_invoice_upd.LKP_INVC_ID1 as LKP_INVC_ID1,
upd_invoice_upd.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_invoice_upd.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
upd_invoice_upd.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
upd_invoice_upd.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_invoice_upd.LKP_TRANS_STRT_DTTM3 as LKP_TRANS_STRT_DTTM3,
upd_invoice_upd.lkp_TRANS_END_DTTM3 as lkp_TRANS_END_DTTM3,
upd_invoice_upd.source_record_id
FROM
upd_invoice_upd
WHERE upd_invoice_upd.lkp_EDW_END_DTTM3 = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_invoice_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_invoice_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_Grp_Insert.in_DOC_ID as in_DOC_ID,
RTR_Insert_Update_Grp_Insert.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
RTR_Insert_Update_Grp_Insert.in_INVC_CURY_CD as in_INVC_CURY_CD,
RTR_Insert_Update_Grp_Insert.in_PRCS_ID as in_PRCS_ID,
RTR_Insert_Update_Grp_Insert.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
RTR_Insert_Update_Grp_Insert.in_INVC_DUE_DT as in_INVC_DUE_DT,
RTR_Insert_Update_Grp_Insert.in_INVC_DTTM as in_INVC_DTTM,
RTR_Insert_Update_Grp_Insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
RTR_Insert_Update_Grp_Insert.EDW_END_DTTM as EDW_END_DTTM1,
RTR_Insert_Update_Grp_Insert.o_DefaultDate as o_DefaultDate1,
RTR_Insert_Update_Grp_Insert.Retired as Retired1,
RTR_Insert_Update_Grp_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
RTR_Insert_Update_Grp_Insert.TRANS_END_DTTM as TRANS_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update_Grp_Insert.source_record_id
FROM
RTR_Insert_Update_Grp_Insert
);


-- Component fil_retired_recs, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_retired_recs AS
(
SELECT
upd_invoice_upd_insert.in_DOC_ID as in_DOC_ID,
upd_invoice_upd_insert.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
upd_invoice_upd_insert.in_INVC_CURY_CD as in_INVC_CURY_CD,
upd_invoice_upd_insert.in_PRCS_ID as in_PRCS_ID,
upd_invoice_upd_insert.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
upd_invoice_upd_insert.in_INVC_DUE_DT as in_INVC_DUE_DT,
upd_invoice_upd_insert.in_INVC_DTTM as in_INVC_DTTM,
upd_invoice_upd_insert.EDW_STRT_DTTM2 as EDW_STRT_DTTM2,
upd_invoice_upd_insert.EDW_END_DTTM2 as EDW_END_DTTM2,
upd_invoice_upd_insert.o_DefaultDate3 as o_DefaultDate3,
upd_invoice_upd_insert.Retired3 as Retired3,
upd_invoice_upd_insert.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_invoice_upd_insert.TRANS_END_DTTM3 as TRANS_END_DTTM3,
upd_invoice_upd_insert.source_record_id
FROM
upd_invoice_upd_insert
WHERE upd_invoice_upd_insert.Retired3 = 0
);


-- Component exp_ins_pass_to_target_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target_upd AS
(
SELECT
fil_retired_recs.in_DOC_ID as in_DOC_ID,
fil_retired_recs.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
fil_retired_recs.in_INVC_CURY_CD as in_INVC_CURY_CD,
fil_retired_recs.in_PRCS_ID as in_PRCS_ID,
fil_retired_recs.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
fil_retired_recs.in_INVC_DUE_DT as in_INVC_DUE_DT,
fil_retired_recs.in_INVC_DTTM as in_INVC_DTTM,
fil_retired_recs.EDW_STRT_DTTM2 as EDW_STRT_DTTM2,
fil_retired_recs.EDW_END_DTTM2 as EDW_END_DTTM2,
fil_retired_recs.o_DefaultDate3 as o_DefaultDate3,
fil_retired_recs.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
fil_retired_recs.TRANS_END_DTTM3 as TRANS_END_DTTM3,
fil_retired_recs.source_record_id
FROM
fil_retired_recs
);


-- Component exp_invoice_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_invoice_upd AS
(
SELECT
fil_active_recs.LKP_INVC_ID1 as LKP_INVC_ID1,
DATEADD (SECOND, -1, fil_active_recs.EDW_STRT_DTTM1) as EDW_END_DTTM11,
fil_active_recs.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
DATEADD (SECOND, -1, fil_active_recs.TRANS_STRT_DTTM3) as TRANS_END_DTTM11,
fil_active_recs.LKP_TRANS_STRT_DTTM3 as LKP_TRANS_STRT_DTTM3,
fil_active_recs.source_record_id
FROM
fil_active_recs
);


-- Component exp_invoice_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_invoice_upd_retired AS
(
SELECT
upd_invoice_upd_retired.LKP_INVC_ID1 as LKP_INVC_ID1,
CURRENT_TIMESTAMP as EDW_END_DTTM11,
upd_invoice_upd_retired.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as TRANS_END_DTTM,
upd_invoice_upd_retired.LKP_TRANS_STRT_DTTM4 as LKP_TRANS_STRT_DTTM4,
upd_invoice_upd_retired.source_record_id
FROM
upd_invoice_upd_retired
);


-- Component exp_ins_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target AS
(
SELECT
upd_invoice_ins.in_DOC_ID as in_DOC_ID,
upd_invoice_ins.in_INVC_SBTYPE_CD as in_INVC_SBTYPE_CD,
upd_invoice_ins.in_INVC_CURY_CD as in_INVC_CURY_CD,
upd_invoice_ins.in_PRCS_ID as in_PRCS_ID,
upd_invoice_ins.in_HOST_INVC_NUM as in_HOST_INVC_NUM,
upd_invoice_ins.in_INVC_DUE_DT as in_INVC_DUE_DT,
upd_invoice_ins.in_INVC_DTTM as in_INVC_DTTM,
upd_invoice_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN upd_invoice_ins.Retired1 = 0 THEN upd_invoice_ins.EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as EDW_END_DTTM11,
upd_invoice_ins.o_DefaultDate1 as o_DefaultDate1,
upd_invoice_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
CASE WHEN upd_invoice_ins.Retired1 = 0 THEN upd_invoice_ins.TRANS_END_DTTM1 ELSE CURRENT_TIMESTAMP END as TRANS_END_DTTM11,
upd_invoice_ins.source_record_id
FROM
upd_invoice_ins
);


-- Component tgt_invc_up_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INVC
(
INVC_ID,
INVC_DTTM,
HOST_INVC_NUM,
INVC_SBTYPE_CD,
INVC_CURY_CD,
INVC_DUE_DTTM,
PRCS_ID,
INVC_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_ins_pass_to_target_upd.in_DOC_ID as INVC_ID,
exp_ins_pass_to_target_upd.in_INVC_DTTM as INVC_DTTM,
exp_ins_pass_to_target_upd.in_HOST_INVC_NUM as HOST_INVC_NUM,
exp_ins_pass_to_target_upd.in_INVC_SBTYPE_CD as INVC_SBTYPE_CD,
exp_ins_pass_to_target_upd.in_INVC_CURY_CD as INVC_CURY_CD,
exp_ins_pass_to_target_upd.in_INVC_DUE_DT as INVC_DUE_DTTM,
exp_ins_pass_to_target_upd.in_PRCS_ID as PRCS_ID,
exp_ins_pass_to_target_upd.o_DefaultDate3 as INVC_STRT_DTTM,
exp_ins_pass_to_target_upd.EDW_STRT_DTTM2 as EDW_STRT_DTTM,
exp_ins_pass_to_target_upd.EDW_END_DTTM2 as EDW_END_DTTM,
exp_ins_pass_to_target_upd.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM,
exp_ins_pass_to_target_upd.TRANS_END_DTTM3 as TRANS_END_DTTM
FROM
exp_ins_pass_to_target_upd;


-- Component tgt_invc_upd_update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INVC
USING exp_invoice_upd ON (INVC.INVC_ID = exp_invoice_upd.LKP_INVC_ID1 AND INVC.EDW_STRT_DTTM = exp_invoice_upd.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INVC_ID = exp_invoice_upd.LKP_INVC_ID1,
EDW_STRT_DTTM = exp_invoice_upd.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_invoice_upd.EDW_END_DTTM11,
TRANS_STRT_DTTM = exp_invoice_upd.LKP_TRANS_STRT_DTTM3,
TRANS_END_DTTM = exp_invoice_upd.TRANS_END_DTTM11;


-- Component tgt_invc_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.INVC
USING exp_invoice_upd_retired ON (INVC.INVC_ID = exp_invoice_upd_retired.LKP_INVC_ID1 AND INVC.EDW_STRT_DTTM = exp_invoice_upd_retired.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INVC_ID = exp_invoice_upd_retired.LKP_INVC_ID1,
EDW_STRT_DTTM = exp_invoice_upd_retired.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_invoice_upd_retired.EDW_END_DTTM11,
TRANS_STRT_DTTM = exp_invoice_upd_retired.LKP_TRANS_STRT_DTTM4,
TRANS_END_DTTM = exp_invoice_upd_retired.TRANS_END_DTTM;


-- Component tgt_invc_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.INVC
(
INVC_ID,
INVC_DTTM,
HOST_INVC_NUM,
INVC_SBTYPE_CD,
INVC_CURY_CD,
INVC_DUE_DTTM,
PRCS_ID,
INVC_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_ins_pass_to_target.in_DOC_ID as INVC_ID,
exp_ins_pass_to_target.in_INVC_DTTM as INVC_DTTM,
exp_ins_pass_to_target.in_HOST_INVC_NUM as HOST_INVC_NUM,
exp_ins_pass_to_target.in_INVC_SBTYPE_CD as INVC_SBTYPE_CD,
exp_ins_pass_to_target.in_INVC_CURY_CD as INVC_CURY_CD,
exp_ins_pass_to_target.in_INVC_DUE_DT as INVC_DUE_DTTM,
exp_ins_pass_to_target.in_PRCS_ID as PRCS_ID,
exp_ins_pass_to_target.o_DefaultDate1 as INVC_STRT_DTTM,
exp_ins_pass_to_target.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_pass_to_target.EDW_END_DTTM11 as EDW_END_DTTM,
exp_ins_pass_to_target.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_ins_pass_to_target.TRANS_END_DTTM11 as TRANS_END_DTTM
FROM
exp_ins_pass_to_target;


END; ';