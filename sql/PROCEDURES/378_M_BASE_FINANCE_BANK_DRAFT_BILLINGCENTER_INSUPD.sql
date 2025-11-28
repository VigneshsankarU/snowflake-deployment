-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_BANK_DRAFT_BILLINGCENTER_INSUPD("RUN_ID" VARCHAR)
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

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL,row_number() over (order by 1) AS source_record_id 

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

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL ,row_number() over (order by 1) AS source_record_id

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_TYPE''

    		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component src_bank_draft, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_bank_draft AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as RefNumber,
$2 as Amount,
$3 as PaidDate,
$4 as UpdateTime,
$5 as Typecode,
$6 as trans_acct_id,
$7 as rnk,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select id, Amount, createtime, updatetime, Fund_trnsfr_mthd_typ,trans_acct_id,Rank()  OVER(PARTITION BY  id  ORDER BY updatetime )  as rnk 

from (

SELECT 

cast(bc_outgoingpayment.ID_stg as varchar(50)) as ID, 

bc_outgoingpayment.Amount_stg as Amount, 

bc_outgoingpayment.CreateTime_stg as CreateTime, 

bc_outgoingpayment.UpdateTime_stg as UpdateTime,

bctl_paymentmethod.typecode_stg as fund_trnsfr_mthd_typ,

cast(bc_outgoingpayment.ID_stg as integer) as trans_acct_id

FROM DB_T_PROD_STAG.bc_outgoingpayment

left outer join DB_T_PROD_STAG.bc_paymentinstrument on bc_outgoingpayment.PaymentInstrumentID_stg = bc_paymentinstrument.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentmethod on bctl_paymentmethod.ID_stg = bc_paymentinstrument.PaymentMethod_stg

where bc_outgoingpayment.UpdateTime_stg > (:start_dttm) and bc_outgoingpayment.UpdateTime_stg <= (:end_dttm)

and bctl_paymentmethod.typecode_stg in (''ach'',''check'',''wire'')



UNION



SELECT 

cast(bc_basemoneyreceived.RefNumber_stg as varchar(50)) ID,

bc_basemoneyreceived.Amount_stg Amount,

bc_basemoneyreceived.CreateTime_stg CreateTime,

bc_basemoneyreceived.UpdateTime_stg UpdateTime,

bctl_paymentmethod.typecode_stg as fund_trnsfr_mthd_typ,

cast(bc_basemoneyreceived.ID_stg as integer) as trans_acct_id

FROM

DB_T_PROD_STAG.bc_basemoneyreceived

left outer join  DB_T_PROD_STAG.bc_paymentinstrument ON bc_basemoneyreceived.PaymentInstrumentID_stg = bc_paymentinstrument.ID_stg

left outer join DB_T_PROD_STAG.bctl_paymentmethod on bctl_paymentmethod.id_stg=bc_paymentinstrument.PaymentMethod_stg

where bc_basemoneyreceived.UpdateTime_stg > (:start_dttm) and bc_basemoneyreceived.UpdateTime_stg <= (:end_dttm)

and bctl_paymentmethod.typecode_stg in (''ach'',''check'',''wire'') and RefNumber_stg is not null

) fnc_bnkdrft_blngcntr
) SRC
)
);


-- Component exp_src_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_pass AS
(
SELECT
src_bank_draft.RefNumber as RefNumber,
src_bank_draft.Amount as Amount,
src_bank_draft.PaidDate as PaidDate,
src_bank_draft.Typecode as Typecode,
src_bank_draft.UpdateTime as UpdateTime,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */ as o_doc_type,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */ as o_doc_category,
src_bank_draft.rnk as rnk,
src_bank_draft.trans_acct_id as trans_acct_id,
src_bank_draft.source_record_id,
row_number() over (partition by src_bank_draft.source_record_id order by src_bank_draft.source_record_id) as RNK1
FROM
src_bank_draft
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''DOC_TYPE1''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = ''DOC_CTGY_TYPE4''
QUALIFY RNK1 = 1
);


-- Component LKP_DOC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DOC AS
(
SELECT
LKP.DOC_ID,
exp_src_pass.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_src_pass.source_record_id ORDER BY LKP.DOC_ID desc,LKP.TM_PRD_CD desc,LKP.DOC_CRTN_DTTM desc,LKP.DOC_RECPT_DT desc,LKP.DOC_PRD_STRT_DTTM desc,LKP.DOC_PRD_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.DOC_ISSUR_NUM desc,LKP.DATA_SRC_TYPE_CD desc,LKP.DOC_DESC_TXT desc,LKP.DOC_NAME desc,LKP.DOC_HOST_NUM desc,LKP.DOC_HOST_VERS_NUM desc,LKP.DOC_CYCL_CD desc,LKP.DOC_TYPE_CD desc,LKP.MM_OBJT_ID desc,LKP.DOC_CTGY_TYPE_CD desc,LKP.LANG_TYPE_CD desc,LKP.PRCS_ID desc,LKP.DOC_STS_CD desc) RNK
FROM
exp_src_pass
LEFT JOIN (
SELECT DOC.DOC_ID as DOC_ID, DOC.TM_PRD_CD as TM_PRD_CD, DOC.DOC_CRTN_DTTM as DOC_CRTN_DTTM, DOC.DOC_RECPT_DT as DOC_RECPT_DT, DOC.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM, DOC.DOC_PRD_END_DTTM as DOC_PRD_END_DTTM, DOC.EDW_STRT_DTTM as EDW_STRT_DTTM, DOC.DATA_SRC_TYPE_CD as DATA_SRC_TYPE_CD, DOC.DOC_DESC_TXT as DOC_DESC_TXT, DOC.DOC_NAME as DOC_NAME, DOC.DOC_HOST_NUM as DOC_HOST_NUM, DOC.DOC_HOST_VERS_NUM as DOC_HOST_VERS_NUM, DOC.DOC_CYCL_CD as DOC_CYCL_CD, DOC.MM_OBJT_ID as MM_OBJT_ID, DOC.LANG_TYPE_CD as LANG_TYPE_CD, DOC.PRCS_ID as PRCS_ID, DOC.DOC_STS_CD as DOC_STS_CD, DOC.DOC_ISSUR_NUM as DOC_ISSUR_NUM, DOC.DOC_TYPE_CD as DOC_TYPE_CD, DOC.DOC_CTGY_TYPE_CD as DOC_CTGY_TYPE_CD FROM DB_T_PROD_CORE.DOC
QUALIFY ROW_NUMBER () OVER (PARTITION BY DOC_ISSUR_NUM,DOC_CTGY_TYPE_CD,DOC_TYPE_CD ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.DOC_ISSUR_NUM = exp_src_pass.RefNumber AND LKP.DOC_TYPE_CD = exp_src_pass.o_doc_type AND LKP.DOC_CTGY_TYPE_CD = exp_src_pass.o_doc_category
QUALIFY RNK = 1
);


-- Component exp_type_code, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_type_code AS
(
SELECT
exp_src_pass.RefNumber as RefNumber,
exp_src_pass.Amount as Amount,
exp_src_pass.PaidDate as PaidDate,
CASE WHEN LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL END as o_typecode,
DATE_TRUNC(DAY, exp_src_pass.UpdateTime) as o_UpdateTime,
LKP_DOC.DOC_ID as DOC_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_src_pass.rnk as rnk,
exp_src_pass.trans_acct_id as trans_acct_id,
exp_src_pass.source_record_id
FROM
exp_src_pass
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_TERADATA_ETL_REF_XLAT ON exp_src_pass.source_record_id = LKP_TERADATA_ETL_REF_XLAT.source_record_id
INNER JOIN LKP_DOC ON LKP_TERADATA_ETL_REF_XLAT.source_record_id = LKP_DOC.source_record_id
);


-- Component LKP_BNK_DRFT_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_BNK_DRFT_TGT AS
(
SELECT
LKP.DRFT_TYPE_CD,
LKP.BNK_DRFT_NUM,
LKP.BNK_DRFT_AMT,
LKP.BNK_DRFT_CLRD_DTTM,
LKP.BNK_DRFT_VOID_DTTM,
LKP.EDW_STRT_DTTM,
LKP.BNK_DRFT_DOC_ID,
LKP.TRANS_ACCT_ID,
exp_type_code.DOC_ID as DOC_ID,
exp_type_code.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_type_code.source_record_id ORDER BY LKP.DRFT_TYPE_CD asc,LKP.BNK_DRFT_NUM asc,LKP.BNK_DRFT_AMT asc,LKP.BNK_DRFT_CLRD_DTTM asc,LKP.BNK_DRFT_VOID_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.BNK_DRFT_DOC_ID asc,LKP.TRANS_ACCT_ID asc) RNK
FROM
exp_type_code
LEFT JOIN (
SELECT BNK_DRFT.DRFT_TYPE_CD AS DRFT_TYPE_CD, BNK_DRFT.BNK_DRFT_NUM AS BNK_DRFT_NUM,
BNK_DRFT.BNK_DRFT_AMT AS BNK_DRFT_AMT, BNK_DRFT.BNK_DRFT_CLRD_DTTM AS BNK_DRFT_CLRD_DTTM, 
  BNK_DRFT.BNK_DRFT_VOID_DTTM AS BNK_DRFT_VOID_DTTM,BNK_DRFT.EDW_STRT_DTTM AS EDW_STRT_DTTM, BNK_DRFT.BNK_DRFT_DOC_ID AS BNK_DRFT_DOC_ID,BNK_DRFT.TRANS_ACCT_ID  AS TRANS_ACCT_ID  FROM DB_T_PROD_CORE.BNK_DRFT
QUALIFY ROW_NUMBER() OVER(PARTITION BY BNK_DRFT_DOC_ID,TRANS_ACCT_ID  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.BNK_DRFT_DOC_ID = exp_type_code.DOC_ID AND LKP.TRANS_ACCT_ID = exp_type_code.trans_acct_id
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_BNK_DRFT_TGT.BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
LKP_BNK_DRFT_TGT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_BNK_DRFT_TGT.TRANS_ACCT_ID as lkp_TRANS_ACCT_ID,
MD5 ( LTRIM ( RTRIM ( LKP_BNK_DRFT_TGT.DRFT_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_BNK_DRFT_TGT.BNK_DRFT_NUM ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_BNK_DRFT_TGT.BNK_DRFT_AMT ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_BNK_DRFT_TGT.BNK_DRFT_CLRD_DTTM ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( LKP_BNK_DRFT_TGT.BNK_DRFT_VOID_DTTM ) ) ) ) as lkp_Checksum,
exp_type_code.o_typecode as in_Typecode,
exp_type_code.RefNumber as in_RefNumber,
exp_type_code.Amount as in_Amount,
exp_type_code.PaidDate as in_PaidDate,
exp_type_code.o_UpdateTime as in_UpdateTime,
exp_type_code.DOC_ID as in_DOC_ID,
exp_type_code.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_type_code.in_EDW_END_DTTM as in_EDW_END_DTTM,
MD5 ( LTRIM ( RTRIM ( exp_type_code.o_typecode ) ) || LTRIM ( RTRIM ( exp_type_code.RefNumber ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_type_code.Amount ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_type_code.PaidDate ) ) ) || LTRIM ( RTRIM ( TO_CHAR ( exp_type_code.o_UpdateTime ) ) ) ) as in_Checksum,
CASE WHEN lkp_Checksum IS NULL THEN ''I'' ELSE CASE WHEN lkp_Checksum != in_Checksum THEN ''U'' ELSE ''R'' END END as Calc_ins_upd,
:PRCS_ID as o_process_id,
to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) as BNK_DRFT_STRT_DT,
exp_type_code.rnk as rnk,
exp_type_code.trans_acct_id as in_trans_acct_id,
exp_type_code.source_record_id
FROM
exp_type_code
INNER JOIN LKP_BNK_DRFT_TGT ON exp_type_code.source_record_id = LKP_BNK_DRFT_TGT.source_record_id
);


-- Component RTR_Insert_Update_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update_INSERT AS
(SELECT
exp_data_transformation.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.lkp_TRANS_ACCT_ID as lkp_TRANS_ACCT_ID,
exp_data_transformation.in_Typecode as in_Typecode,
exp_data_transformation.in_RefNumber as in_RefNumber,
exp_data_transformation.in_Amount as in_Amount,
exp_data_transformation.in_PaidDate as in_PaidDate,
exp_data_transformation.in_UpdateTime as in_UpdateTime,
exp_data_transformation.in_DOC_ID as in_DOC_ID,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.Calc_ins_upd as Calc_ins_upd,
exp_data_transformation.o_process_id as o_process_id,
exp_data_transformation.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT,
exp_data_transformation.rnk as RNK,
exp_data_transformation.in_trans_acct_id as in_trans_acct_id,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.Calc_ins_upd = ''I'' OR exp_data_transformation.Calc_ins_upd = ''U'');


-- Component RTR_Insert_Update_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE RTR_Insert_Update_UPDATE AS
(SELECT
exp_data_transformation.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID,
exp_data_transformation.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation.lkp_TRANS_ACCT_ID as lkp_TRANS_ACCT_ID,
exp_data_transformation.in_Typecode as in_Typecode,
exp_data_transformation.in_RefNumber as in_RefNumber,
exp_data_transformation.in_Amount as in_Amount,
exp_data_transformation.in_PaidDate as in_PaidDate,
exp_data_transformation.in_UpdateTime as in_UpdateTime,
exp_data_transformation.in_DOC_ID as in_DOC_ID,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.Calc_ins_upd as Calc_ins_upd,
exp_data_transformation.o_process_id as o_process_id,
exp_data_transformation.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT,
exp_data_transformation.rnk as RNK,
exp_data_transformation.in_trans_acct_id as in_trans_acct_id,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE FALSE 
-- exp_data_transformation.Calc_ins_upd = ''U''
);


-- Component upd_Bank_Draft_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_Bank_Draft_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_UPDATE.lkp_BNK_DRFT_DOC_ID as lkp_BNK_DRFT_DOC_ID3,
RTR_Insert_Update_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
RTR_Insert_Update_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
RTR_Insert_Update_UPDATE.o_process_id as o_process_id3,
1 as UPDATE_STRATEGY_ACTION,RTR_Insert_Update_UPDATE.SOURCE_RECORD_ID
FROM
RTR_Insert_Update_UPDATE
);


-- Component upd_bank_draft_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_bank_draft_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_INSERT.in_Typecode as in_Typecode1,
RTR_Insert_Update_INSERT.in_RefNumber as in_RefNumber1,
RTR_Insert_Update_INSERT.in_Amount as in_Amount1,
RTR_Insert_Update_INSERT.in_PaidDate as in_PaidDate1,
RTR_Insert_Update_INSERT.in_UpdateTime as in_UpdateTime1,
RTR_Insert_Update_INSERT.in_DOC_ID as in_DOC_ID1,
RTR_Insert_Update_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
RTR_Insert_Update_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
RTR_Insert_Update_INSERT.o_process_id as o_process_id1,
RTR_Insert_Update_INSERT.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT1,
RTR_Insert_Update_INSERT.RNK as RNK1,
RTR_Insert_Update_INSERT.in_trans_acct_id as in_trans_acct_id1,
0 as UPDATE_STRATEGY_ACTION,RTR_Insert_Update_INSERT.source_record_id
FROM
RTR_Insert_Update_INSERT
);


-- Component exp_ins_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target AS
(
SELECT
upd_bank_draft_ins.in_Typecode1 as in_Typecode1,
upd_bank_draft_ins.in_RefNumber1 as in_RefNumber1,
CASE WHEN upd_bank_draft_ins.in_Amount1 IS NULL THEN 0 ELSE upd_bank_draft_ins.in_Amount1 END as out_Amount1,
upd_bank_draft_ins.in_PaidDate1 as in_PaidDate1,
upd_bank_draft_ins.in_UpdateTime1 as in_UpdateTime1,
upd_bank_draft_ins.in_DOC_ID1 as in_DOC_ID1,
upd_bank_draft_ins.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_bank_draft_ins.o_process_id1 as o_process_id1,
upd_bank_draft_ins.BNK_DRFT_STRT_DT1 as BNK_DRFT_STRT_DT1,
DATEADD (
  SECOND,
  (2 * (upd_bank_draft_ins.RNK1 - 1)),
  CURRENT_TIMESTAMP()
) AS in_EDW_STRT_DTTM1,
upd_bank_draft_ins.in_trans_acct_id1 as in_trans_acct_id1,
upd_bank_draft_ins.source_record_id
FROM
upd_bank_draft_ins
);


-- Component upd_bank_draft_Update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_bank_draft_Update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTR_Insert_Update_UPDATE.in_Typecode as in_Typecode1,
RTR_Insert_Update_UPDATE.in_RefNumber as in_RefNumber1,
RTR_Insert_Update_UPDATE.in_Amount as in_Amount1,
RTR_Insert_Update_UPDATE.in_PaidDate as in_PaidDate1,
RTR_Insert_Update_UPDATE.in_UpdateTime as in_UpdateTime1,
RTR_Insert_Update_UPDATE.in_DOC_ID as in_DOC_ID1,
RTR_Insert_Update_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
RTR_Insert_Update_UPDATE.in_EDW_END_DTTM as in_EDW_END_DTTM1,
RTR_Insert_Update_UPDATE.o_process_id as o_process_id1,
RTR_Insert_Update_UPDATE.BNK_DRFT_STRT_DT as BNK_DRFT_STRT_DT3,
RTR_Insert_Update_UPDATE.in_trans_acct_id as in_trans_acct_id3,
0 as UPDATE_STRATEGY_ACTION,
RTR_Insert_Update_UPDATE.source_record_id
FROM
RTR_Insert_Update_UPDATE
);


-- Component Exp_Tgt_Upd_Pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_Tgt_Upd_Pass_to_tgt AS
(
SELECT
upd_Bank_Draft_upd.lkp_BNK_DRFT_DOC_ID3 as lkp_BNK_DRFT_DOC_ID3,
upd_Bank_Draft_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
DATEADD (SECOND, -1, upd_Bank_Draft_upd.in_EDW_STRT_DTTM3) AS Expiry_END_DATE,
upd_Bank_Draft_upd.source_record_id
FROM
upd_Bank_Draft_upd
);


-- Component exp_ins_pass_to_target_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_pass_to_target_update AS
(
SELECT
upd_bank_draft_Update.in_Typecode1 as in_Typecode1,
upd_bank_draft_Update.in_RefNumber1 as in_RefNumber1,
CASE WHEN upd_bank_draft_Update.in_Amount1 IS NULL THEN 0 ELSE upd_bank_draft_Update.in_Amount1 END as out_Amount1,
upd_bank_draft_Update.in_PaidDate1 as in_PaidDate1,
upd_bank_draft_Update.in_UpdateTime1 as in_UpdateTime1,
upd_bank_draft_Update.in_DOC_ID1 as in_DOC_ID1,
upd_bank_draft_Update.in_EDW_STRT_DTTM1 as in_EDW_STRT_DTTM1,
upd_bank_draft_Update.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_bank_draft_Update.o_process_id1 as o_process_id1,
upd_bank_draft_Update.BNK_DRFT_STRT_DT3 as BNK_DRFT_STRT_DT3,
upd_bank_draft_Update.in_trans_acct_id3 as in_trans_acct_id3,
upd_bank_draft_Update.source_record_id
FROM
upd_bank_draft_Update
);


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
exp_ins_pass_to_target_update.in_trans_acct_id3 as TRANS_ACCT_ID,
exp_ins_pass_to_target_update.in_UpdateTime1 as BNK_DRFT_VOID_DTTM,
exp_ins_pass_to_target_update.o_process_id1 as PRCS_ID,
exp_ins_pass_to_target_update.BNK_DRFT_STRT_DT3 as BNK_DRFT_STRT_DTTM,
exp_ins_pass_to_target_update.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_pass_to_target_update.in_EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_ins_pass_to_target_update;


-- Component tgt_bnk_drft_insert, Type TARGET 
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
exp_ins_pass_to_target.in_DOC_ID1 as BNK_DRFT_DOC_ID,
exp_ins_pass_to_target.in_Typecode1 as DRFT_TYPE_CD,
exp_ins_pass_to_target.in_RefNumber1 as BNK_DRFT_NUM,
exp_ins_pass_to_target.out_Amount1 as BNK_DRFT_AMT,
exp_ins_pass_to_target.in_PaidDate1 as BNK_DRFT_CLRD_DTTM,
exp_ins_pass_to_target.in_trans_acct_id1 as TRANS_ACCT_ID,
exp_ins_pass_to_target.in_UpdateTime1 as BNK_DRFT_VOID_DTTM,
exp_ins_pass_to_target.o_process_id1 as PRCS_ID,
exp_ins_pass_to_target.BNK_DRFT_STRT_DT1 as BNK_DRFT_STRT_DTTM,
exp_ins_pass_to_target.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_ins_pass_to_target.in_EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_ins_pass_to_target;


-- Component tgt_bnk_drft_insert, Type Post SQL 
UPDATE  DB_T_PROD_CORE.BNK_DRFT  FROM  

(

SELECT	distinct BNK_DRFT_DOC_ID,EDW_STRT_DTTM,TRANS_ACCT_ID, 

max(EDW_STRT_DTTM) over (partition by BNK_DRFT_DOC_ID,TRANS_ACCT_ID  ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1

FROM	DB_T_PROD_CORE.BNK_DRFT

)  A

SET EDW_END_DTTM= A.lead1

where BNK_DRFT.BNK_DRFT_DOC_ID = A.BNK_DRFT_DOC_ID 

and  BNK_DRFT.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

and  BNK_DRFT.TRANS_ACCT_ID= A.TRANS_ACCT_ID

and A.lead1 is not null  ;


-- Component tgt_bnk_drft_update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.BNK_DRFT
USING Exp_Tgt_Upd_Pass_to_tgt ON (BNK_DRFT.BNK_DRFT_DOC_ID = Exp_Tgt_Upd_Pass_to_tgt.lkp_BNK_DRFT_DOC_ID3 AND BNK_DRFT.EDW_STRT_DTTM = Exp_Tgt_Upd_Pass_to_tgt.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
BNK_DRFT_DOC_ID = Exp_Tgt_Upd_Pass_to_tgt.lkp_BNK_DRFT_DOC_ID3,
EDW_STRT_DTTM = Exp_Tgt_Upd_Pass_to_tgt.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = Exp_Tgt_Upd_Pass_to_tgt.Expiry_END_DATE;


END; ';