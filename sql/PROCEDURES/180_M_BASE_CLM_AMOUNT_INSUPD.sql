-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_AMOUNT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	PRCS_ID integer;

BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
PRCS_ID := 1;

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


-- Component sq_cc_claim, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_claim AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ClaimNumber,
$2 as TYPECODE,
$3 as TransactionAmount,
$4 as src_cd,
$5 as Retired,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT distinct ClaimNumber,

typecode, 

TransactionAmount,

''SRC_SYS6'' as SRC_CD,

Retired

from (

SELECT cc_transactionlineitem.Retired_stg as Retired, 

 cc_transactionlineitem.UpdateTime_stg as UpdateTime,

cc_transactionlineitem.TransactionAmount_stg as TransactionAmount,

cc_transaction.Subtype_stg as Subtype, 

cctl_transaction.ID_stg as ID,

cc_claim.ClaimNumber_stg as ClaimNumber, 

cctl_transaction.typecode_stg as typecode

FROM

 DB_T_PROD_STAG.cc_transactionlineitem inner join  DB_T_PROD_STAG.cc_transaction 

on cc_transactionlineitem.TransactionID_stg =cc_transaction.id_stg inner join  DB_T_PROD_STAG.cctl_transactionstatus 

on cctl_transactionstatus.ID_stg = cc_transaction.Status_stg 

left outer join  DB_T_PROD_STAG.cc_check on cc_check.id_stg = cc_transaction.CheckID_stg

inner join (select cc_claim.id_stg,State_stg,ClaimNumber_stg from  DB_T_PROD_STAG.cc_claim 

inner join  DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim on cc_claim.id_stg=cc_transaction.claimid_stg

join  DB_T_PROD_STAG.cctl_transaction 

on cctl_transaction.ID_stg = cc_transaction.Subtype_stg

where cc_transactionlineitem.UpdateTime_stg > (:start_dttm)

and cc_transactionlineitem.UpdateTime_stg <= (:end_dttm))a
) SRC
)
);


-- Component exp_default_values, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_default_values AS
(
SELECT
sq_cc_claim.ClaimNumber as ClaimNumber,
sq_cc_claim.TYPECODE as TYPECODE,
sq_cc_claim.TransactionAmount as TransactionAmount,
''UNK'' as clm_amt_seq_num,
:PRCS_ID as process_id,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_src_cd,
sq_cc_claim.Retired as Retired,
sq_cc_claim.source_record_id,
row_number() over (partition by sq_cc_claim.source_record_id order by sq_cc_claim.source_record_id) as RNK
FROM
sq_cc_claim
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_cc_claim.src_cd
QUALIFY RNK = 1
);


-- Component agg_sum_amount, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE agg_sum_amount AS
(
SELECT
exp_default_values.ClaimNumber as ClaimNumber,
exp_default_values.TYPECODE as TYPECODE,
MIN(exp_default_values.TransactionAmount) as TransactionAmount,
sum(TransactionAmount) as sum_TransactionAmount,
MIN(exp_default_values.clm_amt_seq_num) as clm_amt_seq_num,
MIN(exp_default_values.process_id) as process_id,
MIN(exp_default_values.out_src_cd) as src_cd,
MIN(exp_default_values.Retired) as Retired,
max(Retired) as out_retired,
MIN(exp_default_values.source_record_id) as source_record_id
FROM
exp_default_values
GROUP BY
exp_default_values.ClaimNumber,
exp_default_values.TYPECODE
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
agg_sum_amount.source_record_id,
ROW_NUMBER() OVER(PARTITION BY agg_sum_amount.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
agg_sum_amount
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM db_t_prod_core.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = agg_sum_amount.ClaimNumber AND LKP.SRC_SYS_CD = agg_sum_amount.src_cd
QUALIFY RNK = 1
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
LKP_CLM.CLM_ID as CLM_ID,
agg_sum_amount.TYPECODE as TYPECODE,
agg_sum_amount.sum_TransactionAmount as sum_TransactionAmount,
agg_sum_amount.clm_amt_seq_num as clm_amt_seq_num1,
agg_sum_amount.process_id as process_id,
agg_sum_amount.out_retired as Retired,
agg_sum_amount.source_record_id
FROM
agg_sum_amount
INNER JOIN LKP_CLM ON agg_sum_amount.source_record_id = LKP_CLM.source_record_id
);


-- Component exp_convert_typecode, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_convert_typecode AS
(
SELECT
CASE WHEN LKP_TERADATA_ETL_REF_XLAT_SRC_CD.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT_SRC_CD.TGT_IDNTFTN_VAL END as out_TGT_REF_TYPE_CD,
exp_pass_through.Retired as Retired,
exp_pass_through.source_record_id
FROM
exp_pass_through
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD ON exp_pass_through.source_record_id = LKP_TERADATA_ETL_REF_XLAT_SRC_CD.source_record_id
);


-- Component LKP_CLM_AMT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_AMT AS
(
SELECT
exp_pass_through.CLM_ID as in_CLM_ID,
exp_convert_typecode.out_TGT_REF_TYPE_CD as in_AMT_TYPE_CD,
exp_pass_through.clm_amt_seq_num1 as in_clm_amt_seq_num,
LKP.CLM_ID,
LKP.CLM_AMT_TYPE_CD,
LKP.CLM_AMT_SEQ_NUM,
LKP.CLM_AMT,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.CLM_ID asc,LKP.CLM_AMT_TYPE_CD asc,LKP.CLM_AMT_SEQ_NUM asc,LKP.CLM_AMT asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_pass_through
INNER JOIN exp_convert_typecode ON exp_pass_through.source_record_id = exp_convert_typecode.source_record_id
LEFT JOIN (
SELECT CLM_AMT.CLM_AMT as CLM_AMT,  CLM_AMT.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_AMT.EDW_END_DTTM as EDW_END_DTTM, CLM_AMT.CLM_ID as CLM_ID, CLM_AMT.CLM_AMT_TYPE_CD as CLM_AMT_TYPE_CD, CLM_AMT.CLM_AMT_SEQ_NUM as CLM_AMT_SEQ_NUM from db_t_prod_core.CLM_AMT
qualify row_number () over (partition by CLM_ID,CLM_AMT_TYPE_CD,CLM_AMT_SEQ_NUM order by EDW_END_DTTM desc)=1
) LKP ON LKP.CLM_ID = exp_pass_through.CLM_ID AND LKP.CLM_AMT_TYPE_CD = exp_convert_typecode.out_TGT_REF_TYPE_CD AND LKP.CLM_AMT_SEQ_NUM = exp_pass_through.clm_amt_seq_num1
QUALIFY RNK = 1
);


-- Component exp_CDC, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC AS
(
SELECT
LKP_CLM_AMT.in_CLM_ID as in_CLM_ID,
LKP_CLM_AMT.in_AMT_TYPE_CD as in_AMT_TYPE_CD,
LKP_CLM_AMT.in_clm_amt_seq_num as in_clm_amt_seq_num,
exp_pass_through.sum_TransactionAmount as in_clm_amt,
CASE WHEN to_char ( TO_NUMBER(exp_pass_through.sum_TransactionAmount) ) = ''0'' THEN ''0'' ELSE TO_CHAR ( LTRIM ( RTRIM ( exp_pass_through.sum_TransactionAmount ) ) ) END as v_clm_amt,
exp_pass_through.process_id as in_process_id,
LKP_CLM_AMT.CLM_ID as CLM_ID,
to_char ( LKP_CLM_AMT.CLM_AMT ) as v_lkp_CLM_AMT,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
md5 ( v_clm_amt ) as v_MD5_src,
MD5 ( v_lkp_CLM_AMT ) as v_MD5_tgt,
CASE WHEN v_MD5_tgt IS NULL THEN ''I'' ELSE CASE WHEN v_MD5_src = v_MD5_tgt THEN ''X'' ELSE ''U'' END END as o_Md5_Check,
LKP_CLM_AMT.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
exp_convert_typecode.Retired as Retired,
LKP_CLM_AMT.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_pass_through.source_record_id
FROM
exp_pass_through
INNER JOIN exp_convert_typecode ON exp_pass_through.source_record_id = exp_convert_typecode.source_record_id
INNER JOIN LKP_CLM_AMT ON exp_convert_typecode.source_record_id = LKP_CLM_AMT.source_record_id
);


-- Component rtr_ins_upd_condition_Insert, Type ROUTER Output Group Insert
create or replace temporary table rtr_ins_upd_condition_Insert as
SELECT
exp_CDC.in_CLM_ID as CLM_ID1,
exp_CDC.in_AMT_TYPE_CD as TYPECODE,
exp_CDC.in_clm_amt_seq_num as clm_amt_seq_num1,
exp_CDC.in_clm_amt as sum_TransactionAmount,
exp_CDC.in_process_id as process_id,
exp_CDC.o_Md5_Check as o_flag,
exp_CDC.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_CDC.EDW_END_DTTM as EDW_END_DTTM,
exp_CDC.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp_CDC.Retired as Retired,
exp_CDC.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC.source_record_id
FROM
exp_CDC
WHERE exp_CDC.o_Md5_Check = ''I'' AND exp_CDC.in_CLM_ID IS NOT NULL OR ( exp_CDC.Retired = 0 and exp_CDC.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );


-- Component rtr_ins_upd_condition_Retire, Type ROUTER Output Group Retire
create or replace temporary table rtr_ins_upd_condition_Retire as
SELECT
exp_CDC.in_CLM_ID as CLM_ID1,
exp_CDC.in_AMT_TYPE_CD as TYPECODE,
exp_CDC.in_clm_amt_seq_num as clm_amt_seq_num1,
exp_CDC.in_clm_amt as sum_TransactionAmount,
exp_CDC.in_process_id as process_id,
exp_CDC.o_Md5_Check as o_flag,
exp_CDC.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_CDC.EDW_END_DTTM as EDW_END_DTTM,
exp_CDC.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp_CDC.Retired as Retired,
exp_CDC.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC.source_record_id
FROM
exp_CDC
WHERE exp_CDC.o_Md5_Check = ''X'' and exp_CDC.Retired != 0 and exp_CDC.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component rtr_ins_upd_condition_Update, Type ROUTER Output Group Update
create or replace temporary table rtr_ins_upd_condition_Update as
SELECT
exp_CDC.in_CLM_ID as CLM_ID1,
exp_CDC.in_AMT_TYPE_CD as TYPECODE,
exp_CDC.in_clm_amt_seq_num as clm_amt_seq_num1,
exp_CDC.in_clm_amt as sum_TransactionAmount,
exp_CDC.in_process_id as process_id,
exp_CDC.o_Md5_Check as o_flag,
exp_CDC.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_CDC.EDW_END_DTTM as EDW_END_DTTM,
exp_CDC.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
exp_CDC.Retired as Retired,
exp_CDC.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC.source_record_id
FROM
exp_CDC
WHERE exp_CDC.o_Md5_Check = ''U'' AND exp_CDC.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_condition_Update.CLM_ID1 as CLM_ID13,
rtr_ins_upd_condition_Update.TYPECODE as TYPECODE3,
rtr_ins_upd_condition_Update.clm_amt_seq_num1 as clm_amt_seq_num13,
rtr_ins_upd_condition_Update.sum_TransactionAmount as sum_TransactionAmount3,
rtr_ins_upd_condition_Update.process_id as process_id3,
rtr_ins_upd_condition_Update.o_flag as o_flag3,
rtr_ins_upd_condition_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_condition_Update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_ins_upd_condition_Update.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM13,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_condition_Update.source_record_id
FROM
rtr_ins_upd_condition_Update
);


-- Component exp_Update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Update AS
(
SELECT
upd_update.CLM_ID13 as CLM_ID13,
upd_update.TYPECODE3 as TYPECODE3,
upd_update.clm_amt_seq_num13 as clm_amt_seq_num13,
dateadd (second, -1, CURRENT_TIMESTAMP) as EDW_END_DTTM3,
upd_update.lkp_EDW_STRT_DTTM13 as lkp_EDW_STRT_DTTM13,
upd_update.source_record_id
FROM
upd_update
);


-- Component tgt_clm_amt_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_AMT
USING exp_Update ON (CLM_AMT.CLM_ID = exp_Update.CLM_ID13 AND CLM_AMT.CLM_AMT_TYPE_CD = exp_Update.TYPECODE3 AND CLM_AMT.CLM_AMT_SEQ_NUM = exp_Update.clm_amt_seq_num13 AND CLM_AMT.EDW_STRT_DTTM = exp_Update.lkp_EDW_STRT_DTTM13)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_Update.CLM_ID13,
CLM_AMT_TYPE_CD = exp_Update.TYPECODE3,
CLM_AMT_SEQ_NUM = exp_Update.clm_amt_seq_num13,
EDW_STRT_DTTM = exp_Update.lkp_EDW_STRT_DTTM13,
EDW_END_DTTM = exp_Update.EDW_END_DTTM3;


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
rtr_ins_upd_condition_Update.CLM_ID1 as CLM_ID13,
rtr_ins_upd_condition_Update.TYPECODE as TYPECODE3,
rtr_ins_upd_condition_Update.clm_amt_seq_num1 as clm_amt_seq_num13,
rtr_ins_upd_condition_Update.sum_TransactionAmount as sum_TransactionAmount3,
rtr_ins_upd_condition_Update.process_id as process_id3,
rtr_ins_upd_condition_Update.o_flag as o_flag3,
rtr_ins_upd_condition_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_condition_Update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_ins_upd_condition_Update.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM13,
rtr_ins_upd_condition_Update.Retired as Retired3,
rtr_ins_upd_condition_Update.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_ins_upd_condition_Update.source_record_id
FROM
rtr_ins_upd_condition_Update
WHERE rtr_ins_upd_condition_Update.Retired = 0
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
rtr_ins_upd_condition_Insert.CLM_ID1 as CLM_ID11,
rtr_ins_upd_condition_Insert.TYPECODE as TYPECODE1,
rtr_ins_upd_condition_Insert.clm_amt_seq_num1 as clm_amt_seq_num11,
rtr_ins_upd_condition_Insert.sum_TransactionAmount as sum_TransactionAmount1,
rtr_ins_upd_condition_Insert.process_id as process_id1,
rtr_ins_upd_condition_Insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
CASE WHEN rtr_ins_upd_condition_Insert.Retired = 0 THEN rtr_ins_upd_condition_Insert.EDW_END_DTTM ELSE CURRENT_TIMESTAMP END as EDW_END_DTTM11,
rtr_ins_upd_condition_Insert.source_record_id
FROM
rtr_ins_upd_condition_Insert
);


-- Component tgt_clm_amt_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_AMT
(
CLM_ID,
CLM_AMT_TYPE_CD,
CLM_AMT_SEQ_NUM,
CLM_AMT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
FILTRANS.CLM_ID13 as CLM_ID,
FILTRANS.TYPECODE3 as CLM_AMT_TYPE_CD,
FILTRANS.clm_amt_seq_num13 as CLM_AMT_SEQ_NUM,
FILTRANS.sum_TransactionAmount3 as CLM_AMT,
FILTRANS.process_id3 as PRCS_ID,
FILTRANS.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
FILTRANS.EDW_END_DTTM3 as EDW_END_DTTM
FROM
FILTRANS;


-- Component upd_retire, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_retire AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_condition_Retire.CLM_ID1 as CLM_ID13,
rtr_ins_upd_condition_Retire.TYPECODE as TYPECODE3,
rtr_ins_upd_condition_Retire.clm_amt_seq_num1 as clm_amt_seq_num13,
rtr_ins_upd_condition_Retire.sum_TransactionAmount as sum_TransactionAmount3,
rtr_ins_upd_condition_Retire.process_id as process_id3,
rtr_ins_upd_condition_Retire.o_flag as o_flag3,
rtr_ins_upd_condition_Retire.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_condition_Retire.EDW_END_DTTM as EDW_END_DTTM3,
rtr_ins_upd_condition_Retire.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM13,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_condition_Retire.source_record_id
FROM
rtr_ins_upd_condition_Retire
);


-- Component tgt_clm_amt_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_AMT
(
CLM_ID,
CLM_AMT_TYPE_CD,
CLM_AMT_SEQ_NUM,
CLM_AMT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
EXPTRANS.CLM_ID11 as CLM_ID,
EXPTRANS.TYPECODE1 as CLM_AMT_TYPE_CD,
EXPTRANS.clm_amt_seq_num11 as CLM_AMT_SEQ_NUM,
EXPTRANS.sum_TransactionAmount1 as CLM_AMT,
EXPTRANS.process_id1 as PRCS_ID,
EXPTRANS.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
EXPTRANS.EDW_END_DTTM11 as EDW_END_DTTM
FROM
EXPTRANS;


-- Component exp_Update1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Update1 AS
(
SELECT
upd_retire.CLM_ID13 as CLM_ID13,
upd_retire.TYPECODE3 as TYPECODE3,
upd_retire.clm_amt_seq_num13 as clm_amt_seq_num13,
CURRENT_TIMESTAMP as EDW_END_DTTM3,
upd_retire.lkp_EDW_STRT_DTTM13 as lkp_EDW_STRT_DTTM13,
upd_retire.source_record_id
FROM
upd_retire
);


-- Component tgt_clm_amt_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_AMT
USING exp_Update1 ON (CLM_AMT.CLM_ID = exp_Update1.CLM_ID13 AND CLM_AMT.CLM_AMT_TYPE_CD = exp_Update1.TYPECODE3 AND CLM_AMT.CLM_AMT_SEQ_NUM = exp_Update1.clm_amt_seq_num13 AND CLM_AMT.EDW_STRT_DTTM = exp_Update1.lkp_EDW_STRT_DTTM13)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_Update1.CLM_ID13,
CLM_AMT_TYPE_CD = exp_Update1.TYPECODE3,
CLM_AMT_SEQ_NUM = exp_Update1.clm_amt_seq_num13,
EDW_STRT_DTTM = exp_Update1.lkp_EDW_STRT_DTTM13,
EDW_END_DTTM = exp_Update1.EDW_END_DTTM3;


END; ';