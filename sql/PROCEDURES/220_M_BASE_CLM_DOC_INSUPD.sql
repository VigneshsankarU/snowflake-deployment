-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_DOC_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
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


-- Component SQ_cc_claim, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_claim AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ClaimNumber,
$2 as CheckNumber,
$3 as Doc_Type,
$4 as Doc_CTGY,
$5 as CLM_SRC_CD,
$6 as UpdateTime,
$7 as Retired,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT distinct 

  cc_claim.ClaimNumber_stg ClaimNumber,

  cc_check.PublicID_stg PublicID,

  ''DOC_TYPE1'' as doc_type,

  ''DOC_CTGY_TYPE1''  as doc_ctgy,

  ''SRC_SYS6'' as CLM_SRC_CD, 

  cc_check.UpdateTime_stg UpdateTime, 

  case when cc_claim.Retired_stg=0 and cc_check.retired_stg=0 then 0 else 1 end Retired

FROM

(select cclm.ClaimNumber_stg,cclm.Retired_stg, cclm.id_stg from DB_T_PROD_STAG.cc_claim cclm

inner join DB_T_PROD_STAG.cctl_claimstate on cclm.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

join DB_T_PROD_STAG.cc_check on cc_claim.id_stg = cc_check.claimid_stg

join DB_T_PROD_STAG.cc_transaction on cc_check.id_stg =cc_transaction.checkid_stg 

join DB_T_PROD_STAG.cc_transactionlineitem on cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg

join DB_T_PROD_STAG.cctl_transactionstatus on cc_check.Status_stg = cctl_transactionstatus.ID_stg

join DB_T_PROD_STAG.cctl_paymentmethod on cc_check.PaymentMethod_stg = cctl_paymentmethod.ID_stg

left join DB_T_PROD_STAG.cctl_insurpaymethod_ext on cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg

where cc_check.UpdateTime_stg>:start_dttm AND cc_check.UpdateTime_stg <= :end_dttm

and  cctl_paymentmethod.TYPECODE_stg <> ''expenseWithheld_alfa''
) SRC
)
);


-- Component exp_all_srcs, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_srcs AS
(
SELECT
SQ_cc_claim.ClaimNumber as ClaimNumber,
SQ_cc_claim.CheckNumber as CheckNumber,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */ as o_Doc_Type,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */ as o_Doc_CTGY,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_CLM_SRC_CD,
SQ_cc_claim.UpdateTime as UpdateTime,
SQ_cc_claim.Retired as Retired,
SQ_cc_claim.source_record_id,
row_number() over (partition by SQ_cc_claim.source_record_id order by SQ_cc_claim.source_record_id) as RNK
FROM
SQ_cc_claim
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_claim.Doc_Type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_claim.Doc_CTGY
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_cc_claim.CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_all_srcs.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_srcs.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_all_srcs
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_all_srcs.ClaimNumber AND LKP.SRC_SYS_CD = exp_all_srcs.out_CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_DOC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_DOC AS
(
SELECT
LKP.DOC_ID,
exp_all_srcs.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_srcs.source_record_id ORDER BY LKP.DOC_ID desc,LKP.TM_PRD_CD desc,LKP.DOC_CRTN_DTTM desc,LKP.DOC_RECPT_DT desc,LKP.DOC_PRD_STRT_DTTM desc,LKP.DOC_PRD_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.DOC_ISSUR_NUM desc,LKP.DATA_SRC_TYPE_CD desc,LKP.DOC_DESC_TXT desc,LKP.DOC_NAME desc,LKP.DOC_HOST_NUM desc,LKP.DOC_HOST_VERS_NUM desc,LKP.DOC_CYCL_CD desc,LKP.DOC_TYPE_CD desc,LKP.MM_OBJT_ID desc,LKP.DOC_CTGY_TYPE_CD desc,LKP.LANG_TYPE_CD desc,LKP.PRCS_ID desc,LKP.DOC_STS_CD desc) RNK
FROM
exp_all_srcs
LEFT JOIN (
SELECT DOC.DOC_ID as DOC_ID, DOC.TM_PRD_CD as TM_PRD_CD, DOC.DOC_CRTN_DTTM as DOC_CRTN_DTTM, DOC.DOC_RECPT_DT as DOC_RECPT_DT, DOC.DOC_PRD_STRT_DTTM as DOC_PRD_STRT_DTTM, DOC.DOC_PRD_END_DTTM as DOC_PRD_END_DTTM, DOC.EDW_STRT_DTTM as EDW_STRT_DTTM, DOC.DATA_SRC_TYPE_CD as DATA_SRC_TYPE_CD, DOC.DOC_DESC_TXT as DOC_DESC_TXT, DOC.DOC_NAME as DOC_NAME, DOC.DOC_HOST_NUM as DOC_HOST_NUM, DOC.DOC_HOST_VERS_NUM as DOC_HOST_VERS_NUM, DOC.DOC_CYCL_CD as DOC_CYCL_CD, DOC.MM_OBJT_ID as MM_OBJT_ID, DOC.LANG_TYPE_CD as LANG_TYPE_CD, DOC.PRCS_ID as PRCS_ID, DOC.DOC_STS_CD as DOC_STS_CD, DOC.DOC_ISSUR_NUM as DOC_ISSUR_NUM, DOC.DOC_TYPE_CD as DOC_TYPE_CD, DOC.DOC_CTGY_TYPE_CD as DOC_CTGY_TYPE_CD FROM DOC
QUALIFY ROW_NUMBER () OVER (PARTITION BY DOC_ISSUR_NUM,DOC_CTGY_TYPE_CD,DOC_TYPE_CD ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.DOC_ISSUR_NUM = exp_all_srcs.CheckNumber AND LKP.DOC_TYPE_CD = exp_all_srcs.o_Doc_Type AND LKP.DOC_CTGY_TYPE_CD = exp_all_srcs.o_Doc_CTGY
QUALIFY RNK = 1
);


-- Component LKP_CLM_DOC_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_DOC_ID AS
(
SELECT
LKP.DOC_ID,
LKP.CLM_ID,
LKP.PROOF_OF_LOSS_IND,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP_CLM.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_CLM.source_record_id ORDER BY LKP.DOC_ID asc,LKP.CLM_ID asc,LKP.PROOF_OF_LOSS_IND asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
LKP_CLM
INNER JOIN LKP_DOC ON LKP_CLM.source_record_id = LKP_DOC.source_record_id
LEFT JOIN (
SELECT CLM_DOC.PROOF_OF_LOSS_IND as PROOF_OF_LOSS_IND, CLM_DOC.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_DOC.EDW_END_DTTM as EDW_END_DTTM, CLM_DOC.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM_DOC.DOC_ID as DOC_ID, CLM_DOC.CLM_ID as CLM_ID FROM CLM_DOC as CLM_DOC
QUALIFY ROW_NUMBER() OVER(PARTITION BY DOC_ID,CLM_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.DOC_ID = LKP_DOC.DOC_ID AND LKP.CLM_ID = LKP_CLM.CLM_ID
QUALIFY RNK = 1
);


-- Component exp_data_transformations, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformations AS
(
SELECT
LKP_DOC.DOC_ID as PRNT_DOC_ID,
LKP_CLM_DOC_ID.DOC_ID as lkp_DOC_ID,
LKP_CLM_DOC_ID.CLM_ID as lkp_CLM_ID,
LKP_CLM.CLM_ID as PRNT_CLM_ID,
:PRCS_ID as PRCS_ID,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_ip THEN ''U'' ELSE ''R'' END END as o_flag,
''UNK'' as in_PROOF_OF_LOSS,
exp_all_srcs.UpdateTime as in_TRNS_STRT_DTTM,
MD5 ( ltrim ( rtrim ( LKP_CLM_DOC_ID.PROOF_OF_LOSS_IND ) ) ) as chksum_lkp,
MD5 ( ltrim ( rtrim ( in_PROOF_OF_LOSS ) ) ) as chksum_ip,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
LKP_CLM_DOC_ID.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
ADD_TO_DATE ( CURRENT_TIMESTAMP , ''ss'' , - 1 ) as EDW_END_DTTM_exp,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_all_srcs.Retired as Retired,
LKP_CLM_DOC_ID.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_srcs.source_record_id
FROM
exp_all_srcs
INNER JOIN LKP_CLM ON exp_all_srcs.source_record_id = LKP_CLM.source_record_id
INNER JOIN LKP_DOC ON LKP_CLM.source_record_id = LKP_DOC.source_record_id
INNER JOIN LKP_CLM_DOC_ID ON LKP_DOC.source_record_id = LKP_CLM_DOC_ID.source_record_id
);


-- Component rtr_clm_doc_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_doc_INSERT AS
(SELECT
exp_data_transformations.PRNT_DOC_ID as PRNT_DOC_ID,
exp_data_transformations.PRNT_CLM_ID as CLM_ID,
exp_data_transformations.PRCS_ID as PRCS_ID,
exp_data_transformations.o_flag as o_flag,
exp_data_transformations.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformations.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformations.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformations.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformations.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM,
exp_data_transformations.Retired as Retired,
exp_data_transformations.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformations.lkp_DOC_ID as lkp_DOC_ID,
exp_data_transformations.lkp_CLM_ID as lkp_CLM_ID,
exp_data_transformations.source_record_id
FROM
exp_data_transformations
WHERE ( exp_data_transformations.o_flag = ''I'' OR ( exp_data_transformations.lkp_EDW_END_DTTM != TO_TIMESTAMP (
  ''12/31/9999 23:59:59.999999'',
  ''mm/DD/yyyy hh24:mi:ss.ff6''
) and exp_data_transformations.Retired = 0 ) ) and exp_data_transformations.PRNT_DOC_ID IS NOT NULL and exp_data_transformations.PRNT_CLM_ID IS NOT NULL -- exp_data_transformations.o_flag = ''I'' and exp_data_transformations.PRNT_DOC_ID IS NOT NULL
);


-- Component rtr_clm_doc_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_doc_RETIRED AS
(SELECT
exp_data_transformations.PRNT_DOC_ID as PRNT_DOC_ID,
exp_data_transformations.PRNT_CLM_ID as CLM_ID,
exp_data_transformations.PRCS_ID as PRCS_ID,
exp_data_transformations.o_flag as o_flag,
exp_data_transformations.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformations.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformations.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformations.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformations.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM,
exp_data_transformations.Retired as Retired,
exp_data_transformations.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformations.lkp_DOC_ID as lkp_DOC_ID,
exp_data_transformations.lkp_CLM_ID as lkp_CLM_ID,
exp_data_transformations.source_record_id
FROM
exp_data_transformations
WHERE exp_data_transformations.o_flag = ''R'' and exp_data_transformations.Retired != 0 and exp_data_transformations.lkp_EDW_END_DTTM = TO_TIMESTAMP (
  ''12/31/9999 23:59:59.999999'',
  ''mm/DD/yyyy hh24:mi:ss.ff6''
));


-- Component rtr_clm_doc_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_doc_UPDATE AS
(SELECT
exp_data_transformations.PRNT_DOC_ID as PRNT_DOC_ID,
exp_data_transformations.PRNT_CLM_ID as CLM_ID,
exp_data_transformations.PRCS_ID as PRCS_ID,
exp_data_transformations.o_flag as o_flag,
exp_data_transformations.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformations.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformations.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_data_transformations.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformations.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM,
exp_data_transformations.Retired as Retired,
exp_data_transformations.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformations.lkp_DOC_ID as lkp_DOC_ID,
exp_data_transformations.lkp_CLM_ID as lkp_CLM_ID,
exp_data_transformations.source_record_id
FROM
exp_data_transformations
WHERE exp_data_transformations.o_flag = ''U'' AND exp_data_transformations.lkp_EDW_END_DTTM = TO_TIMESTAMP (
  ''12/31/9999 23:59:59.999999'',
  ''mm/DD/yyyy hh24:mi:ss.ff6''
) -- exp_data_transformations.o_flag = ''U'' and exp_data_transformations.PRNT_DOC_ID IS NOT NULL
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
rtr_clm_doc_UPDATE.PRNT_DOC_ID as PRNT_DOC_ID,
rtr_clm_doc_UPDATE.CLM_ID as CLM_ID,
rtr_clm_doc_UPDATE.PRCS_ID as PRCS_ID,
rtr_clm_doc_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_clm_doc_UPDATE.EDW_END_DTTM as EDW_END_DTTM31,
rtr_clm_doc_UPDATE.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM3,
rtr_clm_doc_UPDATE.Retired as Retired3,
rtr_clm_doc_UPDATE.source_record_id
FROM
rtr_clm_doc_UPDATE
WHERE rtr_clm_doc_UPDATE.Retired = 0
);


-- Component upd_clm_doc_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_clm_doc_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_doc_UPDATE.lkp_DOC_ID as DOC_ID1,
rtr_clm_doc_UPDATE.lkp_CLM_ID as CLM_ID1,
rtr_clm_doc_UPDATE.PRCS_ID as PRCS_ID1,
rtr_clm_doc_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_clm_doc_UPDATE.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM3,
rtr_clm_doc_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_clm_doc_UPDATE
);


-- Component exp_pass_to_tgt_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd AS
(
SELECT
upd_clm_doc_upd.DOC_ID1 as DOC_ID1,
upd_clm_doc_upd.CLM_ID1 as CLM_ID1,
upd_clm_doc_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
ADD_TO_DATE ( upd_clm_doc_upd.EDW_STRT_DTTM3 , ''ss'' , - 1 ) as EDW_END_DTTM_exp3,
TO_DATE ( ''01/01/1900'' , ''mm/dd/yyyy'' ) as out_start_dt,
add_to_date ( upd_clm_doc_upd.in_TRNS_STRT_DTTM3 , ''ss'' , - 1 ) as in_TRNS_STRT_DTTM31,
upd_clm_doc_upd.source_record_id
FROM
upd_clm_doc_upd
);


-- Component upd_clm_doc_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_clm_doc_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
FILTRANS.PRNT_DOC_ID as PRNT_DOC_ID,
FILTRANS.CLM_ID as CLM_ID,
FILTRANS.PRCS_ID as PRCS_ID,
FILTRANS.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
FILTRANS.EDW_END_DTTM31 as EDW_END_DTTM3,
FILTRANS.in_TRNS_STRT_DTTM3 as in_TRNS_STRT_DTTM3,
0 as UPDATE_STRATEGY_ACTION
FROM
FILTRANS
);


-- Component upd_clm_doc_upd_retire, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_clm_doc_upd_retire AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_doc_RETIRED.lkp_DOC_ID as DOC_ID1,
rtr_clm_doc_RETIRED.lkp_CLM_ID as CLM_ID1,
rtr_clm_doc_RETIRED.PRCS_ID as PRCS_ID1,
rtr_clm_doc_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_clm_doc_RETIRED.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_clm_doc_RETIRED
);


-- Component upd_clm_doc_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_clm_doc_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_doc_INSERT.PRNT_DOC_ID as PRNT_DOC_ID,
rtr_clm_doc_INSERT.CLM_ID as CLM_ID,
rtr_clm_doc_INSERT.PRCS_ID as PRCS_ID,
rtr_clm_doc_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_clm_doc_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_clm_doc_INSERT.in_TRNS_STRT_DTTM as in_TRNS_STRT_DTTM1,
rtr_clm_doc_INSERT.Retired as Retired1,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_clm_doc_INSERT
);


-- Component exp_pass_to_tgt_ins1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins1 AS
(
SELECT
upd_clm_doc_upd_ins.PRNT_DOC_ID as PRNT_DOC_ID,
upd_clm_doc_upd_ins.CLM_ID as CLM_ID,
''UNK'' as in_proof_of_loss,
upd_clm_doc_upd_ins.PRCS_ID as PRCS_ID,
upd_clm_doc_upd_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
upd_clm_doc_upd_ins.EDW_END_DTTM3 as EDW_END_DTTM3,
upd_clm_doc_upd_ins.in_TRNS_STRT_DTTM3 as in_TRNS_STRT_DTTM3,
upd_clm_doc_upd_ins.source_record_id
FROM
upd_clm_doc_upd_ins
);


-- Component tgt_CLM_DOC_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_DOC
USING exp_pass_to_tgt_upd ON (CLM_DOC.DOC_ID = exp_pass_to_tgt_upd.DOC_ID1 AND CLM_DOC.CLM_ID = exp_pass_to_tgt_upd.CLM_ID1 AND CLM_DOC.EDW_STRT_DTTM = exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
DOC_ID = exp_pass_to_tgt_upd.DOC_ID1,
CLM_ID = exp_pass_to_tgt_upd.CLM_ID1,
EDW_STRT_DTTM = exp_pass_to_tgt_upd.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_upd.EDW_END_DTTM_exp3,
TRANS_STRT_DTTM = exp_pass_to_tgt_upd.out_start_dt,
TRANS_END_DTTM = exp_pass_to_tgt_upd.in_TRNS_STRT_DTTM31;


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_clm_doc_ins.PRNT_DOC_ID as PRNT_DOC_ID,
upd_clm_doc_ins.CLM_ID as CLM_ID,
''UNK'' as in_proof_of_loss,
upd_clm_doc_ins.PRCS_ID as PRCS_ID,
upd_clm_doc_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_clm_doc_ins.in_TRNS_STRT_DTTM1 as in_TRNS_STRT_DTTM1,
CASE WHEN upd_clm_doc_ins.Retired1 = 0 THEN upd_clm_doc_ins.EDW_END_DTTM1 ELSE upd_clm_doc_ins.EDW_STRT_DTTM1 END as EDW_END_DTTM,
CASE WHEN upd_clm_doc_ins.Retired1 != 0 THEN upd_clm_doc_ins.in_TRNS_STRT_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd HH24:MI:SS.FF6'' ) END as o_TRANS_END_DTTM,
upd_clm_doc_ins.source_record_id
FROM
upd_clm_doc_ins
);


-- Component exp_pass_to_tgt_upd_retire, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_upd_retire AS
(
SELECT
upd_clm_doc_upd_retire.DOC_ID1 as DOC_ID1,
upd_clm_doc_upd_retire.CLM_ID1 as CLM_ID1,
upd_clm_doc_upd_retire.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as EDW_END_DTTM_exp3,
upd_clm_doc_upd_retire.in_TRNS_STRT_DTTM4 as in_TRNS_STRT_DTTM4,
upd_clm_doc_upd_retire.source_record_id
FROM
upd_clm_doc_upd_retire
);


-- Component tgt_CLM_DOC_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_DOC
(
DOC_ID,
CLM_ID,
PROOF_OF_LOSS_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_ins1.PRNT_DOC_ID as DOC_ID,
exp_pass_to_tgt_ins1.CLM_ID as CLM_ID,
exp_pass_to_tgt_ins1.in_proof_of_loss as PROOF_OF_LOSS_IND,
exp_pass_to_tgt_ins1.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins1.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins1.EDW_END_DTTM3 as EDW_END_DTTM,
exp_pass_to_tgt_ins1.in_TRNS_STRT_DTTM3 as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_ins1;


-- Component tgt_CLM_DOC_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_DOC
(
DOC_ID,
CLM_ID,
PROOF_OF_LOSS_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_ins.PRNT_DOC_ID as DOC_ID,
exp_pass_to_tgt_ins.CLM_ID as CLM_ID,
exp_pass_to_tgt_ins.in_proof_of_loss as PROOF_OF_LOSS_IND,
exp_pass_to_tgt_ins.PRCS_ID as PRCS_ID,
exp_pass_to_tgt_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_tgt_ins.in_TRNS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_to_tgt_ins.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component tgt_CLM_DOC_upd_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_DOC
USING exp_pass_to_tgt_upd_retire ON (CLM_DOC.DOC_ID = exp_pass_to_tgt_upd_retire.DOC_ID1 AND CLM_DOC.CLM_ID = exp_pass_to_tgt_upd_retire.CLM_ID1 AND CLM_DOC.EDW_STRT_DTTM = exp_pass_to_tgt_upd_retire.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
DOC_ID = exp_pass_to_tgt_upd_retire.DOC_ID1,
CLM_ID = exp_pass_to_tgt_upd_retire.CLM_ID1,
EDW_STRT_DTTM = exp_pass_to_tgt_upd_retire.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_upd_retire.EDW_END_DTTM_exp3,
TRANS_END_DTTM = exp_pass_to_tgt_upd_retire.in_TRNS_STRT_DTTM4;


END; ';