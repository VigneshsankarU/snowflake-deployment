-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_TRANS_LN_ITEM_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       start_dttm STRING;
       end_dttm STRING;
       PRCS_ID STRING;
	   v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
	   v_start_time := CURRENT_TIMESTAMP();

-- Component LKP_TERADATA_ETL_REF_XLAT_LINECTGYTYPECD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_LINECTGYTYPECD AS
(
SELECT 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

 ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

 DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CTGY_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_linecategory.typecode'' 

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


-- Component LKP_XREF_CLM, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_CLM AS
(
SELECT
CLM_ID,
NK_SRC_KEY,
DIR_CLM_VAL
FROM DB_T_PROD_CORE.DIR_CLM
);


-- Component sq_cc_transactionlineitem, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_transactionlineitem AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Transactionlineitemid,
$2 as TransactionID,
$3 as CreateTime,
$4 as Comments,
$5 as TransactionAmount,
$6 as LineCategorycd,
$7 as AccountingDay_alfa,
$8 as AccountingMonth_alfa,
$9 as AccountingYear_alfa,
$10 as SRC_CD,
$11 as IssueDate,
$12 as ScheduledSendDate,
$13 as Retired,
$14 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT

a.id_stg AS transactionlineitemid,

a.TransactionID_stg,

a.CreateTime_stg,

a.Comments_stg,

a.transactionamount_stg,

g.typecode_stg AS linectgytypecd,

CAST(EXTRACT(DAY FROM a.AccountingDate_stg) AS VARCHAR(50)) as AccountingDay_alfa,

CAST(EXTRACT(MONTH FROM a.AccountingDate_stg)AS VARCHAR(50)) as AccountingMonth_alfa,

CAST(EXTRACT(YEAR FROM a.AccountingDate_stg)AS VARCHAR(50)) as AccountingYear_alfa,

 ''SRC_SYS6'' AS SRC_CD,

a.IssueDate_stg,

a.ScheduledSendDate_stg,

a.retired_stg AS Retired

 FROM 

 (SELECT  cc_transactionlineitem.ID_stg,

cc_transactionlineitem.TransactionID_stg ,

cc_transactionlineitem.CreateTime_stg,

cc_transactionlineitem.Comments_stg,

cc_transactionlineitem.TransactionAmount_stg,

cc_transactionlineitem.AccountingDay_alfa_stg,

cc_transactionlineitem.AccountingMonth_alfa_stg,

cc_transactionlineitem.AccountingYear_alfa_stg,

cc_check.IssueDate_stg,

cc_check.ScheduledSendDate_stg,

cc_transactionlineitem.Retired_stg,linecategory_stg,

cc_transaction.PublicID_stg,

cc.AccountingDate_stg,

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

when cctl_transactionstatus.TYPECODE_stg = ''recoded'' and cc.payload_new_stg= ''transferred_14'' then ''N''/* EIM-41121 */
else ''Y'' end as eligible

FROM

DB_T_PROD_STAG.cc_transactionlineitem inner join DB_T_PROD_STAG.cc_transaction 

on cc_transactionlineitem.TransactionID_stg =cc_transaction.id_stg 

inner join DB_T_PROD_STAG.cctl_transactionstatus 

on cctl_transactionstatus.ID_stg = cc_transaction.Status_stg 

left outer join DB_T_PROD_STAG.cc_check on cc_check.id_stg = cc_transaction.CheckID_stg

inner join (select cc_claim.* from DB_T_PROD_STAG.cc_claim 

inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

where cctl_claimstate.name_stg <> ''Draft'') cc_claim on cc_claim.id_stg=cc_transaction.claimid_stg

inner join DB_T_PROD_STAG.cc_exposure on cc_transaction.ExposureID_stg=cc_exposure.id_stg 

LEFT OUTER JOIN DB_T_PROD_STAG.GL_EVENTSTAGING_CC CC ON cc_transaction.PublicID_stg=CC.Publicid_stg

where cc_transactionlineitem.UpdateTime_stg > (:start_dttm)

and cc_transactionlineitem.UpdateTime_stg <= (:end_dttm)

) a

LEFT OUTER JOIN  DB_T_PROD_STAG.cctl_linecategory g ON a.linecategory_stg = g.id_stg

where eligible=''Y''
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
sq_cc_transactionlineitem.Transactionlineitemid as Transactionlineitemid,
sq_cc_transactionlineitem.TransactionID as TransactionID,
sq_cc_transactionlineitem.CreateTime as CreateTime,
sq_cc_transactionlineitem.Comments as Comments,
sq_cc_transactionlineitem.TransactionAmount as TransactionAmount,
sq_cc_transactionlineitem.LineCategorycd as LineCategorycd,
sq_cc_transactionlineitem.AccountingDay_alfa as AccountingDay_alfa,
sq_cc_transactionlineitem.AccountingMonth_alfa as AccountingMonth_alfa,
sq_cc_transactionlineitem.AccountingYear_alfa as AccountingYear_alfa,
sq_cc_transactionlineitem.SRC_CD as SRC_CD,
CASE WHEN sq_cc_transactionlineitem.CreateTime IS NULL THEN to_date ( ''1900/01/01'' , ''YYYY/MM/DD'' ) ELSE sq_cc_transactionlineitem.CreateTime END as o_CreateTime,
sq_cc_transactionlineitem.Retired as Retired,
sq_cc_transactionlineitem.source_record_id
FROM
sq_cc_transactionlineitem
);


-- Component LKP_CLM_EXPSR_TRANS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS AS
(
SELECT
LKP.CLM_EXPSR_TRANS_ID,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID desc,LKP.CLM_EXPSR_TRANS_SBTYPE_CD desc,LKP.CLM_EXPSR_ID desc,LKP.EXPSR_COST_TYPE_CD desc,LKP.EXPSR_COST_CTGY_TYPE_CD desc,LKP.PMT_TYPE_CD desc,LKP.CLM_EXPSR_TRANS_DTTM desc,LKP.CLM_EXPSR_TRANS_TXT desc,LKP.RCVRY_CTGY_TYPE_CD desc,LKP.DOES_NOT_ERODE_RSERV_IND desc,LKP.CRTD_BY_PRTY_ID desc,LKP.NK_CLM_EXPSR_TRANS_ID desc,LKP.GL_MTH_NUM desc,LKP.GL_YR_NUM desc,LKP.TRTY_CD desc,LKP.PRCS_ID desc,LKP.CLM_EXPSR_TRANS_STRT_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source
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
) LKP ON LKP.NK_CLM_EXPSR_TRANS_ID = exp_all_source.TransactionID
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_all_source.Transactionlineitemid as NK_LN_ITEM_ID,
exp_all_source.CreateTime as CLM_EXPSR_LNITM_DTTM,
exp_all_source.Comments as CLM_EXPSR_LNITM_TXT,
exp_all_source.TransactionAmount as CLM_EXPSR_LNITM_AMT,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LINECTGYTYPECD */ as LNITM_CTGY_TYPE_CD,
:PRCS_ID as out_PRCS_ID,
exp_all_source.AccountingDay_alfa as AccountingDay_alfa,
exp_all_source.AccountingMonth_alfa as AccountingMonth_alfa,
exp_all_source.AccountingYear_alfa as AccountingYear_alfa,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_SRC_CD,
exp_all_source.o_CreateTime as TRANS_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM1,
exp_all_source.Retired as Retired,
exp_all_source.source_record_id,
row_number() over (partition by exp_all_source.source_record_id order by exp_all_source.source_record_id) as RNK
FROM
exp_all_source
INNER JOIN LKP_CLM_EXPSR_TRANS ON exp_all_source.source_record_id = LKP_CLM_EXPSR_TRANS.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_LINECTGYTYPECD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_all_source.LineCategorycd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_all_source.SRC_CD
QUALIFY RNK = 1
);


-- Component rtr_filter_invalid_record_Valid, Type ROUTER Output Group Valid
CREATE OR REPLACE TEMPORARY TABLE rtr_filter_invalid_record_Valid AS (
SELECT
exp_data_transformation.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_data_transformation.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD,
exp_data_transformation.NK_LN_ITEM_ID as NK_LN_ITEM_ID,
exp_data_transformation.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM,
exp_data_transformation.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT,
exp_data_transformation.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
exp_data_transformation.AccountingDay_alfa as AccountingDay_alfa,
exp_data_transformation.AccountingMonth_alfa as AccountingMonth_alfa,
exp_data_transformation.AccountingYear_alfa as AccountingYear_alfa,
exp_data_transformation.o_SRC_CD as o_SRC_CD,
exp_data_transformation.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_data_transformation.TRANS_END_DTTM1 as TRANS_END_DTTM1,
exp_data_transformation.Retired as Retired,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE CASE WHEN exp_data_transformation.CLM_EXPSR_TRANS_ID IS NOT NULL THEN TRUE ELSE FALSE END
);


-- Component LKP_CLM_EXPSR_TRANS_LN_ITEM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS_LN_ITEM AS
(
SELECT
LKP.CLM_EXPSR_TRANS_LNITM_ID,
LKP.CLM_EXPSR_TRANS_ID,
LKP.LNITM_CTGY_TYPE_CD,
LKP.CLM_EXPSR_LNITM_AMT,
LKP.CLM_EXPSR_LNITM_DTTM,
LKP.CLM_EXPSR_LNITM_TXT,
LKP.ACCNTG_DY_NUM,
LKP.ACCNTG_MTH_NUM,
LKP.ACCNTG_YR_NUM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
rtr_filter_invalid_record_Valid.source_record_id,
ROW_NUMBER() OVER(PARTITION BY rtr_filter_invalid_record_Valid.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_LNITM_ID asc,LKP.CLM_EXPSR_TRANS_ID asc,LKP.LNITM_CTGY_TYPE_CD asc,LKP.CLM_EXPSR_LNITM_AMT asc,LKP.CLM_EXPSR_LNITM_DTTM asc,LKP.CLM_EXPSR_LNITM_TXT asc,LKP.NK_LN_ITEM_ID asc,LKP.ACCNTG_DY_NUM asc,LKP.ACCNTG_MTH_NUM asc,LKP.ACCNTG_YR_NUM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
rtr_filter_invalid_record_Valid
LEFT JOIN (
SELECT CLM_EXPSR_TRANS_LNITM.CLM_EXPSR_TRANS_LNITM_ID as CLM_EXPSR_TRANS_LNITM_ID, 
CLM_EXPSR_TRANS_LNITM.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID, 
CLM_EXPSR_TRANS_LNITM.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD, 
CLM_EXPSR_TRANS_LNITM.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT, 
CLM_EXPSR_TRANS_LNITM.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM, 
CLM_EXPSR_TRANS_LNITM.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT, 
CLM_EXPSR_TRANS_LNITM.ACCNTG_DY_NUM as ACCNTG_DY_NUM, 
CLM_EXPSR_TRANS_LNITM.ACCNTG_MTH_NUM as ACCNTG_MTH_NUM, 
CLM_EXPSR_TRANS_LNITM.ACCNTG_YR_NUM as ACCNTG_YR_NUM, 
CLM_EXPSR_TRANS_LNITM.EDW_STRT_DTTM as EDW_STRT_DTTM, 
CLM_EXPSR_TRANS_LNITM.EDW_END_DTTM as EDW_END_DTTM, 
CLM_EXPSR_TRANS_LNITM.NK_LN_ITEM_ID as NK_LN_ITEM_ID FROM DB_T_PROD_CORE.CLM_EXPSR_TRANS_LNITM
QUALIFY ROW_NUMBER() OVER(PARTITION BY  CLM_EXPSR_TRANS_LNITM.NK_LN_ITEM_ID  ORDER BY CLM_EXPSR_TRANS_LNITM.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_LN_ITEM_ID = rtr_filter_invalid_record_Valid.NK_LN_ITEM_ID
QUALIFY RNK = 1
);


-- Component exp_set_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_set_flag AS
(
SELECT
LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
LKP_CLM_EXPSR_TRANS_LN_ITEM.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
rtr_filter_invalid_record_Valid.TRANS_STRT_DTTM as in_CLM_EXPSR_TRANS_LNITM_STRT_DT,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
dateadd (second, -1, CURRENT_TIMESTAMP  ) as EDW_END_DTTM_exp,
MD5 ( ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.LNITM_CTGY_TYPE_CD ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_LNITM_AMT ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_LNITM_DTTM ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_LNITM_TXT ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.ACCNTG_DY_NUM ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.ACCNTG_MTH_NUM ) ) || ltrim ( rtrim ( LKP_CLM_EXPSR_TRANS_LN_ITEM.ACCNTG_YR_NUM ) ) ) as chksum_lkp,
MD5 ( ltrim ( rtrim ( rtr_filter_invalid_record_Valid.LNITM_CTGY_TYPE_CD ) ) || ltrim ( rtrim ( rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_AMT ) ) || ltrim ( rtrim ( rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_DTTM ) ) || ltrim ( rtrim ( rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_TXT ) ) || ltrim ( rtrim ( rtr_filter_invalid_record_Valid.AccountingDay_alfa ) ) || ltrim ( rtrim ( rtr_filter_invalid_record_Valid.AccountingMonth_alfa ) ) || ltrim ( rtrim ( rtr_filter_invalid_record_Valid.AccountingYear_alfa ) ) ) as chksum_inp,
CASE WHEN chksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN chksum_lkp != chksum_inp THEN ''U'' ELSE ''R'' END END as o_flag,
rtr_filter_invalid_record_Valid.o_SRC_CD as o_SRC_CD1,
rtr_filter_invalid_record_Valid.TRANS_END_DTTM1 as TRANS_END_DTTM11,
rtr_filter_invalid_record_Valid.Retired as Retired1,
LKP_CLM_EXPSR_TRANS_LN_ITEM.EDW_END_DTTM as lkp_EDW_END_DTTM,
rtr_filter_invalid_record_Valid.source_record_id
FROM
rtr_filter_invalid_record_Valid
INNER JOIN LKP_CLM_EXPSR_TRANS_LN_ITEM ON rtr_filter_invalid_record_Valid.source_record_id = LKP_CLM_EXPSR_TRANS_LN_ITEM.source_record_id
);


-- Component rtr_clm_expsr_trans_ln_item_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_trans_ln_item_INSERT AS (
SELECT
LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_TRANS_LNITM_ID as lkp_CLM_EXPSR_TRANS_LN_ITEM_ID,
rtr_filter_invalid_record_Valid.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_filter_invalid_record_Valid.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD,
rtr_filter_invalid_record_Valid.NK_LN_ITEM_ID as NK_LN_ITEM_ID,
rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM,
rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT,
rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT,
rtr_filter_invalid_record_Valid.PRCS_ID as PRCS_ID,
rtr_filter_invalid_record_Valid.AccountingDay_alfa as AccountingDay_alfa1,
rtr_filter_invalid_record_Valid.AccountingMonth_alfa as AccountingMonth_alfa1,
rtr_filter_invalid_record_Valid.AccountingYear_alfa as AccountingYear_alfa1,
exp_set_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_set_flag.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_set_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_set_flag.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_set_flag.o_flag as o_flag,
exp_set_flag.o_SRC_CD1 as o_SRC_CD1,
exp_set_flag.in_CLM_EXPSR_TRANS_LNITM_STRT_DT as TRANS_STRT_DTTM1,
exp_set_flag.TRANS_END_DTTM11 as TRANS_END_DTTM11,
exp_set_flag.Retired1 as Retired1,
exp_set_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_set_flag.lkp_CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
rtr_filter_invalid_record_Valid.source_record_id
FROM
rtr_filter_invalid_record_Valid
LEFT JOIN LKP_CLM_EXPSR_TRANS_LN_ITEM ON rtr_filter_invalid_record_Valid.source_record_id = LKP_CLM_EXPSR_TRANS_LN_ITEM.source_record_id
LEFT JOIN exp_set_flag ON LKP_CLM_EXPSR_TRANS_LN_ITEM.source_record_id = exp_set_flag.source_record_id
WHERE ( exp_set_flag.o_flag = ''I'' ) OR ( exp_set_flag.Retired1 = 0 AND exp_set_flag.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) or ( exp_set_flag.o_flag = ''U'' AND exp_set_flag.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) -- CASE WHEN LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_TRANS_LNITM_ID IS NULL THEN -- TRUE ELSE FALSE END
);


-- Component rtr_clm_expsr_trans_ln_item_Retired, Type ROUTER Output Group Retired
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_trans_ln_item_Retired AS (
SELECT
LKP_CLM_EXPSR_TRANS_LN_ITEM.CLM_EXPSR_TRANS_LNITM_ID as lkp_CLM_EXPSR_TRANS_LN_ITEM_ID,
rtr_filter_invalid_record_Valid.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_filter_invalid_record_Valid.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD,
rtr_filter_invalid_record_Valid.NK_LN_ITEM_ID as NK_LN_ITEM_ID,
rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM,
rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT,
rtr_filter_invalid_record_Valid.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT,
rtr_filter_invalid_record_Valid.PRCS_ID as PRCS_ID,
rtr_filter_invalid_record_Valid.AccountingDay_alfa as AccountingDay_alfa1,
rtr_filter_invalid_record_Valid.AccountingMonth_alfa as AccountingMonth_alfa1,
rtr_filter_invalid_record_Valid.AccountingYear_alfa as AccountingYear_alfa1,
exp_set_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_set_flag.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_set_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_set_flag.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_set_flag.o_flag as o_flag,
exp_set_flag.o_SRC_CD1 as o_SRC_CD1,
exp_set_flag.in_CLM_EXPSR_TRANS_LNITM_STRT_DT as TRANS_STRT_DTTM1,
exp_set_flag.TRANS_END_DTTM11 as TRANS_END_DTTM11,
exp_set_flag.Retired1 as Retired1,
exp_set_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_set_flag.lkp_CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
rtr_filter_invalid_record_Valid.source_record_id
FROM
rtr_filter_invalid_record_Valid
LEFT JOIN LKP_CLM_EXPSR_TRANS_LN_ITEM ON rtr_filter_invalid_record_Valid.source_record_id = LKP_CLM_EXPSR_TRANS_LN_ITEM.source_record_id
LEFT JOIN exp_set_flag ON LKP_CLM_EXPSR_TRANS_LN_ITEM.source_record_id = exp_set_flag.source_record_id
WHERE exp_set_flag.o_flag = ''R'' and exp_set_flag.Retired1 != 0 and exp_set_flag.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component updstr_clm_expsr_trans_ln_item_Retire_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_trans_ln_item_Retire_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_ln_item_Retired.lkp_CLM_EXPSR_TRANS_LN_ITEM_ID as CLM_EXPSR_TRANS_LN_ITEM_ID,
rtr_clm_expsr_trans_ln_item_Retired.lkp_CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_ln_item_Retired.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_clm_expsr_trans_ln_item_Retired.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_clm_expsr_trans_ln_item_Retired
);


-- Component updstr_clm_expsr_trans_ln_item_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_clm_expsr_trans_ln_item_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_ln_item_INSERT.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_ln_item_INSERT.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD,
rtr_clm_expsr_trans_ln_item_INSERT.NK_LN_ITEM_ID as NK_LN_ITEM_ID,
rtr_clm_expsr_trans_ln_item_INSERT.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM,
rtr_clm_expsr_trans_ln_item_INSERT.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT,
rtr_clm_expsr_trans_ln_item_INSERT.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT,
rtr_clm_expsr_trans_ln_item_INSERT.PRCS_ID as PRCS_ID,
rtr_clm_expsr_trans_ln_item_INSERT.AccountingDay_alfa1 as ACCNTG_DY_NUM,
rtr_clm_expsr_trans_ln_item_INSERT.AccountingMonth_alfa1 as ACCNTG_MTH_NUM,
rtr_clm_expsr_trans_ln_item_INSERT.AccountingYear_alfa1 as ACCNTG_YR_NUM,
rtr_clm_expsr_trans_ln_item_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_clm_expsr_trans_ln_item_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
rtr_clm_expsr_trans_ln_item_INSERT.o_SRC_CD1 as o_SRC_CD11,
rtr_clm_expsr_trans_ln_item_INSERT.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM11,
rtr_clm_expsr_trans_ln_item_INSERT.Retired1 as Retired11,
rtr_clm_expsr_trans_ln_item_INSERT.source_record_id,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_clm_expsr_trans_ln_item_INSERT
);


-- Component exp_pass_to_target_upd_Retire_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_Retire_rejected AS
(
SELECT
updstr_clm_expsr_trans_ln_item_Retire_rejected.CLM_EXPSR_TRANS_LN_ITEM_ID as CLM_EXPSR_TRANS_LNITM_ID,
updstr_clm_expsr_trans_ln_item_Retire_rejected.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_ln_item_Retire_rejected.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
updstr_clm_expsr_trans_ln_item_Retire_rejected.source_record_id
FROM
updstr_clm_expsr_trans_ln_item_Retire_rejected
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
updstr_clm_expsr_trans_ln_item_ins.NK_LN_ITEM_ID as NK_LN_ITEM_ID,
LKP_1.CLM_ID /* replaced lookup LKP_XREF_CLM */ as CLM_EXPSR_TRANS_LN_ITEM_ID,
updstr_clm_expsr_trans_ln_item_ins.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
updstr_clm_expsr_trans_ln_item_ins.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD,
updstr_clm_expsr_trans_ln_item_ins.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM,
updstr_clm_expsr_trans_ln_item_ins.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT,
updstr_clm_expsr_trans_ln_item_ins.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT,
updstr_clm_expsr_trans_ln_item_ins.PRCS_ID as PRCS_ID,
updstr_clm_expsr_trans_ln_item_ins.ACCNTG_DY_NUM as ACCNTG_DY_NUM,
updstr_clm_expsr_trans_ln_item_ins.ACCNTG_MTH_NUM as ACCNTG_MTH_NUM,
updstr_clm_expsr_trans_ln_item_ins.ACCNTG_YR_NUM as ACCNTG_YR_NUM,
updstr_clm_expsr_trans_ln_item_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN updstr_clm_expsr_trans_ln_item_ins.Retired11 != 0 THEN updstr_clm_expsr_trans_ln_item_ins.EDW_STRT_DTTM1 ELSE updstr_clm_expsr_trans_ln_item_ins.EDW_END_DTTM1 END as o_EDW_END_DTTM,
updstr_clm_expsr_trans_ln_item_ins.o_SRC_CD11 as o_SRC_CD11,
updstr_clm_expsr_trans_ln_item_ins.TRANS_STRT_DTTM11 as CLM_EXPSR_TRANS_LNITM_STRT_DTTM11,
updstr_clm_expsr_trans_ln_item_ins.source_record_id,
row_number() over (partition by updstr_clm_expsr_trans_ln_item_ins.source_record_id order by updstr_clm_expsr_trans_ln_item_ins.source_record_id) as RNK
FROM
updstr_clm_expsr_trans_ln_item_ins
LEFT JOIN LKP_XREF_CLM LKP_1 ON LKP_1.NK_SRC_KEY = RTRIM ( LTRIM ( updstr_clm_expsr_trans_ln_item_ins.NK_LN_ITEM_ID ) ) AND LKP_1.DIR_CLM_VAL = ''CLMEXPSRTRANSLNITM''
QUALIFY RNK = 1
);


-- Component tgt_clm_expsr_trans_lnitem_upd_Retire_rejected, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS_LNITM
USING exp_pass_to_target_upd_Retire_rejected ON (CLM_EXPSR_TRANS_LNITM.CLM_EXPSR_TRANS_LNITM_ID = exp_pass_to_target_upd_Retire_rejected.CLM_EXPSR_TRANS_LNITM_ID AND CLM_EXPSR_TRANS_LNITM.EDW_STRT_DTTM = exp_pass_to_target_upd_Retire_rejected.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_EXPSR_TRANS_LNITM_ID = exp_pass_to_target_upd_Retire_rejected.CLM_EXPSR_TRANS_LNITM_ID,
CLM_EXPSR_TRANS_ID = exp_pass_to_target_upd_Retire_rejected.CLM_EXPSR_TRANS_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd_Retire_rejected.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_Retire_rejected.o_EDW_END_DTTM;


-- Component tgt_clm_expsr_trans_lnitem_upd_Retire_rejected, Type Post SQL 
UPDATE db_t_prod_core.CLM_EXPSR_TRANS_LNITM
SET EDW_END_DTTM = A.lead1
FROM

(

SELECT distinct NK_LN_ITEM_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over(partition by NK_LN_ITEM_ID ORDER BY

EDW_STRT_DTTM ASC rows between 1 following

and 1 following) - INTERVAL ''1 SECOND'' as lead1

FROM DB_T_PROD_CORE.CLM_EXPSR_TRANS_LNITM) A

WHERE

CLM_EXPSR_TRANS_LNITM.EDW_STRT_DTTM = A.EDW_STRT_DTTM

AND CLM_EXPSR_TRANS_LNITM.NK_LN_ITEM_ID = A.NK_LN_ITEM_ID

AND CAST(CLM_EXPSR_TRANS_LNITM.EDW_END_DTTM AS DATE)=''9999-12-31''

AND A.lead1 is not null;


-- Component tgt_clm_expsr_trans_lnitem_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS_LNITM
(
CLM_EXPSR_TRANS_LNITM_ID,
CLM_EXPSR_TRANS_ID,
LNITM_CTGY_TYPE_CD,
CLM_EXPSR_LNITM_AMT,
CLM_EXPSR_LNITM_DTTM,
CLM_EXPSR_LNITM_TXT,
NK_LN_ITEM_ID,
PRCS_ID,
ACCNTG_DY_NUM,
ACCNTG_MTH_NUM,
ACCNTG_YR_NUM,
CLM_EXPSR_TRANS_LNITM_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
SRC_SYS_CD,
CLM_EXPSR_TRANS_LNITM_END_DTTM 
)
SELECT
exp_pass_to_target_ins.CLM_EXPSR_TRANS_LN_ITEM_ID as CLM_EXPSR_TRANS_LNITM_ID,
exp_pass_to_target_ins.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_pass_to_target_ins.LNITM_CTGY_TYPE_CD as LNITM_CTGY_TYPE_CD,
exp_pass_to_target_ins.CLM_EXPSR_LNITM_AMT as CLM_EXPSR_LNITM_AMT,
exp_pass_to_target_ins.CLM_EXPSR_LNITM_DTTM as CLM_EXPSR_LNITM_DTTM,
exp_pass_to_target_ins.CLM_EXPSR_LNITM_TXT as CLM_EXPSR_LNITM_TXT,
exp_pass_to_target_ins.NK_LN_ITEM_ID as NK_LN_ITEM_ID,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.ACCNTG_DY_NUM as ACCNTG_DY_NUM,
exp_pass_to_target_ins.ACCNTG_MTH_NUM as ACCNTG_MTH_NUM,
exp_pass_to_target_ins.ACCNTG_YR_NUM as ACCNTG_YR_NUM,
exp_pass_to_target_ins.CLM_EXPSR_TRANS_LNITM_STRT_DTTM11 as CLM_EXPSR_TRANS_LNITM_STRT_DTTM,
exp_pass_to_target_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.o_SRC_CD11 as SRC_SYS_CD,
''9999-01-01''
FROM
exp_pass_to_target_ins;


END; ';