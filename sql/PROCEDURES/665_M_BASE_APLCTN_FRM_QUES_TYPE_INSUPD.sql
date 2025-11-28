-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_FRM_QUES_TYPE_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare		
    run_id varchar;
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


-- Component SQ_pc_questionsetlookup, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_questionsetlookup AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as StartEffectiveDate,
$2 as EndEffectiveDate,
$3 as UpdateTime,
$4 as Retired1,
$5 as out_QuestionSetCode,
$6 as CTGY_DOC_ID,
$7 as TGT_APLCTN_QUES_TYPE_CD,
$8 as TGT_DOC_ID,
$9 as TGT_EDW_STRT_DTTM,
$10 as TGT_EDW_END_DTTM,
$11 as CDC_check,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 
SQ1.StartEffectiveDate,
SQ1.EndEffectiveDate,
SQ1.UpdateTime,
SQ1.Retired1,
SQ1.out_QuestionSetCode,
SQ1.CTGY_DOC_ID,
SQ1.TGT_APLCTN_QUES_TYPE_CD,
SQ1.TGT_DOC_ID,
SQ1.TGT_EDW_STRT_DTTM,
SQ1.TGT_EDW_END_DTTM,
SQ1.CDC_check
from (
select 
LookupTableCode,
StartEffectiveDate,
UpdateTime,
SourceFile,
EndEffectiveDate,
Retired1,
/*LKP_XLAT_APLCTN_QUES_TYE_CD*/
CASE WHEN APLCTN_QUES_TYPE_CD.TGT_IDNTFTN_VAL is null then ''UNK'' ELSE APLCTN_QUES_TYPE_CD.TGT_IDNTFTN_VAL END as out_QuestionSetCode,
/*LKP_DOC_TYPE_CD*/
XLAT_DOC_TYPE.TGT_IDNTFTN_VAL as out_doc_type_cd,
/*LKP_DOC_CTGY_TYPE_CD*/
XLAT_DOC_CTGY_TYPE.TGT_IDNTFTN_VAL as out_doc_ctgy_type_cd,
/*LKP_DOC_CTGY_TYPE_CD*/
XLAT_DOC_ID.DOC_ID as CTGY_DOC_ID,

/*LKP_APLCTN_FRM_QUES_TYPE*/

TGT_APLCTN_FRM_QUES_TYPE.APLCTN_QUES_TYPE_CD as TGT_APLCTN_QUES_TYPE_CD,
TGT_APLCTN_FRM_QUES_TYPE.DOC_ID as TGT_DOC_ID,
TGT_APLCTN_FRM_QUES_TYPE.APLCTN_FRM_QUES_TYPE_STRT_DTTM as TGT_APLCTN_FRM_QUES_TYPE_STRT_DTTM,
TGT_APLCTN_FRM_QUES_TYPE.APLCTN_FRM_QUES_TYPE_END_DTTM as TGT_APLCTN_FRM_QUES_TYPE_END_DTTM,
TGT_APLCTN_FRM_QUES_TYPE.EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
TGT_APLCTN_FRM_QUES_TYPE.EDW_END_DTTM as TGT_EDW_END_DTTM,

/*SOURCE MD5*/
CAST(CONCAT(COALESCE(TRIM(CAST(StartEffectiveDate as varchar(100))),''''),
COALESCE(TRIM(CAST(EndEffectiveDate as varchar(100))),'''')) as VARCHAR(1000)) as SOURCEDATA,

/*TARGET MD5*/
CAST(CONCAT(COALESCE(TRIM(CAST(TGT_APLCTN_FRM_QUES_TYPE_STRT_DTTM as varchar(100))),''''),
COALESCE(TRIM(CAST(TGT_APLCTN_FRM_QUES_TYPE_END_DTTM as varchar(100))),'''')) as VARCHAR(1000)) as TARGETDATA,

/*FLAG*/
CASE WHEN LENGTH(TARGETDATA) =0 THEN ''I''
      WHEN TRIM(TARGETDATA) <> TRIM(SOURCEDATA) THEN ''U''
      ELSE ''R'' END AS CDC_check


from (
SELECT LookupTableCode_stg  as LookupTableCode
,  COALESCE(StartEffectiveDate_stg,CAST(:start_dttm as timestamp))  as StartEffectiveDate, 
UpdateTime_stg as UpdateTime
, SourceFile_stg as SourceFile
,  COALESCE(EndEffectiveDate_stg,CAST(:end_dttm as timestamp))  as EndEffectiveDate
, Retired_stg as Retired1
FROM
DB_T_PROD_STAG.pc_questionsetlookup 
WHERE
 pc_questionsetlookup.UpdateTime_stg > cast(:start_dttm as timestamp)
	AND pc_questionsetlookup.UpdateTime_stg <= cast(:end_dttm as timestamp))SQ

left outer join(SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT 
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''APLCTN_QUES_TYPE''
/*  AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' */
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''GW''
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')APLCTN_QUES_TYPE_CD 
		ON SQ.LookupTableCode=APLCTN_QUES_TYPE_CD.SRC_IDNTFTN_VAL
LEFT OUTER JOIN(SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_TYPE''
    		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_DOC_TYPE
			ON XLAT_DOC_TYPE.SRC_IDNTFTN_VAL=''DOC_TYPE5''
LEFT OUTER JOIN(SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DOC_CTGY_TYPE''
    		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_DOC_CTGY_TYPE
			ON XLAT_DOC_CTGY_TYPE.SRC_IDNTFTN_VAL=''DOC_CTGY_TYPE5''

LEFT OUTER JOIN(SELECT DOC.DOC_ID as DOC_ID,DOC.DOC_ISSUR_NUM as DOC_ISSUR_NUM, DOC.DOC_TYPE_CD as DOC_TYPE_CD, DOC.DOC_CTGY_TYPE_CD as DOC_CTGY_TYPE_CD 
FROM DB_T_PROD_CORE.DOC
QUALIFY ROW_NUMBER () OVER (PARTITION BY DOC_ISSUR_NUM,DOC_CTGY_TYPE_CD,DOC_TYPE_CD ORDER BY edw_end_dttm DESC)=1) XLAT_DOC_ID
ON SQ.SourceFile=XLAT_DOC_ID.DOC_ISSUR_NUM
AND out_doc_type_cd=XLAT_DOC_ID.DOC_TYPE_CD
and out_doc_ctgy_type_cd=XLAT_DOC_ID.DOC_CTGY_TYPE_CD

LEFT OUTER JOIN (SELECT APLCTN_FRM_QUES_TYPE.APLCTN_FRM_QUES_TYPE_STRT_DTTM as APLCTN_FRM_QUES_TYPE_STRT_DTTM,
APLCTN_FRM_QUES_TYPE.APLCTN_FRM_QUES_TYPE_END_DTTM as APLCTN_FRM_QUES_TYPE_END_DTTM,
APLCTN_FRM_QUES_TYPE.EDW_STRT_DTTM as EDW_STRT_DTTM, 
APLCTN_FRM_QUES_TYPE.EDW_END_DTTM as EDW_END_DTTM, 
APLCTN_FRM_QUES_TYPE.APLCTN_QUES_TYPE_CD as APLCTN_QUES_TYPE_CD, APLCTN_FRM_QUES_TYPE.DOC_ID as DOC_ID 
FROM DB_T_PROD_CORE.APLCTN_FRM_QUES_TYPE 
QUALIFY ROW_NUMBER() OVER(PARTITION BY APLCTN_QUES_TYPE_CD,DOC_ID ORDER BY EDW_END_DTTM desc) = 1) TGT_APLCTN_FRM_QUES_TYPE
ON out_QuestionSetCode=TGT_APLCTN_FRM_QUES_TYPE.APLCTN_QUES_TYPE_CD
and CTGY_DOC_ID=TGT_APLCTN_FRM_QUES_TYPE.DOC_ID)SQ1
) SRC
)
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
SQ_pc_questionsetlookup.StartEffectiveDate as StartEffectiveDate,
SQ_pc_questionsetlookup.EndEffectiveDate as EndEffectiveDate,
SQ_pc_questionsetlookup.UpdateTime as UpdateTime,
SQ_pc_questionsetlookup.Retired1 as Retired1,
SQ_pc_questionsetlookup.out_QuestionSetCode as out_QuestionSetCode,
SQ_pc_questionsetlookup.CTGY_DOC_ID as CTGY_DOC_ID,
SQ_pc_questionsetlookup.TGT_APLCTN_QUES_TYPE_CD as TGT_APLCTN_QUES_TYPE_CD,
SQ_pc_questionsetlookup.TGT_DOC_ID as TGT_DOC_ID,
SQ_pc_questionsetlookup.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
SQ_pc_questionsetlookup.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
SQ_pc_questionsetlookup.CDC_check as CDC_check,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
to_date ( ''1900/01/01'' , ''YYYY/MM/DD'' ) as TRANS_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''yyyy-mm-dd HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
SQ_pc_questionsetlookup.source_record_id
FROM
SQ_pc_questionsetlookup
);


-- Component rtr_aplctn_frm_ques_type_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_aplctn_frm_ques_type_INSERT AS
SELECT
exp_SrcFields.out_QuestionSetCode as in_APLCTN_QUES_TYPE_CD,
exp_SrcFields.CTGY_DOC_ID as in_DOC_ID,
exp_SrcFields.StartEffectiveDate as in_APLCTN_FRM_QUES_TYPE_STRT_DT,
exp_SrcFields.EndEffectiveDate as in_APLCTN_FRM_QUES_TYPE_END_DT,
exp_SrcFields.PRCS_ID as in_PRCS_ID,
exp_SrcFields.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.EDW_END_DTTM as in_EDW_END_DTTM,
exp_SrcFields.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_SrcFields.TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_SrcFields.TGT_APLCTN_QUES_TYPE_CD as lkp_APLCTN_QUES_TYPE_CD,
exp_SrcFields.TGT_DOC_ID as lkp_DOC_ID,
exp_SrcFields.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_SrcFields.CDC_check as o_CDC_Check,
exp_SrcFields.Retired1 as Retired,
exp_SrcFields.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields.UpdateTime as UpdateTime,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
WHERE exp_SrcFields.CDC_check = ''I'' OR ( exp_SrcFields.TGT_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_SrcFields.Retired1 = 0 );


-- Component rtr_aplctn_frm_ques_type_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE rtr_aplctn_frm_ques_type_RETIRED AS
SELECT
exp_SrcFields.out_QuestionSetCode as in_APLCTN_QUES_TYPE_CD,
exp_SrcFields.CTGY_DOC_ID as in_DOC_ID,
exp_SrcFields.StartEffectiveDate as in_APLCTN_FRM_QUES_TYPE_STRT_DT,
exp_SrcFields.EndEffectiveDate as in_APLCTN_FRM_QUES_TYPE_END_DT,
exp_SrcFields.PRCS_ID as in_PRCS_ID,
exp_SrcFields.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.EDW_END_DTTM as in_EDW_END_DTTM,
exp_SrcFields.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_SrcFields.TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_SrcFields.TGT_APLCTN_QUES_TYPE_CD as lkp_APLCTN_QUES_TYPE_CD,
exp_SrcFields.TGT_DOC_ID as lkp_DOC_ID,
exp_SrcFields.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_SrcFields.CDC_check as o_CDC_Check,
exp_SrcFields.Retired1 as Retired,
exp_SrcFields.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields.UpdateTime as UpdateTime,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
WHERE exp_SrcFields.CDC_check = ''R'' and exp_SrcFields.Retired1 != 0 and LKP_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component rtr_aplctn_frm_ques_type_UPDATE, Type ROUTER Output Group UPDATE
create or replace temporary table rtr_aplctn_frm_ques_type_UPDATE AS
SELECT
exp_SrcFields.out_QuestionSetCode as in_APLCTN_QUES_TYPE_CD,
exp_SrcFields.CTGY_DOC_ID as in_DOC_ID,
exp_SrcFields.StartEffectiveDate as in_APLCTN_FRM_QUES_TYPE_STRT_DT,
exp_SrcFields.EndEffectiveDate as in_APLCTN_FRM_QUES_TYPE_END_DT,
exp_SrcFields.PRCS_ID as in_PRCS_ID,
exp_SrcFields.EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.EDW_END_DTTM as in_EDW_END_DTTM,
exp_SrcFields.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_SrcFields.TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_SrcFields.TGT_APLCTN_QUES_TYPE_CD as lkp_APLCTN_QUES_TYPE_CD,
exp_SrcFields.TGT_DOC_ID as lkp_DOC_ID,
exp_SrcFields.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_SrcFields.CDC_check as o_CDC_Check,
exp_SrcFields.Retired1 as Retired,
exp_SrcFields.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields.UpdateTime as UpdateTime,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
WHERE exp_SrcFields.CDC_check = ''U'' AND exp_SrcFields.TGT_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component flt_retiree0, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_retiree0 AS
(
SELECT
rtr_aplctn_frm_ques_type_UPDATE.in_APLCTN_QUES_TYPE_CD as in_APLCTN_QUES_TYPE_CD3,
rtr_aplctn_frm_ques_type_UPDATE.in_DOC_ID as in_DOC_ID3,
rtr_aplctn_frm_ques_type_UPDATE.in_APLCTN_FRM_QUES_TYPE_STRT_DT as in_APLCTN_FRM_QUES_TYPE_STRT_DT3,
rtr_aplctn_frm_ques_type_UPDATE.in_APLCTN_FRM_QUES_TYPE_END_DT as in_APLCTN_FRM_QUES_TYPE_END_DT3,
rtr_aplctn_frm_ques_type_UPDATE.in_PRCS_ID as in_PRCS_ID3,
rtr_aplctn_frm_ques_type_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_aplctn_frm_ques_type_UPDATE.in_EDW_END_DTTM as in_EDW_END_DTTM3,
rtr_aplctn_frm_ques_type_UPDATE.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM3,
rtr_aplctn_frm_ques_type_UPDATE.in_TRANS_END_DTTM as in_TRANS_END_DTTM3,
rtr_aplctn_frm_ques_type_UPDATE.Retired as Retired3,
rtr_aplctn_frm_ques_type_UPDATE.UpdateTime as UpdateTime3,
rtr_aplctn_frm_ques_type_UPDATE.source_record_id
FROM
rtr_aplctn_frm_ques_type_UPDATE
WHERE rtr_aplctn_frm_ques_type_UPDATE.Retired = 0
);


-- Component upd_aplctn_frm_ques_type_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_aplctn_frm_ques_type_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_aplctn_frm_ques_type_UPDATE.lkp_APLCTN_QUES_TYPE_CD as lkp_APLCTN_QUES_TYPE_CD3,
rtr_aplctn_frm_ques_type_UPDATE.lkp_DOC_ID as lkp_DOC_ID3,
rtr_aplctn_frm_ques_type_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_aplctn_frm_ques_type_UPDATE.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_aplctn_frm_ques_type_UPDATE.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM3,
source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_aplctn_frm_ques_type_UPDATE
);


-- Component exp_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_update AS
(
SELECT
rtr_aplctn_frm_ques_type_INSERT.in_APLCTN_QUES_TYPE_CD as in_APLCTN_QUES_TYPE_CD1,
rtr_aplctn_frm_ques_type_INSERT.in_DOC_ID as in_DOC_ID1,
rtr_aplctn_frm_ques_type_INSERT.in_APLCTN_FRM_QUES_TYPE_STRT_DT as in_APLCTN_FRM_QUES_TYPE_STRT_DT1,
rtr_aplctn_frm_ques_type_INSERT.in_APLCTN_FRM_QUES_TYPE_END_DT as in_APLCTN_FRM_QUES_TYPE_END_DT1,
rtr_aplctn_frm_ques_type_INSERT.in_PRCS_ID as in_PRCS_ID1,
rtr_aplctn_frm_ques_type_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_aplctn_frm_ques_type_INSERT.UpdateTime as in_TRANS_STRT_DTTM1,
CASE WHEN rtr_aplctn_frm_ques_type_INSERT.Retired <> 0 THEN CURRENT_TIMESTAMP ELSE rtr_aplctn_frm_ques_type_INSERT.in_EDW_END_DTTM END as o_EDW_END_DTTM,
CASE WHEN rtr_aplctn_frm_ques_type_INSERT.Retired <> 0 THEN rtr_aplctn_frm_ques_type_INSERT.UpdateTime ELSE rtr_aplctn_frm_ques_type_INSERT.in_TRANS_END_DTTM END as o_TRANS_END_DTTM,
rtr_aplctn_frm_ques_type_INSERT.source_record_id
FROM
rtr_aplctn_frm_ques_type_INSERT
);


-- Component tgt_APLCTN_FRM_QUES_TYPE_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.APLCTN_FRM_QUES_TYPE
(
APLCTN_QUES_TYPE_CD,
DOC_ID,
APLCTN_FRM_QUES_TYPE_STRT_DTTM,
APLCTN_FRM_QUES_TYPE_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_update.in_APLCTN_QUES_TYPE_CD1 as APLCTN_QUES_TYPE_CD,
exp_update.in_DOC_ID1 as DOC_ID,
exp_update.in_APLCTN_FRM_QUES_TYPE_STRT_DT1 as APLCTN_FRM_QUES_TYPE_STRT_DTTM,
exp_update.in_APLCTN_FRM_QUES_TYPE_END_DT1 as APLCTN_FRM_QUES_TYPE_END_DTTM,
exp_update.in_PRCS_ID1 as PRCS_ID,
exp_update.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_update.o_EDW_END_DTTM as EDW_END_DTTM,
exp_update.in_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_update.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_update;


-- Component upd_aplctn_frm_ques_type_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_aplctn_frm_ques_type_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_aplctn_frm_ques_type_RETIRED.lkp_APLCTN_QUES_TYPE_CD as lkp_APLCTN_QUES_TYPE_CD3,
rtr_aplctn_frm_ques_type_RETIRED.lkp_DOC_ID as lkp_DOC_ID3,
rtr_aplctn_frm_ques_type_RETIRED.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_aplctn_frm_ques_type_RETIRED.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM4,
source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_aplctn_frm_ques_type_RETIRED
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
upd_aplctn_frm_ques_type_upd.lkp_APLCTN_QUES_TYPE_CD3 as lkp_APLCTN_QUES_TYPE_CD3,
upd_aplctn_frm_ques_type_upd.lkp_DOC_ID3 as lkp_DOC_ID3,
upd_aplctn_frm_ques_type_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
dateadd ( s,-1,upd_aplctn_frm_ques_type_upd.in_EDW_STRT_DTTM3  ) as o_EndDate,
dateadd ( s,-1,upd_aplctn_frm_ques_type_upd.in_TRANS_STRT_DTTM3 ) as o_TRANS_END_DTTM,
upd_aplctn_frm_ques_type_upd.source_record_id
FROM
upd_aplctn_frm_ques_type_upd
);


-- Component tgt_APLCTN_FRM_QUES_TYPE_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.APLCTN_FRM_QUES_TYPE
(
APLCTN_QUES_TYPE_CD,
DOC_ID,
APLCTN_FRM_QUES_TYPE_STRT_DTTM,
APLCTN_FRM_QUES_TYPE_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
flt_retiree0.in_APLCTN_QUES_TYPE_CD3 as APLCTN_QUES_TYPE_CD,
flt_retiree0.in_DOC_ID3 as DOC_ID,
flt_retiree0.in_APLCTN_FRM_QUES_TYPE_STRT_DT3 as APLCTN_FRM_QUES_TYPE_STRT_DTTM,
flt_retiree0.in_APLCTN_FRM_QUES_TYPE_END_DT3 as APLCTN_FRM_QUES_TYPE_END_DTTM,
flt_retiree0.in_PRCS_ID3 as PRCS_ID,
flt_retiree0.in_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
flt_retiree0.in_EDW_END_DTTM3 as EDW_END_DTTM,
flt_retiree0.UpdateTime3 as TRANS_STRT_DTTM,
flt_retiree0.in_TRANS_END_DTTM3 as TRANS_END_DTTM
FROM
flt_retiree0;


-- Component exp_pass_to_tgt_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_retired AS
(
SELECT
upd_aplctn_frm_ques_type_upd_retired.lkp_APLCTN_QUES_TYPE_CD3 as lkp_APLCTN_QUES_TYPE_CD3,
upd_aplctn_frm_ques_type_upd_retired.lkp_DOC_ID3 as lkp_DOC_ID3,
upd_aplctn_frm_ques_type_upd_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as o_EndDate,
upd_aplctn_frm_ques_type_upd_retired.in_TRANS_STRT_DTTM4 as in_TRANS_STRT_DTTM4,
upd_aplctn_frm_ques_type_upd_retired.source_record_id
FROM
upd_aplctn_frm_ques_type_upd_retired
);


-- Component tgt_APLCTN_FRM_QUES_TYPE_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.APLCTN_FRM_QUES_TYPE
USING exp_pass_to_tgt_retired ON (APLCTN_FRM_QUES_TYPE.APLCTN_QUES_TYPE_CD = exp_pass_to_tgt_retired.lkp_APLCTN_QUES_TYPE_CD3 AND APLCTN_FRM_QUES_TYPE.DOC_ID = exp_pass_to_tgt_retired.lkp_DOC_ID3 AND APLCTN_FRM_QUES_TYPE.EDW_STRT_DTTM = exp_pass_to_tgt_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
APLCTN_QUES_TYPE_CD = exp_pass_to_tgt_retired.lkp_APLCTN_QUES_TYPE_CD3,
DOC_ID = exp_pass_to_tgt_retired.lkp_DOC_ID3,
EDW_STRT_DTTM = exp_pass_to_tgt_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt_retired.o_EndDate,
TRANS_END_DTTM = exp_pass_to_tgt_retired.in_TRANS_STRT_DTTM4;


-- Component tgt_APLCTN_FRM_QUES_TYPE_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.APLCTN_FRM_QUES_TYPE
USING exp_pass_to_tgt ON (APLCTN_FRM_QUES_TYPE.APLCTN_QUES_TYPE_CD = exp_pass_to_tgt.lkp_APLCTN_QUES_TYPE_CD3 AND APLCTN_FRM_QUES_TYPE.DOC_ID = exp_pass_to_tgt.lkp_DOC_ID3 AND APLCTN_FRM_QUES_TYPE.EDW_STRT_DTTM = exp_pass_to_tgt.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
APLCTN_QUES_TYPE_CD = exp_pass_to_tgt.lkp_APLCTN_QUES_TYPE_CD3,
DOC_ID = exp_pass_to_tgt.lkp_DOC_ID3,
EDW_STRT_DTTM = exp_pass_to_tgt.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_tgt.o_EndDate,
TRANS_END_DTTM = exp_pass_to_tgt.o_TRANS_END_DTTM;


END; 
';