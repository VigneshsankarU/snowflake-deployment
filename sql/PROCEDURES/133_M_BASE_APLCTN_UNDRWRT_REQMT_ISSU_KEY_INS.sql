-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_APLCTN_UNDRWRT_REQMT_ISSU_KEY_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
	   v_start_time TIMESTAMP;
BEGIN
      run_id := (SELECT run_id FROM control_worklet WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
   v_start_time := CURRENT_TIMESTAMP();

-- Component SQ_pc_uwissuehistory, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_uwissuehistory AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as IssueKey,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT distinct pc_uwissuehistory.IssueKey_stg  COLLATE ''en-ci''  as IssueKey

FROM

 DB_T_PROD_STAG.pc_uwissuehistory

WHERE pc_uwissuehistory.UpdateTime_stg > (:start_dttm)

and pc_uwissuehistory.UpdateTime_stg <= (:end_dttm)
) SRC
)
);


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
SQ_pc_uwissuehistory.IssueKey as IssueKey,
SQ_pc_uwissuehistory.source_record_id
FROM
SQ_pc_uwissuehistory
);


-- Component LKP_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TGT AS
(
SELECT
LKP.HOST_ISSU_KEY,
exp.IssueKey as IssueKey,
exp.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp.source_record_id ORDER BY LKP.HOST_ISSU_KEY asc) RNK
FROM
exp
LEFT JOIN (
SELECT distinct host_issu_key COLLATE ''en-ci''  as host_issu_key FROM db_t_prod_core.APLCTN_UNDRWRT_REQMT_ISSU_KEY
) LKP ON LKP.HOST_ISSU_KEY = exp.IssueKey
QUALIFY RNK = 1
);


-- Component exp_chck, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_chck AS
(
SELECT
LKP_TGT.IssueKey as IssueKey,
CURRENT_TIMESTAMP as date,
CASE WHEN LKP_TGT.HOST_ISSU_KEY IS NULL THEN ''I'' ELSE ''R'' END as cdc_chck,
LKP_TGT.source_record_id
FROM
LKP_TGT
);


-- Component fil, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil AS
(
SELECT
exp_chck.IssueKey as IssueKey,
exp_chck.date as date,
exp_chck.cdc_chck as cdc_chck,
exp_chck.source_record_id
FROM
exp_chck
WHERE exp_chck.cdc_chck = ''I''
);


-- Component APLCTN_UNDRWRT_REQMT_ISSU_KEY, Type TARGET 
INSERT INTO DB_T_PROD_CORE.APLCTN_UNDRWRT_REQMT_ISSU_KEY
(
APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
HOST_ISSU_KEY,
EDW_STRT_DTTM
)
SELECT
row_number() over (order by 1) as APLCTN_UNDRWRT_REQ_ISSU_KEY_ID,
fil.IssueKey as HOST_ISSU_KEY,
fil.date as EDW_STRT_DTTM
FROM
fil;


END; ';