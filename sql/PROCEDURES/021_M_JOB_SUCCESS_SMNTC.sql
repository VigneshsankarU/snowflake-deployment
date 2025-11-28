-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_JOB_SUCCESS_SMNTC("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  PRCS_ID STRING;
  SUB_PRCS_NM STRING;
  SRC_CNT STRING;
  TGT_CNT STRING;
  SRC_EXTRACT_DT STRING;
  SRC_NM STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):PRCS_ID::STRING,
    TRY_PARSE_JSON(:param_json):SUB_PRCS_NM::STRING,
    TRY_PARSE_JSON(:param_json):SRC_CNT::STRING,
    TRY_PARSE_JSON(:param_json):TGT_CNT::STRING,
    TRY_PARSE_JSON(:param_json):SRC_EXTRACT_DT::STRING,
    TRY_PARSE_JSON(:param_json):SRC_NM::STRING
  INTO
    PRCS_ID,
    SUB_PRCS_NM,
    SRC_CNT,
    TGT_CNT,
    SRC_EXTRACT_DT,
    SRC_NM;

-- Component src_edw_gen_seq, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_edw_gen_seq AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRCS_STAG,
$2 as PRCS_STS,
$3 as JOB_STRT_DT,
$4 as JOB_END_DT,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

''SMNTC'',''SUCCEEDED'',

CASE WHEN STRT_DT> STRT_DT1 THEN STRT_DT1 ELSE STRT_DT END AS STRT_DT_FINAL ,END_DT FROM

(

SELECT MIN (CAST(JOB_START_DT AS DATE)) STRT_DT



FROM

(

SELECT * FROM DB_T_PROD_CORE.ETL_PRCS_CTRL   A 

WHERE  

PRCS_STS=''SUCCEEDED''

AND PRCS_NM=''WF_BASE_MASTER_DAILY''

QUALIFY RANK() OVER (PARTITION BY PRCS_NM,SUB_PRCS_NM ORDER BY  JOB_START_DT DESC ) = 1

) A

) A,

(



SELECT MAX (CAST(SRC_EXTRACT_DT AS DATE)) END_DT



FROM

(

SELECT * FROM DB_T_PROD_CORE.ETL_PRCS_CTRL   A 

WHERE  

PRCS_STS=''SUCCEEDED''

AND PRCS_NM=''WF_BASE_EDW_LOAD_COMPLETION_DAILY''

QUALIFY RANK() OVER (PARTITION BY PRCS_NM,SUB_PRCS_NM ORDER BY  SRC_EXTRACT_DT DESC ) = 1

) A

)B

,

(



SELECT MAX (CAST(JOB_END_DT AS DATE)) STRT_DT1



FROM

(

SELECT * FROM DB_T_PROD_CORE.ETL_PRCS_CTRL   A 

WHERE  

PRCS_STS=''SUCCEEDED''

AND PRCS_NM=''WF_SMNT_LOAD_COMPLETION_DAILY''



) A

) C
) SRC
)
);


-- Component exp_prcs_id_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_id_upd AS
(
SELECT
:PRCS_ID as PRCS_ID,
:SUB_PRCS_NM as SUB_PRCS_NM,
:SRC_CNT as SRC_CNT,
:TGT_CNT as TGT_CNT,
:SRC_EXTRACT_DT as SRC_EXTRACT_DT,
:SRC_NM as SRC_NM,
src_edw_gen_seq.JOB_END_DT as END_DT,
src_edw_gen_seq.PRCS_STAG as PRCS_STAG,
src_edw_gen_seq.PRCS_STS as PRCS_STS,
src_edw_gen_seq.JOB_STRT_DT as STRT_DT,
src_edw_gen_seq.source_record_id
FROM
src_edw_gen_seq
);


-- Component upd_stg_prcs_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_prcs_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_prcs_id_upd.PRCS_ID as PRCS_ID,
exp_prcs_id_upd.END_DT as END_DT,
exp_prcs_id_upd.PRCS_STS as PRCS_STS,
exp_prcs_id_upd.SUB_PRCS_NM as SUB_PRCS_NM,
exp_prcs_id_upd.SRC_CNT as SRC_CNT,
exp_prcs_id_upd.TGT_CNT as TGT_CNT,
exp_prcs_id_upd.PRCS_STAG as PRCS_STAG,
exp_prcs_id_upd.SRC_EXTRACT_DT as SRC_EXTRACT_DT,
exp_prcs_id_upd.SRC_NM as SRC_NM,
exp_prcs_id_upd.STRT_DT as STRT_DT,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_prcs_id_upd
);


-- Component exp_prcs_tgt_pass_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_tgt_pass_upd AS
(
SELECT
upd_stg_prcs_upd.PRCS_ID as PRCS_ID,
upd_stg_prcs_upd.END_DT as END_DT,
upd_stg_prcs_upd.PRCS_STS as PRCS_STS,
upd_stg_prcs_upd.SUB_PRCS_NM as SUB_PRCS_NM,
upd_stg_prcs_upd.SRC_CNT as SRC_CNT,
upd_stg_prcs_upd.TGT_CNT as TGT_CNT,
upd_stg_prcs_upd.PRCS_STAG as PRCS_STAG,
upd_stg_prcs_upd.SRC_EXTRACT_DT as SRC_EXTRACT_DT,
upd_stg_prcs_upd.SRC_NM as SRC_NM,
upd_stg_prcs_upd.STRT_DT as STRT_DT,
upd_stg_prcs_upd.source_record_id
FROM
upd_stg_prcs_upd
);


-- Component etl_prcs_ctrl_upd_1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.ETL_PRCS_CTRL
USING exp_prcs_tgt_pass_upd ON (ETL_PRCS_CTRL.PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID AND ETL_PRCS_CTRL.PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG)
WHEN MATCHED THEN UPDATE
SET
PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID,
PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG,
SRC_NM = exp_prcs_tgt_pass_upd.SRC_NM,
SUB_PRCS_NM = exp_prcs_tgt_pass_upd.SUB_PRCS_NM,
JOB_START_DT = exp_prcs_tgt_pass_upd.STRT_DT,
JOB_END_DT = exp_prcs_tgt_pass_upd.END_DT,
SRC_EXTRACT_DT = exp_prcs_tgt_pass_upd.SRC_EXTRACT_DT,
PRCS_STS = exp_prcs_tgt_pass_upd.PRCS_STS,
SRC_CNT = exp_prcs_tgt_pass_upd.SRC_CNT,
TGT_CNT = exp_prcs_tgt_pass_upd.TGT_CNT;


END; ';