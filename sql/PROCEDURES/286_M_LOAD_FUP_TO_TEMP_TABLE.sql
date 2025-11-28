-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_FUP_TO_TEMP_TABLE("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- PIPELINE START FOR 1

-- Component SQ_RTS_CONTROL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_RTS_CONTROL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PROCESS_NAME,
$2 as ACTUAL_RUN_TS,
$3 as ACTUAL_BEG_TS,
$4 as ACTUAL_END_TS,
$5 as REC_COUNT,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select ''FNOL''
, current_timestamp
, max (ACTUAL_END_TS  )
, timestamp  ''2099-12-31 00:01:01.000000''
, 0
from DB_T_CTRL_PROD.RTS_CONTROL
) SRC
)
);


-- PIPELINE START FOR 2
-- Component FNOL_TEMP, Type Pre SQL 
Delete from DB_T_STAG_MEMBXREF_PROD.FNOL_TEMP;


-- Component SQ_CLAIM_TAB, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CLAIM_TAB AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Claim_Nbr,
$2 as Claim_Name,
$3 as Claim_num2,
$4 as Claim_cd,
$5 as Claim_agent,
$6 as Claim_date,
$7 as claim_desc,
$8 as create_ts,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select
CLM_CLAIM_NBR
, '' ''
, CLM_CLAIM_NBR
, CLM_CAUSE_LOSS_CD
, '' ''
, CLM_LOSS_DT
, '' ''
, CLM_CREATE_TS
from DB_T_ONSITE_PROD.CLAIM_TAB a,
DB_T_CTRL_PROD.RTS_CONTROL b
where (a.CLM_CREATE_TS between b.actual_beg_ts and actual_end_ts)
and (a.clm_status_cd = ''c'' and a.clm_loss_dt <= (cast(b.actual_beg_ts as date)) or (a.clm_status_cd in (''o'', ''m'')))
and actual_run_ts in (select max(actual_run_ts) from DB_T_CTRL_PROD.RTS_CONTROL)
) SRC
)
);


-- Component exp_Pass_Through1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Pass_Through1 AS
(
SELECT
SQ_CLAIM_TAB.Claim_Nbr as Claim_Nbr,
SQ_CLAIM_TAB.Claim_Name as Claim_Name,
SQ_CLAIM_TAB.Claim_num2 as Claim_num2,
SQ_CLAIM_TAB.Claim_cd as Claim_cd,
SQ_CLAIM_TAB.Claim_agent as Claim_agent,
SQ_CLAIM_TAB.Claim_date as Claim_date,
SQ_CLAIM_TAB.claim_desc as claim_desc,
SQ_CLAIM_TAB.create_ts as create_ts,
SQ_CLAIM_TAB.source_record_id
FROM
SQ_CLAIM_TAB
);


-- Component FNOL_TEMP, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FNOL_TEMP
(
Claim_Nbr,
Claim_Name,
Claim_num2,
Claim_cd,
Claim_agent,
Claim_date,
claim_desc,
create_ts
)
SELECT
exp_Pass_Through1.Claim_Nbr as Claim_Nbr,
exp_Pass_Through1.Claim_Name as Claim_Name,
exp_Pass_Through1.Claim_num2 as Claim_num2,
exp_Pass_Through1.Claim_cd as Claim_cd,
exp_Pass_Through1.Claim_agent as Claim_agent,
exp_Pass_Through1.Claim_date as Claim_date,
exp_Pass_Through1.claim_desc as claim_desc,
exp_Pass_Through1.create_ts as create_ts
FROM
exp_Pass_Through1;


-- PIPELINE END FOR 2
-- Component FNOL_TEMP, Type Post SQL 
update DB_T_CTRL_PROD.RTS_CONTROL
set ACTUAL_END_TS = current_timestamp
where ACTUAL_END_TS = timestamp  ''2099-12-31 00:01:01.000000''
and rec_count = 0;


-- Component exp_Pass_Through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Pass_Through AS
(
SELECT
SQ_RTS_CONTROL.PROCESS_NAME as PROCESS_NAME,
SQ_RTS_CONTROL.ACTUAL_RUN_TS as ACTUAL_RUN_TS,
SQ_RTS_CONTROL.ACTUAL_BEG_TS as ACTUAL_BEG_TS,
SQ_RTS_CONTROL.ACTUAL_END_TS as ACTUAL_END_TS,
SQ_RTS_CONTROL.REC_COUNT as REC_COUNT,
SQ_RTS_CONTROL.source_record_id
FROM
SQ_RTS_CONTROL
);


-- Component RTS_CONTROL1, Type TARGET 
INSERT INTO DB_T_CTRL_PROD.RTS_CONTROL
(
PROCESS_NAME,
ACTUAL_RUN_TS,
ACTUAL_BEG_TS,
ACTUAL_END_TS,
REC_COUNT
)
SELECT
exp_Pass_Through.PROCESS_NAME as PROCESS_NAME,
exp_Pass_Through.ACTUAL_RUN_TS as ACTUAL_RUN_TS,
exp_Pass_Through.ACTUAL_BEG_TS as ACTUAL_BEG_TS,
exp_Pass_Through.ACTUAL_END_TS as ACTUAL_END_TS,
exp_Pass_Through.REC_COUNT as REC_COUNT
FROM
exp_Pass_Through;


-- PIPELINE END FOR 1

END; ';