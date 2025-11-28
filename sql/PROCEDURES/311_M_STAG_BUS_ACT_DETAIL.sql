-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_BUS_ACT_DETAIL("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT date;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 
FEED_DT:=(select current_date); 

-- Component BUS_ACT_DETAIL2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.BUS_ACT_DETAIL;


-- PIPELINE START FOR 1

-- Component SQ_BUS_ACT_DETAIL3, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_BUS_ACT_DETAIL3 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_Count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count 
FROM
DB_T_STAG_MEMBXREF_PROD.BUS_ACT_DETAIL
) SRC
)
);


-- PIPELINE START FOR 2

-- Component SQ_BUS_ACT_DETAIL2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_BUS_ACT_DETAIL2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ACT_DATE,
$2 as STATE,
$3 as CMPY,
$4 as LOB,
$5 as POL_MEMB,
$6 as UNIT_CNT,
$7 as TX_TYPE,
$8 as AGENT,
$9 as SVC,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
BUS_ACT_DETAIL3.ACT_DATE,
BUS_ACT_DETAIL3.STATE,
BUS_ACT_DETAIL3.CMPY,
BUS_ACT_DETAIL3.LOB,
BUS_ACT_DETAIL3.POL_MEMB,
BUS_ACT_DETAIL3.UNIT_CNT,
BUS_ACT_DETAIL3.TX_TYPE,
BUS_ACT_DETAIL3.AGENT,
BUS_ACT_DETAIL3.SVC
FROM DB_T_STAG_MEMBXREF_PROD.BUS_ACT_DETAIL BUS_ACT_DETAIL3
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_BUS_ACT_DETAIL3.record_Count as record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_BUS_ACT_DETAIL3.source_record_id
FROM
SQ_BUS_ACT_DETAIL3
);


-- Component TRG_MEMBXREF, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_MEMBXREF AS
(
SELECT
exp_rec_cnt.feed_ind as feed_ind,
exp_rec_cnt.feed_dt as feed_dt,
exp_rec_cnt.record_count as record_cnt
FROM
exp_rec_cnt
);


-- Component TRG_MEMBXREF, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 1

-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_BUS_ACT_DETAIL2.ACT_DATE as ACT_DATE,
SQ_BUS_ACT_DETAIL2.STATE as STATE,
SQ_BUS_ACT_DETAIL2.CMPY as CMPY,
SQ_BUS_ACT_DETAIL2.LOB as LOB,
SQ_BUS_ACT_DETAIL2.POL_MEMB as POL_MEMB,
SQ_BUS_ACT_DETAIL2.UNIT_CNT as UNIT_CNT,
SQ_BUS_ACT_DETAIL2.TX_TYPE as TX_TYPE,
SQ_BUS_ACT_DETAIL2.AGENT as AGENT,
SQ_BUS_ACT_DETAIL2.SVC as SVC,
SQ_BUS_ACT_DETAIL2.source_record_id
FROM
SQ_BUS_ACT_DETAIL2
);


-- Component BUS_ACT_DETAIL2, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.BUS_ACT_DETAIL
(
ACT_DATE,
STATE,
CMPY,
LOB,
POL_MEMB,
UNIT_CNT,
TX_TYPE,
AGENT,
SVC
)
SELECT
exp_pass_through.ACT_DATE as ACT_DATE,
exp_pass_through.STATE as STATE,
exp_pass_through.CMPY as CMPY,
exp_pass_through.LOB as LOB,
exp_pass_through.POL_MEMB as POL_MEMB,
exp_pass_through.UNIT_CNT as UNIT_CNT,
exp_pass_through.TX_TYPE as TX_TYPE,
exp_pass_through.AGENT as AGENT,
exp_pass_through.SVC as SVC
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';