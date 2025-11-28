-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_FARMNAME("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT date;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 
FEED_DT:=(select current_date); 

-- Component FARMNAME2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.FARMNAME;


-- PIPELINE START FOR 1

-- Component SQ_FARMNAME3, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FARMNAME3 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
DB_T_STAG_MEMBXREF_PROD.FARMNAME
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_FARMNAME3.record_count as record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_FARMNAME3.source_record_id
FROM
SQ_FARMNAME3
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

-- PIPELINE START FOR 2

-- Component SQ_FARMNAME2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FARMNAME2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SC_POLICY_NUM,
$2 as RFD_NAME_NDX,
$3 as RFD_NAME_IND,
$4 as RFD_NAMED_NAME,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
FARMNAME3.SC_POLICY_NUM,
FARMNAME3.RFD_NAME_NDX,
FARMNAME3.RFD_NAME_IND,
FARMNAME3.RFD_NAMED_NAME
FROM DB_T_STAG_MEMBXREF_PROD.FARMNAME FARMNAME3
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_FARMNAME2.SC_POLICY_NUM as SC_POLICY_NUM,
SQ_FARMNAME2.RFD_NAME_NDX as RFD_NAME_NDX,
SQ_FARMNAME2.RFD_NAME_IND as RFD_NAME_IND,
SQ_FARMNAME2.RFD_NAMED_NAME as RFD_NAMED_NAME,
SQ_FARMNAME2.source_record_id
FROM
SQ_FARMNAME2
);


-- Component FARMNAME2, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FARMNAME
(
SC_POLICY_NUM,
RFD_NAME_NDX,
RFD_NAME_IND,
RFD_NAMED_NAME
)
SELECT
exp_pass_through.SC_POLICY_NUM as SC_POLICY_NUM,
exp_pass_through.RFD_NAME_NDX as RFD_NAME_NDX,
exp_pass_through.RFD_NAME_IND as RFD_NAME_IND,
exp_pass_through.RFD_NAMED_NAME as RFD_NAMED_NAME
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';