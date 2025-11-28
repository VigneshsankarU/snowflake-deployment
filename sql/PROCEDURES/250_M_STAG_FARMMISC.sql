-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_FARMMISC("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT date;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 
FEED_DT:=(select current_date); 

-- Component FARMMISC2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.FARMMISC;


-- PIPELINE START FOR 1

-- Component SQ_FARMMISC, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FARMMISC AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_Count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
DB_T_STAG_MEMBXREF_PROD.FARMMISC
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_FARMMISC.record_Count as record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_FARMMISC.source_record_id
FROM
SQ_FARMMISC
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

-- Component SQ_FARMMISC2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FARMMISC2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SC_POLICY_NUM,
$2 as RFD_MISC_NDX,
$3 as RFD_MISC_IND,
$4 as RFD_MISC_INFO,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
FARMMISC.SC_POLICY_NUM,
FARMMISC.RFD_MISC_NDX,
FARMMISC.RFD_MISC_IND,
FARMMISC.RFD_MISC_INFO
FROM DB_T_STAG_MEMBXREF_PROD.FARMMISC
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_FARMMISC2.SC_POLICY_NUM as SC_POLICY_NUM,
SQ_FARMMISC2.RFD_MISC_NDX as RFD_MISC_NDX,
SQ_FARMMISC2.RFD_MISC_IND as RFD_MISC_IND,
SQ_FARMMISC2.RFD_MISC_INFO as RFD_MISC_INFO,
SQ_FARMMISC2.source_record_id
FROM
SQ_FARMMISC2
);


-- Component FARMMISC2, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FARMMISC
(
SC_POLICY_NUM,
RFD_MISC_NDX,
RFD_MISC_IND,
RFD_MISC_INFO
)
SELECT
exp_pass_through.SC_POLICY_NUM as SC_POLICY_NUM,
exp_pass_through.RFD_MISC_NDX as RFD_MISC_NDX,
exp_pass_through.RFD_MISC_IND as RFD_MISC_IND,
exp_pass_through.RFD_MISC_INFO as RFD_MISC_INFO
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';