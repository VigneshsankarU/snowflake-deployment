-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_ALFA_DNC("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT date;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 
FEED_DT:=(select current_date); 

-- Component ALFA_DNC2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.ALFA_DNC;


-- PIPELINE START FOR 1

-- Component SQ_ALFA_DNC3, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ALFA_DNC3 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_Count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
DB_T_STAG_MEMBXREF_PROD.ALFA_DNC
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
TO_NUMBER(SQ_ALFA_DNC3.record_Count) as o_record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_ALFA_DNC3.source_record_id
FROM
SQ_ALFA_DNC3
);


-- Component TRG_MEMBXREF, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_MEMBXREF AS
(
SELECT
exp_rec_cnt.feed_ind as feed_ind,
exp_rec_cnt.feed_dt as feed_dt,
exp_rec_cnt.o_record_count as record_cnt
FROM
exp_rec_cnt
);


-- Component TRG_MEMBXREF, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_ALFA_DNC2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ALFA_DNC2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PHONE_NBR,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
ALFA_DNC3.PHONE_NBR
FROM DB_T_STAG_MEMBXREF_PROD.ALFA_DNC ALFA_DNC3
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_ALFA_DNC2.PHONE_NBR as PHONE_NBR,
SQ_ALFA_DNC2.source_record_id
FROM
SQ_ALFA_DNC2
);


-- Component ALFA_DNC2, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.ALFA_DNC
(
PHONE_NBR
)
SELECT
exp_pass_through.PHONE_NBR as PHONE_NBR
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';