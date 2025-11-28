-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CTS_CAT_CD_HIST("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CTS_CAT_CD_HIST2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CTS_CAT_CD_HIST;


-- PIPELINE START FOR 1

-- Component SQ_CTS_CAT_CD_HIST1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CTS_CAT_CD_HIST1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as RECORD_COUNT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
 DB_T_ML_PROD.CTS_CAT_CD_HIST
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_CTS_CAT_CD_HIST1.RECORD_COUNT as record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CTS_CAT_CD_HIST1.source_record_id
FROM
SQ_CTS_CAT_CD_HIST1
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

-- Component SQ_CTS_CAT_CD_HIST, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CTS_CAT_CD_HIST AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CLAIMS_TRACK_NUM,
$2 as CAT_CD_SEQ_NUM,
$3 as CAT_CD,
$4 as CAT_CD_DATE,
$5 as CAT_CD_TIME,
$6 as CAT_CD_USERID,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
CTS_CAT_CD_HIST.CLAIMS_TRACK_NUM,
CTS_CAT_CD_HIST.CAT_CD_SEQ_NUM,
CTS_CAT_CD_HIST.CAT_CD,
CTS_CAT_CD_HIST.CAT_CD_DATE,
CTS_CAT_CD_HIST.CAT_CD_TIME,
CTS_CAT_CD_HIST.CAT_CD_USERID
FROM DB_T_ML_PROD.CTS_CAT_CD_HIST
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CTS_CAT_CD_HIST.CLAIMS_TRACK_NUM as CLAIMS_TRACK_NUM,
SQ_CTS_CAT_CD_HIST.CAT_CD_SEQ_NUM as CAT_CD_SEQ_NUM,
SQ_CTS_CAT_CD_HIST.CAT_CD as CAT_CD,
SQ_CTS_CAT_CD_HIST.CAT_CD_DATE as CAT_CD_DATE,
TO_CHAR ( SQ_CTS_CAT_CD_HIST.CAT_CD_TIME ) as o_CAT_CD_TIME,
SQ_CTS_CAT_CD_HIST.CAT_CD_USERID as CAT_CD_USERID,
SQ_CTS_CAT_CD_HIST.source_record_id
FROM
SQ_CTS_CAT_CD_HIST
);


-- Component CTS_CAT_CD_HIST2, Type TARGET 
INSERT INTO DB_T_ML_PROD.CTS_CAT_CD_HIST
(
CLAIMS_TRACK_NUM,
CAT_CD_SEQ_NUM,
CAT_CD,
CAT_CD_DATE,
CAT_CD_TIME,
CAT_CD_USERID
)
SELECT
exp_pass_through.CLAIMS_TRACK_NUM as CLAIMS_TRACK_NUM,
exp_pass_through.CAT_CD_SEQ_NUM as CAT_CD_SEQ_NUM,
exp_pass_through.CAT_CD as CAT_CD,
exp_pass_through.CAT_CD_DATE as CAT_CD_DATE,
exp_pass_through.o_CAT_CD_TIME as CAT_CD_TIME,
exp_pass_through.CAT_CD_USERID as CAT_CD_USERID
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';