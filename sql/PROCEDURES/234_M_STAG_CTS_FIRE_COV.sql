-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CTS_FIRE_COV("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CTS_FIRE_COV2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CTS_FIRE_COV;


-- PIPELINE START FOR 1

-- Component SQ_CTS_FIRE_COV1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CTS_FIRE_COV1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as RECORD_COUNT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
 DB_T_ML_PROD.CTS_FIRE_COV
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_CTS_FIRE_COV1.RECORD_COUNT as record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CTS_FIRE_COV1.source_record_id
FROM
SQ_CTS_FIRE_COV1
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

-- Component SQ_CTS_FIRE_COV, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CTS_FIRE_COV AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CLAIMS_TRACK_NUM,
$2 as COVERAGE_SEQ,
$3 as FI_CLASS,
$4 as FI_COVERAGE,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT CLAIMS_TRACK_NUM, COVERAGE_SEQ, FI_CLASS, FI_COVERAGE FROM DB_T_ML_PROD.CTS_FIRE_COV
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CTS_FIRE_COV.CLAIMS_TRACK_NUM as CLAIMS_TRACK_NUM,
SQ_CTS_FIRE_COV.COVERAGE_SEQ as COVERAGE_SEQ,
SQ_CTS_FIRE_COV.FI_CLASS as FI_CLASS,
SQ_CTS_FIRE_COV.FI_COVERAGE as FI_COVERAGE,
SQ_CTS_FIRE_COV.source_record_id
FROM
SQ_CTS_FIRE_COV
);


-- Component CTS_FIRE_COV2, Type TARGET 
INSERT INTO DB_T_ML_PROD.CTS_FIRE_COV
(
CLAIMS_TRACK_NUM,
COVERAGE_SEQ,
FI_CLASS,
FI_COVERAGE
)
SELECT
exp_pass_through.CLAIMS_TRACK_NUM as CLAIMS_TRACK_NUM,
exp_pass_through.COVERAGE_SEQ as COVERAGE_SEQ,
exp_pass_through.FI_CLASS as FI_CLASS,
exp_pass_through.FI_COVERAGE as FI_COVERAGE
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';