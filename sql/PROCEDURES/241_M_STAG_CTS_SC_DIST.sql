-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CTS_SC_DIST("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CTS_SC_DIST2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CTS_SC_DIST;


-- PIPELINE START FOR 1

-- Component SQ_CTS_SC_DIST1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CTS_SC_DIST1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as RECORD_COUNT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
 DB_T_ML_PROD.CTS_SC_DIST
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_CTS_SC_DIST1.RECORD_COUNT as record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CTS_SC_DIST1.source_record_id
FROM
SQ_CTS_SC_DIST1
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

-- Component SQ_CTS_SC_DIST, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CTS_SC_DIST AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SC_NBR,
$2 as NAME,
$3 as DISTRICT,
$4 as STATE,
$5 as COUNTY,
$6 as COMMENT,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
CTS_SC_DIST.SC_NBR,
CTS_SC_DIST.NAME,
CTS_SC_DIST.DISTRICT,
CTS_SC_DIST.STATE,
CTS_SC_DIST.COUNTY,
null COMMENT
FROM DB_T_ML_PROD.CTS_SC_DIST
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CTS_SC_DIST.SC_NBR as SC_NBR,
SQ_CTS_SC_DIST.NAME as NAME,
SQ_CTS_SC_DIST.DISTRICT as DISTRICT,
SQ_CTS_SC_DIST.STATE as STATE,
SQ_CTS_SC_DIST.COUNTY as COUNTY,
SQ_CTS_SC_DIST.COMMENT as COMMENT,
SQ_CTS_SC_DIST.source_record_id
FROM
SQ_CTS_SC_DIST
);


-- Component CTS_SC_DIST2, Type TARGET 
INSERT INTO DB_T_ML_PROD.CTS_SC_DIST
(
SC_NBR,
NAME,
DISTRICT,
STATE,
COUNTY,
COMMENTS
)
SELECT
exp_pass_through.SC_NBR as SC_NBR,
exp_pass_through.NAME as NAME,
exp_pass_through.DISTRICT as DISTRICT,
exp_pass_through.STATE as STATE,
exp_pass_through.COUNTY as COUNTY,
exp_pass_through.COMMENT as COMMENTS
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';