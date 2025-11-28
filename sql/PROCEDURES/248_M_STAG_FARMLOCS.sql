-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_FARMLOCS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT date;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 
FEED_DT:=(select current_date); 

-- Component FARMLOCS, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.FARMLOCS;


-- PIPELINE START FOR 1

-- Component SQ_FARMLOCS3, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FARMLOCS3 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
DB_T_STAG_MEMBXREF_PROD.FARMLOCS
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_FARMLOCS3.record_count as record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_FARMLOCS3.source_record_id
FROM
SQ_FARMLOCS3
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

-- Component SQ_FARMLOCS2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FARMLOCS2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SC_POLICY_NUM,
$2 as RFD_LOC_NDX,
$3 as RFD_LOC_DESC,
$4 as RFD_LOC_ZIP,
$5 as RFD_LOC_BLDG_IND,
$6 as RFD_LOC_ACRES,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
FARMLOCS.SC_POLICY_NUM,
FARMLOCS.RFD_LOC_NDX,
FARMLOCS.RFD_LOC_DESC,
FARMLOCS.RFD_LOC_ZIP,
FARMLOCS.RFD_LOC_BLDG_IND,
FARMLOCS.RFD_LOC_ACRES
FROM DB_T_STAG_MEMBXREF_PROD.FARMLOCS
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_FARMLOCS2.SC_POLICY_NUM as SC_POLICY_NUM,
SQ_FARMLOCS2.RFD_LOC_NDX as RFD_LOC_NDX,
SQ_FARMLOCS2.RFD_LOC_DESC as RFD_LOC_DESC,
SQ_FARMLOCS2.RFD_LOC_ZIP as RFD_LOC_ZIP,
SQ_FARMLOCS2.RFD_LOC_BLDG_IND as RFD_LOC_BLDG_IND,
SQ_FARMLOCS2.RFD_LOC_ACRES as RFD_LOC_ACRES,
SQ_FARMLOCS2.source_record_id
FROM
SQ_FARMLOCS2
);


-- Component FARMLOCS, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FARMLOCS
(
SC_POLICY_NUM,
RFD_LOC_NDX,
RFD_LOC_DESC,
RFD_LOC_ZIP,
RFD_LOC_BLDG_IND,
RFD_LOC_ACRES
)
SELECT
exp_pass_through.SC_POLICY_NUM as SC_POLICY_NUM,
exp_pass_through.RFD_LOC_NDX as RFD_LOC_NDX,
exp_pass_through.RFD_LOC_DESC as RFD_LOC_DESC,
exp_pass_through.RFD_LOC_ZIP as RFD_LOC_ZIP,
exp_pass_through.RFD_LOC_BLDG_IND as RFD_LOC_BLDG_IND,
exp_pass_through.RFD_LOC_ACRES as RFD_LOC_ACRES
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';