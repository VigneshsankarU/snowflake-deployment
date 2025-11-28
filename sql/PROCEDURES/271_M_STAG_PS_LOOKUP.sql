-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_PS_LOOKUP("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT date;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id);
FEED_DT:=(select current_date);

-- Component PS_LOOKUP2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.PS_LOOKUP;


-- PIPELINE START FOR 1

-- Component SQ_PS_LOOKUP3, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_LOOKUP3 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM
DB_T_STAG_MEMBXREF_PROD.PS_LOOKUP
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
TO_NUMBER(SQ_PS_LOOKUP3.record_count) as o_record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_PS_LOOKUP3.source_record_id
FROM
SQ_PS_LOOKUP3
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

-- Component SQ_PS_LOOKUP2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_LOOKUP2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FIELD_NAME,
$2 as STATE_CODE,
$3 as FIELD_VALUE,
$4 as DESC1,
$5 as DESC2,
$6 as DESC3,
$7 as DESC4,
$8 as SC_DESC,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
PS_LOOKUP3.FIELD_NAME,
PS_LOOKUP3.STATE_CODE,
PS_LOOKUP3.FIELD_VALUE,
PS_LOOKUP3.DESC1,
PS_LOOKUP3.DESC2,
PS_LOOKUP3.DESC3,
PS_LOOKUP3.DESC4,
PS_LOOKUP3.SC_DESC
FROM DB_T_STAG_MEMBXREF_PROD.PS_LOOKUP PS_LOOKUP3
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PS_LOOKUP2.FIELD_NAME as FIELD_NAME,
SQ_PS_LOOKUP2.STATE_CODE as STATE_CODE,
SQ_PS_LOOKUP2.FIELD_VALUE as FIELD_VALUE,
SQ_PS_LOOKUP2.DESC1 as DESC1,
SQ_PS_LOOKUP2.DESC2 as DESC2,
SQ_PS_LOOKUP2.DESC3 as DESC3,
SQ_PS_LOOKUP2.DESC4 as DESC4,
SQ_PS_LOOKUP2.SC_DESC as SC_DESC,
SQ_PS_LOOKUP2.source_record_id
FROM
SQ_PS_LOOKUP2
);


-- Component PS_LOOKUP2, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.PS_LOOKUP
(
FIELD_NAME,
STATE_CODE,
FIELD_VALUE,
DESC1,
DESC2,
DESC3,
DESC4,
SC_DESC
)
SELECT
exp_pass_through.FIELD_NAME as FIELD_NAME,
exp_pass_through.STATE_CODE as STATE_CODE,
exp_pass_through.FIELD_VALUE as FIELD_VALUE,
exp_pass_through.DESC1 as DESC1,
exp_pass_through.DESC2 as DESC2,
exp_pass_through.DESC3 as DESC3,
exp_pass_through.DESC4 as DESC4,
exp_pass_through.SC_DESC as SC_DESC
FROM
exp_pass_through;


-- PIPELINE END FOR 2

END; ';