-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_MEMBER("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
FEED_DT varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 
FEED_DT:=(select current_date); 

-- Component SQ_MEMBER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_MEMBER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT count(*) as record_count
FROM DB_T_STAG_MEMBXREF_PROD.MEMBER
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_MEMBER.record_count as record_count,
:feed_ind as feed_ind,
:feed_dt as feed_dt,
SQ_MEMBER.source_record_id
FROM
SQ_MEMBER
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


END; ';