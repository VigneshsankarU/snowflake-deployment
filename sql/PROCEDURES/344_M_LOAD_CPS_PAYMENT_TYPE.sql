-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_CPS_PAYMENT_TYPE("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CPS_PAYMENT_TYPE, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CPS_PAYMENT_TYPE;


-- PIPELINE START FOR 1

-- Component SQ_CPS_PAYMENT_TYPE, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_PAYMENT_TYPE AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PAYMENT_TYPE,
$2 as PAYMENT_TYPE_DESC,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
CPS_PAYMENT_TYPE.PAYMENT_TYPE,
CPS_PAYMENT_TYPE.PAYMENT_TYPE_DESC
FROM DB_T_ML_PROD.CPS_PAYMENT_TYPE
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CPS_PAYMENT_TYPE.PAYMENT_TYPE as PAYMENT_TYPE,
SQ_CPS_PAYMENT_TYPE.PAYMENT_TYPE_DESC as PAYMENT_TYPE_DESC,
SQ_CPS_PAYMENT_TYPE.source_record_id
FROM
SQ_CPS_PAYMENT_TYPE
);


-- Component CPS_PAYMENT_TYPE, Type TARGET 
INSERT INTO DB_T_ML_PROD.CPS_PAYMENT_TYPE
(
PAYMENT_TYPE,
PAYMENT_TYPE_DESC
)
SELECT
exp_pass_through.PAYMENT_TYPE as PAYMENT_TYPE,
exp_pass_through.PAYMENT_TYPE_DESC as PAYMENT_TYPE_DESC
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_CPS_PAYMENT_TYPE1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_PAYMENT_TYPE1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count
from DB_T_ML_PROD.cps_payment_type
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
TO_NUMBER(SQ_CPS_PAYMENT_TYPE1.record_count) as o_record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CPS_PAYMENT_TYPE1.source_record_id
FROM
SQ_CPS_PAYMENT_TYPE1
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


-- PIPELINE END FOR 2

END; ';