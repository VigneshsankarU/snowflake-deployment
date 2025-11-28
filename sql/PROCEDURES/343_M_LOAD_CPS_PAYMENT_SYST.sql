-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_CPS_PAYMENT_SYST("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CPS_PAYMENT_SYST2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CPS_PAYMENT_SYST;


-- PIPELINE START FOR 1

-- Component SQ_CPS_PAYMENT_SYST, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_PAYMENT_SYST AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PAYMENT_SYST,
$2 as REPORT_LINE,
$3 as LINE_TYPE,
$4 as LINE_DESC,
$5 as LOB,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
CPS_PAYMENT_SYST.PAYMENT_SYST,
CPS_PAYMENT_SYST.REPORT_LINE,
CPS_PAYMENT_SYST.LINE_TYPE,
CPS_PAYMENT_SYST.LINE_DESC,
CPS_PAYMENT_SYST.LOB
FROM DB_T_ML_PROD.CPS_PAYMENT_SYST
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CPS_PAYMENT_SYST.PAYMENT_SYST as PAYMENT_SYST,
SQ_CPS_PAYMENT_SYST.REPORT_LINE as REPORT_LINE,
SQ_CPS_PAYMENT_SYST.LINE_TYPE as LINE_TYPE,
SQ_CPS_PAYMENT_SYST.LINE_DESC as LINE_DESC,
SQ_CPS_PAYMENT_SYST.LOB as LOB,
SQ_CPS_PAYMENT_SYST.source_record_id
FROM
SQ_CPS_PAYMENT_SYST
);


-- Component CPS_PAYMENT_SYST2, Type TARGET 
INSERT INTO DB_T_ML_PROD.CPS_PAYMENT_SYST
(
PAYMENT_SYST,
REPORT_LINE,
LINE_TYPE,
LINE_DESC,
LOB
)
SELECT
exp_pass_through.PAYMENT_SYST as PAYMENT_SYST,
exp_pass_through.REPORT_LINE as REPORT_LINE,
exp_pass_through.LINE_TYPE as LINE_TYPE,
exp_pass_through.LINE_DESC as LINE_DESC,
exp_pass_through.LOB as LOB
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_CPS_PAYMENT_SYST1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_PAYMENT_SYST1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count
from DB_T_ML_PROD.cps_payment_syst
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
TO_NUMBER(SQ_CPS_PAYMENT_SYST1.record_count) as o_record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CPS_PAYMENT_SYST1.source_record_id
FROM
SQ_CPS_PAYMENT_SYST1
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