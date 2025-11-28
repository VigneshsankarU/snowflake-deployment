-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_CPS_PMT_EPO("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CPS_PMT_EPO2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CPS_PMT_EPO;


-- PIPELINE START FOR 1

-- Component SQ_CPS_PMT_EPO, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_PMT_EPO AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as RECEIPT_NBR,
$2 as SEQ_NBR,
$3 as EPO_AMT,
$4 as PMT_METHOD_CD,
$5 as CUR_STAT_CD,
$6 as TRANS_REF_NBR,
$7 as EP_ACCT_NBR,
$8 as CARD_TYPE,
$9 as TRANS_TYPE,
$10 as BATCH_NBR,
$11 as TRANS_NBR,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
CPS_PMT_EPO.RECEIPT_NBR,
CPS_PMT_EPO.SEQ_NBR,
CPS_PMT_EPO.EPO_AMT,
CPS_PMT_EPO.PMT_METHOD_CD,
CPS_PMT_EPO.CUR_STAT_CD,
CPS_PMT_EPO.TRANS_REF_NBR,
CPS_PMT_EPO.EP_ACCT_NBR,
CPS_PMT_EPO.CARD_TYPE,
CPS_PMT_EPO.TRANS_TYPE,
CPS_PMT_EPO.BATCH_NBR,
CPS_PMT_EPO.TRANS_NBR
FROM DB_T_ML_PROD.CPS_PMT_EPO
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CPS_PMT_EPO.RECEIPT_NBR as RECEIPT_NBR,
SQ_CPS_PMT_EPO.SEQ_NBR as SEQ_NBR,
SQ_CPS_PMT_EPO.EPO_AMT as EPO_AMT,
SQ_CPS_PMT_EPO.PMT_METHOD_CD as PMT_METHOD_CD,
SQ_CPS_PMT_EPO.CUR_STAT_CD as CUR_STAT_CD,
SQ_CPS_PMT_EPO.TRANS_REF_NBR as TRANS_REF_NBR,
SQ_CPS_PMT_EPO.EP_ACCT_NBR as EP_ACCT_NBR,
SQ_CPS_PMT_EPO.CARD_TYPE as CARD_TYPE,
SQ_CPS_PMT_EPO.TRANS_TYPE as TRANS_TYPE,
SQ_CPS_PMT_EPO.BATCH_NBR as BATCH_NBR,
SQ_CPS_PMT_EPO.TRANS_NBR as TRANS_NBR,
SQ_CPS_PMT_EPO.source_record_id
FROM
SQ_CPS_PMT_EPO
);


-- Component CPS_PMT_EPO2, Type TARGET 
INSERT INTO DB_T_ML_PROD.CPS_PMT_EPO
(
RECEIPT_NBR,
SEQ_NBR,
EPO_AMT,
PMT_METHOD_CD,
CUR_STAT_CD,
TRANS_REF_NBR,
EP_ACCT_NBR,
CARD_TYPE,
TRANS_TYPE,
BATCH_NBR,
TRANS_NBR
)
SELECT
exp_pass_through.RECEIPT_NBR as RECEIPT_NBR,
exp_pass_through.SEQ_NBR as SEQ_NBR,
exp_pass_through.EPO_AMT as EPO_AMT,
exp_pass_through.PMT_METHOD_CD as PMT_METHOD_CD,
exp_pass_through.CUR_STAT_CD as CUR_STAT_CD,
exp_pass_through.TRANS_REF_NBR as TRANS_REF_NBR,
exp_pass_through.EP_ACCT_NBR as EP_ACCT_NBR,
exp_pass_through.CARD_TYPE as CARD_TYPE,
exp_pass_through.TRANS_TYPE as TRANS_TYPE,
exp_pass_through.BATCH_NBR as BATCH_NBR,
exp_pass_through.TRANS_NBR as TRANS_NBR
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_CPS_PMT_EPO1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_PMT_EPO1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count
from DB_T_ML_PROD.cps_pmt_epo
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
TO_NUMBER(SQ_CPS_PMT_EPO1.record_count) as o_record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CPS_PMT_EPO1.source_record_id
FROM
SQ_CPS_PMT_EPO1
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