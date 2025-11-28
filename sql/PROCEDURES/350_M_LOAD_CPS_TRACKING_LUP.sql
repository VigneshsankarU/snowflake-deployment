-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_CPS_TRACKING_LUP("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE FEED_IND varchar;
BEGIN 

FEED_IND:=(select DAILY_FEED_IND from DB_T_CTRL_PROD.ECTL_JOB_LOAD_STATUS_LOG where ECTL_BATCH_ID= :run_id); 

-- Component CPS_TRACKING_LUP2, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_ML_PROD.CPS_TRACKING_LUP;


-- PIPELINE START FOR 1

-- Component SQ_CPS_TRACKING_LUP, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_TRACKING_LUP AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SYST,
$2 as PYMT_TYPE,
$3 as ADD_PYMT_TYPE,
$4 as MAN_RSN_CD,
$5 as BAL_TYPE,
$6 as BEG_TRACKING_DAYS,
$7 as DEPT,
$8 as DEPT_HEAD,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 
SYST,
PYMT_TYPE,
ADD_PYMT_TYPE,
MAN_RSN_CD,
BAL_TYPE,
BEG_TRACKING_DAYS,
DEPT,
DEPT_HEAD FROM DB_T_ML_PROD.CPS_TRACKING_LUP
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_CPS_TRACKING_LUP.SYST as SYST,
SQ_CPS_TRACKING_LUP.PYMT_TYPE as PYMT_TYPE,
SQ_CPS_TRACKING_LUP.ADD_PYMT_TYPE as ADD_PYMT_TYPE,
SQ_CPS_TRACKING_LUP.MAN_RSN_CD as MAN_RSN_CD,
SQ_CPS_TRACKING_LUP.BAL_TYPE as BAL_TYPE,
SQ_CPS_TRACKING_LUP.BEG_TRACKING_DAYS as BEG_TRACKING_DAYS,
SQ_CPS_TRACKING_LUP.DEPT as DEPT,
SQ_CPS_TRACKING_LUP.DEPT_HEAD as DEPT_HEAD,
SQ_CPS_TRACKING_LUP.source_record_id
FROM
SQ_CPS_TRACKING_LUP
);


-- PIPELINE START FOR 2

-- Component SQ_CPS_TRACKING_LUP1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CPS_TRACKING_LUP1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count
from DB_T_ML_PROD.cps_tracking_lup
) SRC
)
);


-- Component exp_rec_cnt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_rec_cnt AS
(
SELECT
SQ_CPS_TRACKING_LUP1.record_count as record_count,
:feed_ind as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
SQ_CPS_TRACKING_LUP1.source_record_id
FROM
SQ_CPS_TRACKING_LUP1
);


-- Component CPS_TRACKING_LUP2, Type TARGET 
INSERT INTO DB_T_ML_PROD.CPS_TRACKING_LUP
(
SYST,
PYMT_TYPE,
ADD_PYMT_TYPE,
MAN_RSN_CD,
BAL_TYPE,
BEG_TRACKING_DAYS,
DEPT,
DEPT_HEAD
)
SELECT
exp_pass_through.SYST as SYST,
exp_pass_through.PYMT_TYPE as PYMT_TYPE,
exp_pass_through.ADD_PYMT_TYPE as ADD_PYMT_TYPE,
exp_pass_through.MAN_RSN_CD as MAN_RSN_CD,
exp_pass_through.BAL_TYPE as BAL_TYPE,
exp_pass_through.BEG_TRACKING_DAYS as BEG_TRACKING_DAYS,
exp_pass_through.DEPT as DEPT,
exp_pass_through.DEPT_HEAD as DEPT_HEAD
FROM
exp_pass_through;


-- PIPELINE END FOR 1

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


-- PIPELINE END FOR 2

END; ';