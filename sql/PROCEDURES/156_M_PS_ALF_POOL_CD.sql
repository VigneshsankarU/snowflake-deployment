-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PS_ALF_POOL_CD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- PIPELINE START FOR 1

-- Component SQ_PS_CHARTFIELD2_TBL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_CHARTFIELD2_TBL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SETID,
$2 as EFFDT,
$3 as EFF_STATUS,
$4 as DESCR,
$5 as DESCRSHORT,
$6 as CHARTFIELD2,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT SETID, EFFDT, EFF_STATUS, DESCR, DESCRSHORT, CHARTFIELD2 FROM PS_DB2_OWNER.PS_CHARTFIELD2_TBL
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PS_CHARTFIELD2_TBL.SETID as SETID,
SQ_PS_CHARTFIELD2_TBL.EFFDT as EFFDT,
SQ_PS_CHARTFIELD2_TBL.EFF_STATUS as EFF_STATUS,
SQ_PS_CHARTFIELD2_TBL.DESCR as DESCR,
SQ_PS_CHARTFIELD2_TBL.DESCRSHORT as DESCRSHORT,
SQ_PS_CHARTFIELD2_TBL.CHARTFIELD2 as CHARTFIELD2,
SQ_PS_CHARTFIELD2_TBL.source_record_id
FROM
SQ_PS_CHARTFIELD2_TBL
);


-- Component PS_ALF_POOL_CD, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.PS_ALF_POOL_CD
(
SETID,
CHARTFIELD2,
EFFDT,
EFF_STATUS,
DESCR,
DESCRSHORT
)
SELECT
exp_pass_through.SETID as SETID,
exp_pass_through.CHARTFIELD2 as CHARTFIELD2,
exp_pass_through.EFFDT as EFFDT,
exp_pass_through.EFF_STATUS as EFF_STATUS,
exp_pass_through.DESCR as DESCR,
exp_pass_through.DESCRSHORT as DESCRSHORT
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_PS_CHARTFIELD2_TBL1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_CHARTFIELD2_TBL1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count

from

PS_DB2_OWNER.ps_chartfield2_tbl
) SRC
)
);


-- Component exp_ps_chartfield2_tbl, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ps_chartfield2_tbl AS
(
SELECT
SQ_PS_CHARTFIELD2_TBL1.record_count as record_count,
:FEED_IND as feed_ind,
to_Char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
filler,
SQ_PS_CHARTFIELD2_TBL1.source_record_id
FROM
SQ_PS_CHARTFIELD2_TBL1
);


-- Component TRG_STAG_PS_LOOKUP, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_STAG_PS_LOOKUP AS
(
SELECT
exp_ps_chartfield2_tbl.feed_ind as feed_ind,
exp_ps_chartfield2_tbl.feed_dt as feed_dt,
exp_ps_chartfield2_tbl.record_count as record_cnt,
exp_ps_chartfield2_tbl.filler as FIELD1,
exp_ps_chartfield2_tbl.filler as FIELD2,
exp_ps_chartfield2_tbl.filler as FIELD3,
exp_ps_chartfield2_tbl.filler as FIELD4,
exp_ps_chartfield2_tbl.filler as FIELD5,
exp_ps_chartfield2_tbl.filler as FIELD6,
exp_ps_chartfield2_tbl.filler as FIELD7,
exp_ps_chartfield2_tbl.filler as FIELD8,
exp_ps_chartfield2_tbl.filler as FIELD9,
exp_ps_chartfield2_tbl.filler as FIELD10,
exp_ps_chartfield2_tbl.filler as FIELD11,
exp_ps_chartfield2_tbl.filler as FIELD12,
exp_ps_chartfield2_tbl.filler as FIELD13,
exp_ps_chartfield2_tbl.filler as FIELD14,
exp_ps_chartfield2_tbl.filler as FIELD15,
exp_ps_chartfield2_tbl.filler as FIELD16,
exp_ps_chartfield2_tbl.filler as FIELD17,
exp_ps_chartfield2_tbl.filler as FIELD18,
exp_ps_chartfield2_tbl.filler as FIELD19,
exp_ps_chartfield2_tbl.filler as FIELD20,
exp_ps_chartfield2_tbl.filler as FIELD21,
exp_ps_chartfield2_tbl.filler as FIELD22,
exp_ps_chartfield2_tbl.filler as FIELD23,
exp_ps_chartfield2_tbl.filler as FIELD24,
exp_ps_chartfield2_tbl.filler as FIELD25,
exp_ps_chartfield2_tbl.filler as FIELD26,
exp_ps_chartfield2_tbl.filler as FIELD27
FROM
exp_ps_chartfield2_tbl
);


-- Component TRG_STAG_PS_LOOKUP, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 2

END; ';