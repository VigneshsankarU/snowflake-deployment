-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PS_ALF_COMPANY_CD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
FEED_IND varchar; 
BEGIN 
set FEED_IND = ''Y'';
-- PIPELINE START FOR 1

-- Component SQ_PS_ALF_CMPY_TRANS, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_ALF_CMPY_TRANS AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SETID,
$2 as ALF_CMPY_CD,
$3 as ALF_COMPANY_CD,
$4 as BUSINESS_UNIT_GL,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT SETID, ALF_CMPY_CD, ALF_COMPANY_CD, BUSINESS_UNIT_GL FROM PS_DB2_OWNER.PS_ALF_CMPY_TRANS
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PS_ALF_CMPY_TRANS.SETID as SETID,
SQ_PS_ALF_CMPY_TRANS.ALF_CMPY_CD as ALF_CMPY_CD,
SQ_PS_ALF_CMPY_TRANS.ALF_COMPANY_CD as ALF_COMPANY_CD,
SQ_PS_ALF_CMPY_TRANS.BUSINESS_UNIT_GL as BUSINESS_UNIT_GL,
SQ_PS_ALF_CMPY_TRANS.source_record_id
FROM
SQ_PS_ALF_CMPY_TRANS
);


-- Component PS_ALF_COMPANY_CD, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.PS_ALF_COMPANY_CD
(
SETID,
ALF_CMPY_CD,
ALF_COMPANY_CD,
BUSINESS_UNIT_GL
)
SELECT
exp_pass_through.SETID as SETID,
exp_pass_through.ALF_CMPY_CD as ALF_CMPY_CD,
exp_pass_through.ALF_COMPANY_CD as ALF_COMPANY_CD,
exp_pass_through.BUSINESS_UNIT_GL as BUSINESS_UNIT_GL
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_PS_ALF_CMPY_TRANS1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_ALF_CMPY_TRANS1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_Count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count

from

PS_DB2_OWNER.ps_alf_cmpy_trans
) SRC
)
);


-- Component exp_PS_ALF_CMPY_TRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_PS_ALF_CMPY_TRANS AS
(
SELECT
TO_NUMBER(SQ_PS_ALF_CMPY_TRANS1.record_Count) as o_record_count,
:FEED_IND as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
filler,
SQ_PS_ALF_CMPY_TRANS1.source_record_id
FROM
SQ_PS_ALF_CMPY_TRANS1
);


-- Component TRG_STAG_PS_LOOKUP, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_STAG_PS_LOOKUP AS
(
SELECT
exp_PS_ALF_CMPY_TRANS.feed_ind as feed_ind,
exp_PS_ALF_CMPY_TRANS.feed_dt as feed_dt,
exp_PS_ALF_CMPY_TRANS.o_record_count as record_cnt,
exp_PS_ALF_CMPY_TRANS.filler as FIELD1,
exp_PS_ALF_CMPY_TRANS.filler as FIELD2,
exp_PS_ALF_CMPY_TRANS.filler as FIELD3,
exp_PS_ALF_CMPY_TRANS.filler as FIELD4,
exp_PS_ALF_CMPY_TRANS.filler as FIELD5,
exp_PS_ALF_CMPY_TRANS.filler as FIELD6,
exp_PS_ALF_CMPY_TRANS.filler as FIELD7,
exp_PS_ALF_CMPY_TRANS.filler as FIELD8,
exp_PS_ALF_CMPY_TRANS.filler as FIELD9,
exp_PS_ALF_CMPY_TRANS.filler as FIELD10,
exp_PS_ALF_CMPY_TRANS.filler as FIELD11,
exp_PS_ALF_CMPY_TRANS.filler as FIELD12,
exp_PS_ALF_CMPY_TRANS.filler as FIELD13,
exp_PS_ALF_CMPY_TRANS.filler as FIELD14,
exp_PS_ALF_CMPY_TRANS.filler as FIELD15,
exp_PS_ALF_CMPY_TRANS.filler as FIELD16,
exp_PS_ALF_CMPY_TRANS.filler as FIELD17,
exp_PS_ALF_CMPY_TRANS.filler as FIELD18,
exp_PS_ALF_CMPY_TRANS.filler as FIELD19,
exp_PS_ALF_CMPY_TRANS.filler as FIELD20,
exp_PS_ALF_CMPY_TRANS.filler as FIELD21,
exp_PS_ALF_CMPY_TRANS.filler as FIELD22,
exp_PS_ALF_CMPY_TRANS.filler as FIELD23,
exp_PS_ALF_CMPY_TRANS.filler as FIELD24,
exp_PS_ALF_CMPY_TRANS.filler as FIELD25,
exp_PS_ALF_CMPY_TRANS.filler as FIELD26,
exp_PS_ALF_CMPY_TRANS.filler as FIELD27
FROM
exp_PS_ALF_CMPY_TRANS
);


-- Component TRG_STAG_PS_LOOKUP, Type EXPORT_DATA Exporting data
;
COPY INTO @my_internal_stage/my_export_folder/TRG_STAG_PS_LOOKUP_
FROM (SELECT * FROM TRG_STAG_PS_LOOKUP)
HEADER = TRUE
OVERWRITE = TRUE;

-- PIPELINE END FOR 2

END; ';