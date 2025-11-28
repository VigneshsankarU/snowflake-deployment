-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PS_ALF_GL_ACCOUNT("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
FEED_IND STRING;
BEGIN 

-- PIPELINE START FOR 1

-- Component SQ_PS_GL_ACCOUNT_TBL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_GL_ACCOUNT_TBL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SETID,
$2 as ACCOUNT,
$3 as EFFDT,
$4 as EFF_STATUS,
$5 as DESCR,
$6 as DESCRSHORT,
$7 as BUDG_OVERRIDE_ACCT,
$8 as ACCOUNTING_OWNER,
$9 as AB_ACCOUNT_SW,
$10 as GL_ACCOUNT_SW,
$11 as PF_ACCOUNT_SW,
$12 as ACCOUNT_TYPE,
$13 as UNIT_OF_MEASURE,
$14 as OPEN_ITEM,
$15 as OPEN_ITEM_DESCR,
$16 as OPEN_ITEM_EDIT_REC,
$17 as OPEN_ITEM_EDIT_FLD,
$18 as OPEN_ITEM_PROMPT,
$19 as OPEN_ITEM_TOL_AMT,
$20 as CURRENCY_CD,
$21 as STATISTICS_ACCOUNT,
$22 as BALANCE_FWD_SW,
$23 as CONTROL_FLAG,
$24 as BOOK_CODE,
$25 as BAL_SHEET_IND,
$26 as BUDGETARY_ONLY,
$27 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT SETID,

ACCOUNT,

EFFDT,

EFF_STATUS, 

DESCR,

DESCRSHORT, 

BUDG_OVERRIDE_ACCT,

ACCOUNTING_OWNER, 

AB_ACCOUNT_SW, 

GL_ACCOUNT_SW, 

PF_ACCOUNT_SW, 

ACCOUNT_TYPE, 

UNIT_OF_MEASURE, 

OPEN_ITEM, 

OPEN_ITEM_DESCR, 

OPEN_ITEM_EDIT_REC, 

OPEN_ITEM_EDIT_FLD, 

OPEN_ITEM_PROMPT, 

OPEN_ITEM_TOL_AMT, 

CURRENCY_CD, 

STATISTICS_ACCOUNT,

BALANCE_FWD_SW, 

CONTROL_FLAG, 

BOOK_CODE, 

BAL_SHEET_IND,

BUDGETARY_ONLY

 FROM PS_DB2_OWNER.PS_GL_ACCOUNT_TBL
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_PS_GL_ACCOUNT_TBL.SETID as SETID,
SQ_PS_GL_ACCOUNT_TBL.ACCOUNT as ACCOUNT,
SQ_PS_GL_ACCOUNT_TBL.EFFDT as EFFDT,
SQ_PS_GL_ACCOUNT_TBL.EFF_STATUS as EFF_STATUS,
SQ_PS_GL_ACCOUNT_TBL.DESCR as DESCR,
SQ_PS_GL_ACCOUNT_TBL.DESCRSHORT as DESCRSHORT,
SQ_PS_GL_ACCOUNT_TBL.BUDG_OVERRIDE_ACCT as BUDG_OVERRIDE_ACCT,
SQ_PS_GL_ACCOUNT_TBL.ACCOUNTING_OWNER as ACCOUNTING_OWNER,
SQ_PS_GL_ACCOUNT_TBL.AB_ACCOUNT_SW as AB_ACCOUNT_SW,
SQ_PS_GL_ACCOUNT_TBL.GL_ACCOUNT_SW as GL_ACCOUNT_SW,
SQ_PS_GL_ACCOUNT_TBL.PF_ACCOUNT_SW as PF_ACCOUNT_SW,
SQ_PS_GL_ACCOUNT_TBL.ACCOUNT_TYPE as ACCOUNT_TYPE,
SQ_PS_GL_ACCOUNT_TBL.UNIT_OF_MEASURE as UNIT_OF_MEASURE,
SQ_PS_GL_ACCOUNT_TBL.OPEN_ITEM as OPEN_ITEM,
SQ_PS_GL_ACCOUNT_TBL.OPEN_ITEM_DESCR as OPEN_ITEM_DESCR,
SQ_PS_GL_ACCOUNT_TBL.OPEN_ITEM_EDIT_REC as OPEN_ITEM_EDIT_REC,
SQ_PS_GL_ACCOUNT_TBL.OPEN_ITEM_EDIT_FLD as OPEN_ITEM_EDIT_FLD,
SQ_PS_GL_ACCOUNT_TBL.OPEN_ITEM_PROMPT as OPEN_ITEM_PROMPT,
SQ_PS_GL_ACCOUNT_TBL.OPEN_ITEM_TOL_AMT as OPEN_ITEM_TOL_AMT,
SQ_PS_GL_ACCOUNT_TBL.CURRENCY_CD as CURRENCY_CD,
SQ_PS_GL_ACCOUNT_TBL.STATISTICS_ACCOUNT as STATISTICS_ACCOUNT,
SQ_PS_GL_ACCOUNT_TBL.BALANCE_FWD_SW as BALANCE_FWD_SW,
SQ_PS_GL_ACCOUNT_TBL.CONTROL_FLAG as CONTROL_FLAG,
SQ_PS_GL_ACCOUNT_TBL.BOOK_CODE as BOOK_CODE,
SQ_PS_GL_ACCOUNT_TBL.BAL_SHEET_IND as BAL_SHEET_IND,
SQ_PS_GL_ACCOUNT_TBL.BUDGETARY_ONLY as BUDGETARY_ONLY,
SQ_PS_GL_ACCOUNT_TBL.source_record_id
FROM
SQ_PS_GL_ACCOUNT_TBL
);


-- Component PS_ALF_GL_ACCOUNT, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.PS_ALF_GL_ACCOUNT
(
SETID,
ACCOUNT_CD,
EFFDT,
DESCR,
ACCOUNT_TYPE,
EFF_STATUS,
DESCRSHORT,
BUDGETARY_ONLY,
BUDG_OVERRIDE_ACCT,
ACCOUNTING_OWNER,
AB_ACCOUNT_SW,
GL_ACCOUNT_SW,
PF_ACCOUNT_SW,
UNIT_OF_MEASURE,
OPEN_ITEM,
OPEN_ITEM_DESCR,
OPEN_ITEM_EDIT_REC,
OPEN_ITEM_EDIT_FLD,
OPEN_ITEM_PROMPT,
OPEN_ITEM_TOL_AMT,
CURRENCY_CD,
STATISTICS_ACCOUNT,
BALANCE_FWD_SW,
CONTROL_FLAG,
BOOK_CODE,
BAL_SHEET_IND
)
SELECT
exp_pass_through.SETID as SETID,
exp_pass_through.ACCOUNT as ACCOUNT_CD,
exp_pass_through.EFFDT as EFFDT,
exp_pass_through.DESCR as DESCR,
exp_pass_through.ACCOUNT_TYPE as ACCOUNT_TYPE,
exp_pass_through.EFF_STATUS as EFF_STATUS,
exp_pass_through.DESCRSHORT as DESCRSHORT,
exp_pass_through.BUDGETARY_ONLY as BUDGETARY_ONLY,
exp_pass_through.BUDG_OVERRIDE_ACCT as BUDG_OVERRIDE_ACCT,
exp_pass_through.ACCOUNTING_OWNER as ACCOUNTING_OWNER,
exp_pass_through.AB_ACCOUNT_SW as AB_ACCOUNT_SW,
exp_pass_through.GL_ACCOUNT_SW as GL_ACCOUNT_SW,
exp_pass_through.PF_ACCOUNT_SW as PF_ACCOUNT_SW,
exp_pass_through.UNIT_OF_MEASURE as UNIT_OF_MEASURE,
exp_pass_through.OPEN_ITEM as OPEN_ITEM,
exp_pass_through.OPEN_ITEM_DESCR as OPEN_ITEM_DESCR,
exp_pass_through.OPEN_ITEM_EDIT_REC as OPEN_ITEM_EDIT_REC,
exp_pass_through.OPEN_ITEM_EDIT_FLD as OPEN_ITEM_EDIT_FLD,
exp_pass_through.OPEN_ITEM_PROMPT as OPEN_ITEM_PROMPT,
exp_pass_through.OPEN_ITEM_TOL_AMT as OPEN_ITEM_TOL_AMT,
exp_pass_through.CURRENCY_CD as CURRENCY_CD,
exp_pass_through.STATISTICS_ACCOUNT as STATISTICS_ACCOUNT,
exp_pass_through.BALANCE_FWD_SW as BALANCE_FWD_SW,
exp_pass_through.CONTROL_FLAG as CONTROL_FLAG,
exp_pass_through.BOOK_CODE as BOOK_CODE,
exp_pass_through.BAL_SHEET_IND as BAL_SHEET_IND
FROM
exp_pass_through;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_PS_GL_ACCOUNT_TBL1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PS_GL_ACCOUNT_TBL1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as record_count,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select count(*) as record_count

from

PS_DB2_OWNER.ps_gl_account_tbl
) SRC
)
);


-- Component exp_ps_gl_account_tbl, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ps_gl_account_tbl AS
(
SELECT
SQ_PS_GL_ACCOUNT_TBL1.record_count as record_count,
:FEED_IND as feed_ind,
to_char ( CURRENT_TIMESTAMP , ''YYYY-MM-DD'' ) as feed_dt,
filler,
SQ_PS_GL_ACCOUNT_TBL1.source_record_id
FROM
SQ_PS_GL_ACCOUNT_TBL1
);


-- Component TRG_STAG_PS_LOOKUP, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_STAG_PS_LOOKUP AS
(
SELECT
exp_ps_gl_account_tbl.feed_ind as feed_ind,
exp_ps_gl_account_tbl.feed_dt as feed_dt,
exp_ps_gl_account_tbl.record_count as record_cnt,
exp_ps_gl_account_tbl.filler as FIELD1,
exp_ps_gl_account_tbl.filler as FIELD2,
exp_ps_gl_account_tbl.filler as FIELD3,
exp_ps_gl_account_tbl.filler as FIELD4,
exp_ps_gl_account_tbl.filler as FIELD5,
exp_ps_gl_account_tbl.filler as FIELD6,
exp_ps_gl_account_tbl.filler as FIELD7,
exp_ps_gl_account_tbl.filler as FIELD8,
exp_ps_gl_account_tbl.filler as FIELD9,
exp_ps_gl_account_tbl.filler as FIELD10,
exp_ps_gl_account_tbl.filler as FIELD11,
exp_ps_gl_account_tbl.filler as FIELD12,
exp_ps_gl_account_tbl.filler as FIELD13,
exp_ps_gl_account_tbl.filler as FIELD14,
exp_ps_gl_account_tbl.filler as FIELD15,
exp_ps_gl_account_tbl.filler as FIELD16,
exp_ps_gl_account_tbl.filler as FIELD17,
exp_ps_gl_account_tbl.filler as FIELD18,
exp_ps_gl_account_tbl.filler as FIELD19,
exp_ps_gl_account_tbl.filler as FIELD20,
exp_ps_gl_account_tbl.filler as FIELD21,
exp_ps_gl_account_tbl.filler as FIELD22,
exp_ps_gl_account_tbl.filler as FIELD23,
exp_ps_gl_account_tbl.filler as FIELD24,
exp_ps_gl_account_tbl.filler as FIELD25,
exp_ps_gl_account_tbl.filler as FIELD26,
exp_ps_gl_account_tbl.filler as FIELD27
FROM
exp_ps_gl_account_tbl
);


-- Component TRG_STAG_PS_LOOKUP, Type EXPORT_DATA Exporting data
;
COPY INTO @public.edw_stage/my_export_folder/TRG_STAG_PS_LOOKUP_
FROM (SELECT * FROM TRG_STAG_PS_LOOKUP)
HEADER = TRUE
OVERWRITE = TRUE;

-- PIPELINE END FOR 2

END; ';