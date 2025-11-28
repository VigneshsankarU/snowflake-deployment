-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_MANUAL_ENTRIES_GL("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File
(
Ledger varchar(25),
Unit varchar(10),
Journal_ID varchar(25),
Date varchar(29),
Line_number integer,
--commented as special symbols not supported Line_# 
Account varchar(50),
Dept integer,
State varchar(5),
Pool_Ind varchar(10),
Reins_Cd varchar(5),
Project varchar(25),
Line_Descr varchar(25),
Sum_Amount varchar(26),
Source varchar(5),
Ref_No integer,
User varchar(50),
Unit1 varchar(10),
Affiliate varchar(25),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Ledger ) ) as out_ledger,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Unit ) ) as out_unit,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Journal_ID ) ) as out_journal_ID,
TO_DATE ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Date , ''MM-DD-YYYY'' ) as o_Date,
LTRIM ( RTRIM ( Line_ ) ) as "out_Line_number",
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Account ) ) as out_account,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Dept ) ) as out_dept,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.State ) ) as out_state,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Pool_Ind ) ) as out_pool_ind,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Reins_Cd ) ) as out_reins_cd,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Project ) ) as out_project,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Line_Descr ) ) as out_line_descr,
REPLACE(ltrim ( rtrim ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Sum_Amount ) ),'','','''') as v_Sum_Amount,
IFNULL(TRY_TO_DECIMAL(v_Sum_Amount), 0) as o_Sum_Amount,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Source ) ) as out_source,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Ref_No ) ) as out_ref_no,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Unit1 ) ) as out_unit1,
LTRIM ( RTRIM ( SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.Affiliate ) ) as out_affiliate,
SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File.source_record_id
FROM
SQ_Shortcut_to_Alf_GL_Manual_Entries_SRC_File
);


-- Component Shortcut_to_STG_FIN_MANUAL_ENTRIES_GL, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.STG_FIN_MANUAL_GL_ENTRIES
(
LEDGER,
BUSINESS_UNIT,
JOURNAL_ID,
JOURNAL_DATE,
JOURNAL_LINE,
ACCOUNT_NBR,
DEPT_ID,
STATE,
POOL,
REINSURANCE_CD,
PROJECT_ID,
POLICY_NBR,
SUM_AMOUNT,
SRC,
REF_NBR,
BUSINESS_UNIT_GL,
AFFILIATE
)
SELECT
EXPTRANS.out_ledger as LEDGER,
EXPTRANS.out_unit as BUSINESS_UNIT,
EXPTRANS.out_journal_ID as JOURNAL_ID,
EXPTRANS.o_Date as JOURNAL_DATE,
EXPTRANS.out_Line_number as JOURNAL_LINE,
EXPTRANS.out_account as ACCOUNT_NBR,
EXPTRANS.out_dept as DEPT_ID,
EXPTRANS.out_state as STATE,
EXPTRANS.out_pool_ind as POOL,
EXPTRANS.out_reins_cd as REINSURANCE_CD,
EXPTRANS.out_project as PROJECT_ID,
EXPTRANS.out_line_descr as POLICY_NBR,
EXPTRANS.o_Sum_Amount as SUM_AMOUNT,
EXPTRANS.out_source as SRC,
EXPTRANS.out_ref_no as REF_NBR,
EXPTRANS.out_unit1 as BUSINESS_UNIT_GL,
EXPTRANS.out_affiliate as AFFILIATE
FROM
EXPTRANS;


END; ';