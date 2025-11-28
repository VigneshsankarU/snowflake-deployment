-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_SP_ACCTBLY_DTL("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
PRCS_ID varchar;

BEGIN
   PRCS_ID := (SELECT value FROM control_params WHERE run_id = run_id AND param_name = ''PRCS_ID''   LIMIT 1);



-- PIPELINE START FOR 1

-- Component SQ_SP_SRC1_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_SP_SRC1
(
SUBJECT_AREA varchar(30),
SP_NAME varchar(250),
PROCESS_IND varchar(1),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_SP_SRC1_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_SP_SRC1.SP_NAME as SP_NAME,
''status'' as v_PROCESS_IND,
:PRCS_ID as PRCS_ID,
DATE_PART(''Y'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) as v_year,
DATE_PART(''MM'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) as v_month,
''call '' || SQ_SP_SRC1.SP_NAME || ''('' || v_year || '','' || v_month || '','' || v_PROCESS_IND || '')'' as SP_CALL,
SQ_SP_SRC1.source_record_id
FROM
SQ_SP_SRC1
);


// ******** No handler defined for type SQL_TRANSFORM, node SQL *******


-- Component SP_TGT, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE SP_TGT AS
(
SELECT
SQL.PRCS_ID_output as PRCS_ID,
SQL.SP_NAME_output as SP_NAME,
SQL.SQLError as SP_ERROR
FROM
SQL
);


-- Component SP_TGT, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_SP_TGT_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_SP_TGT
(
PRCS_ID bigint,
SP_NAME varchar(50),
SP_ERROR varchar(4096),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_SP_TGT_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
CASE WHEN SQ_SP_TGT.SP_ERROR IS NOT NULL THEN RAISE_ERROR(''Stored procedure/s failed,please check mail for failed list file'') ELSE $3 END as ABORT,
SQ_SP_TGT.source_record_id
FROM
SQ_SP_TGT
);


-- Component SP_TGT2, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE SP_TGT2 AS
(
SELECT
EXPTRANS1.ABORT as SP_ERROR
FROM
EXPTRANS1
);


-- Component SP_TGT2, Type EXPORT_DATA Exporting data
;


-- PIPELINE END FOR 2

END; ';