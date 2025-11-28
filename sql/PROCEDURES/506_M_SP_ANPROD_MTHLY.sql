-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_SP_ANPROD_MTHLY("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

  CAL_START_DT varchar;
  PRCS_ID varchar;
  CAL_START_MTH_ID varchar;
  CAL_END_DT varchar;
BEGIN
   CAL_START_DT := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''CAL_START_DT'' LIMIT 1);
   PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''PRCS_ID'' LIMIT 1);
   CAL_START_MTH_ID := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''CAL_START_MTH_ID'' LIMIT 1);
   CAL_END_DT := (SELECT param_value FROM control_params WHERE run_id = run_id AND param_name = ''CAL_END_DT'' LIMIT 1);

-- Component SQ_SP_SRC1_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_SP_SRC1
(
SUBJECT_AREA varchar(30),
SP_NAME varchar(50),
PROCESS_IND varchar(1),
source_record_id number autoincrement start 1 increment 1
);


copy into @my_internal_stage/SQ_SP_SRC1_SRC from (select * from SQ_SP_SRC1_SRC)
header=true
overwrite=true;

-- Component SQ_SP_SRC1_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_SP_SRC1.SUBJECT_AREA as SUBJECT_AREA,
SQ_SP_SRC1.SP_NAME as SP_NAME,
SQ_SP_SRC1.PROCESS_IND as PROCESS_IND,
:PRCS_ID as PRCS_ID,
:CAL_START_MTH_ID as PROC_MOID,
:CAL_START_DT as STRT_DT,
:CAL_END_DT as END_DT,
CASE WHEN SQ_SP_SRC1.SUBJECT_AREA = ''HSB'' THEN ''call '' || SQ_SP_SRC1.SP_NAME || ''('' || CHR ( 39 ) || ltrim ( rtrim ( STRT_DT ) ) || CHR ( 39 ) || '','' || CHR ( 39 ) || ltrim ( rtrim ( END_DT ) ) || CHR ( 39 ) || '')'' ELSE ''call '' || SQ_SP_SRC1.SP_NAME || ''('' || CHR ( 39 ) || ''MONTH'' || CHR ( 39 ) || '','' || SUBSTR ( PROC_MOID , 5 , 2 ) || '','' || SUBSTR ( PROC_MOID , 1 , 4 ) || '','' || ''2'' || '')'' END as SP_CALL,
SQ_SP_SRC1.source_record_id
FROM
SQ_SP_SRC1
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
EXPTRANS.SUBJECT_AREA as SUBJECT_AREA,
EXPTRANS.SP_NAME as SP_NAME,
EXPTRANS.PROCESS_IND as PROCESS_IND,
EXPTRANS.PRCS_ID as PRCS_ID,
EXPTRANS.SP_CALL as SP_CALL,
EXPTRANS.source_record_id
FROM
EXPTRANS
WHERE EXPTRANS.PROCESS_IND = ''Y'' AND ( EXPTRANS.SUBJECT_AREA = ''ANPROD'' or EXPTRANS.SUBJECT_AREA = ''HSB'' )
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

copy into @my_internal_stage/SP_TGT from (select * from SP_TGT)
header=true
overwrite=true;

-- Component SP_TGT, Type EXPORT_DATA Exporting data
;


END; ';