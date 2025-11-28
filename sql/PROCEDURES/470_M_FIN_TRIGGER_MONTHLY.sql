-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_FIN_TRIGGER_MONTHLY("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 
FEED_IND varchar; 
BEGIN 
set FEED_IND = ''Y'';
-- Component SQ_Shortcut_to_Dummy_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_Dummy
(
DUMMY varchar(19),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Shortcut_to_Dummy_SRC, Type IMPORT_DATA Importing Data
;


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
:FEED_IND as feed_ind,
TO_CHAR (
  LAST_DAY (DATEADD (MONTH, -1, CURRENT_TIMESTAMP())),
  ''YYYY-MM-DD''
) AS feed_dt,
SQ_Shortcut_to_Dummy.source_record_id
FROM
SQ_Shortcut_to_Dummy
);


-- Component TRG_STAG_PS_LOOKUP, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE TRG_STAG_PS_LOOKUP AS
(
SELECT
exp.feed_ind as feed_ind,
exp.feed_dt as feed_dt
FROM
exp
);


-- Component TRG_STAG_PS_LOOKUP, Type EXPORT_DATA Exporting data
;
COPY INTO @my_internal_stage/my_export_folder/TRG_STAG_PS_LOOKUP_
FROM (SELECT * FROM TRG_STAG_PS_LOOKUP)
HEADER = TRUE
OVERWRITE = TRUE;

END; ';