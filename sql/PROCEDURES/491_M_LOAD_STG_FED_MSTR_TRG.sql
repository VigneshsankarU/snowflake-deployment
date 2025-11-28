-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_STG_FED_MSTR_TRG("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Shortcut_to_TRG_FED_MSTR_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_TRG_FED_MSTR
(
FEED_IND varchar(1),
FEED_DATE varchar(10),
RECORD_COUNT integer,
DOLLAR_AMT integer,
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Shortcut_to_TRG_FED_MSTR_SRC, Type IMPORT_DATA Importing Data
--;


-- Component exp_convert_date, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_convert_date AS
(
SELECT
--CASE WHEN is_date ( SQ_Shortcut_to_TRG_FED_MSTR.FEED_DATE , ''YYYY-MM-DD'' ) THEN to_date ( SQ_Shortcut_to_TRG_FED_MSTR.FEED_DATE , ''YYYY-MM-DD'' ) ELSE RAISE_ERROR(''Invalid Feed date'') END as FEED_DATE,
try_to_date( SQ_Shortcut_to_TRG_FED_MSTR.FEED_DATE , ''YYYY-MM-DD'' ) as FEED_DATE,
SQ_Shortcut_to_TRG_FED_MSTR.source_record_id
FROM
SQ_Shortcut_to_TRG_FED_MSTR
);



-- Component STG_FED_MSTR_TRG, Type TARGET 
INSERT INTO DB_T_STAG_PROD.STG_FED_MSTR_TRG
(
FEED_IND,
FEED_DT,
REC_COUNT,
DOLLAR_AMT
)
SELECT
SQ_Shortcut_to_TRG_FED_MSTR.FEED_IND as FEED_IND,
exp_convert_date.FEED_DATE as FEED_DT,
SQ_Shortcut_to_TRG_FED_MSTR.RECORD_COUNT as REC_COUNT,
SQ_Shortcut_to_TRG_FED_MSTR.DOLLAR_AMT as DOLLAR_AMT
FROM
SQ_Shortcut_to_TRG_FED_MSTR
INNER JOIN exp_convert_date ON SQ_Shortcut_to_TRG_FED_MSTR.source_record_id = exp_convert_date.source_record_id;


END; ';