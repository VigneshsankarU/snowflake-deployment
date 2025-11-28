-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_CHECK_TRG_DATE("RUN_ID" VARCHAR)
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
FIELD4 varchar(25),
FIELD5 varchar(26),
FIELD6 decimal,
FIELD7 varchar(30),
FIELD8 varchar(1),
FIELD9 varchar(2),
FIELD10 decimal,
FIELD11 varchar(2),
DOLLAR_AMT integer,
FIELD13 varchar(2),
FIELD14 decimal,
FIELD15 varchar(2),
FIELD16 decimal,
FIELD17 varchar(2),
FIELD18 decimal,
FIELD19 varchar(2),
FIELD20 decimal,
FIELD21 varchar(2),
FIELD22 decimal,
FIELD23 varchar(2),
FIELD24 decimal,
FIELD25 varchar(2),
FIELD26 decimal,
FIELD27 varchar(2),
FIELD28 decimal,
FIELD29 varchar(2),
FIELD30 decimal,
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Shortcut_to_TRG_FED_MSTR_SRC, Type IMPORT_DATA Importing Data
;


-- Component exp_ECTL_VALUES, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ECTL_VALUES AS
(
SELECT
7 as out_ECTL_PARM_ID,
0 as out_ECTL_PRGM_ID,
SQ_Shortcut_to_TRG_FED_MSTR.source_record_id
FROM
SQ_Shortcut_to_TRG_FED_MSTR
);


-- Component LKP_ECTL_PRGM_PARM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ECTL_PRGM_PARM AS
(
SELECT
LKP.ECTL_PARAM_VALUE,
exp_ECTL_VALUES.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_ECTL_VALUES.source_record_id ORDER BY LKP.ECTL_PARAM_VALUE asc) RNK
FROM
exp_ECTL_VALUES
LEFT JOIN (
SELECT LTRIM(RTRIM(ECTL_PRGM_PARAM.ECTL_PARAM_VALUE))  as ECTL_PARAM_VALUE, ECTL_PRGM_PARAM.ECTL_PRGM_ID as ECTL_PRGM_ID, ECTL_PRGM_PARAM.ECTL_PARAM_ID as ECTL_PARAM_ID FROM DB_T_CTRL_PROD.ECTL_PRGM_PARAM
) LKP ON LKP.ECTL_PRGM_ID = exp_ECTL_VALUES.out_ECTL_PRGM_ID AND LKP.ECTL_PARAM_ID = exp_ECTL_VALUES.out_ECTL_PARM_ID
QUALIFY RNK = 1
);


-- Component exp_removing_spaces_in_Date, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_removing_spaces_in_Date AS
(
SELECT
SQ_Shortcut_to_TRG_FED_MSTR.FEED_DATE as FEED_DATE,
LTRIM ( RTRIM ( LKP_ECTL_PRGM_PARM.ECTL_PARAM_VALUE ) ) as out_ECTL_PARAM_VALUE,
SQ_Shortcut_to_TRG_FED_MSTR.source_record_id
FROM
SQ_Shortcut_to_TRG_FED_MSTR
INNER JOIN LKP_ECTL_PRGM_PARM ON SQ_Shortcut_to_TRG_FED_MSTR.source_record_id = LKP_ECTL_PRGM_PARM.source_record_id
);


-- Component exp_Date_conv, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Date_conv AS
(
SELECT
CASE WHEN is_date ( exp_removing_spaces_in_Date.FEED_DATE , ''YYYY-MM-DD'' ) THEN to_date ( exp_removing_spaces_in_Date.FEED_DATE , ''YYYY-MM-DD'' ) ELSE ''Invalid Feed date'' END as FEED_DATE,
CASE WHEN is_date ( exp_removing_spaces_in_Date.out_ECTL_PARAM_VALUE , ''YYYY-MM-DD'' ) THEN to_date ( exp_removing_spaces_in_Date.out_ECTL_PARAM_VALUE , ''YYYY-MM-DD'' ) ELSE ''Invalid ECTL FED date'' END as ECTL_PARAM_VALUE,
exp_removing_spaces_in_Date.source_record_id
FROM
exp_removing_spaces_in_Date
);


-- Component exp_Date_check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Date_check AS
(
SELECT
CASE WHEN exp_Date_conv.FEED_DATE <= exp_Date_conv.ECTL_PARAM_VALUE THEN ''THIS IS OLD FEDERATION FILE - ABORTING THE FEDERATION PROCESS'' ELSE $3 END as STATUS,
exp_Date_conv.source_record_id
FROM
exp_Date_conv
);


-- Component Dummy_TRG_FILE_TARGET1, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE Dummy_TRG_FILE_TARGET1 AS
(
SELECT
exp_Date_check.STATUS as DUMMY
FROM
exp_Date_check
);


-- Component Dummy_TRG_FILE_TARGET1, Type EXPORT_DATA Exporting data
;


END; ';