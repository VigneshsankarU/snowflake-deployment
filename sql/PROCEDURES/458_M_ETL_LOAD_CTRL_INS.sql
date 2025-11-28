-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ETL_LOAD_CTRL_INS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE OVERLAP date;
PRCS_NM varchar;
FREQUENCY varchar;
BEGIN 

OVERLAP:=''1900-01-01''; 
PRCS_NM:='' ''; 
FREQUENCY:='' ''; 

-- PIPELINE START FOR 1

-- Component SQ_Shortcut_to_MEMBER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_MEMBER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as START_DTTM,
$2 as END_DTTM,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select * from (SELECT MAX(END_DTTM) as START_DTTM,  MAX(LAST_UPDATE_TS ) as END_DTTM 
FROM
 DB_T_STAG_MEMBXREF_PROD.MEMBER a join  DB_T_STAG_MEMBXREF_PROD.ETL_LOAD_CTRL B on 1=1 and  B.PRCS_NM =''FEDERATION'' AND B.STATUS =''IN PROGRESS'')a where START_DTTM IS NOT NULL
) SRC
)
);


-- Component exp_calc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_calc AS
(
SELECT
CASE WHEN ( SQ_Shortcut_to_MEMBER.START_DTTM IS NOT NULL ) THEN 
null --RAISE_ERROR(''Previous Load got failed and control table ETL_LOAD_CTRL not closed Properly. Please Close ETL_LOAD_CTRL table and rerun'') 
ELSE ''1'' END as out_ABORT,
SQ_Shortcut_to_MEMBER.source_record_id
FROM
SQ_Shortcut_to_MEMBER
);


-- Component flt_record, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE flt_record AS
(
SELECT
exp_calc.out_ABORT as out_ABORT,
exp_calc.source_record_id
FROM
exp_calc
WHERE exp_calc.out_ABORT <> ''1''
);


-- Component Shortcut_to_ETL_LOAD_CTRL, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.ETL_LOAD_CTRL
(
PRCS_NM
)
SELECT
flt_record.out_ABORT as PRCS_NM
FROM
flt_record;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_Shortcut_to_MEMBER1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_MEMBER1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as START_DTTM,
$2 as END_DTTM,
$3 as ID,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT try_to_timestamp( cast( cast(MAX(END_DTTM) as date) - :overlap as string)) as START_DTTM,  MAX(LAST_UPDATE_TS ) as END_DTTM , maX(Id)
FROM
 DB_T_STAG_MEMBXREF_PROD.MEMBER a join  DB_T_STAG_MEMBXREF_PROD.ETL_LOAD_CTRL B on 1=1 and  B.PRCS_NM =''FEDERATION'' AND B.STATUS =''SUCCEEDED''
) SRC
)
);


-- Component exp_calc1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_calc1 AS
(
SELECT
:PRCS_NM as out_PRCS_NM,
SQ_Shortcut_to_MEMBER1.START_DTTM as START_DTTM,
SQ_Shortcut_to_MEMBER1.END_DTTM as END_DTTM,
CURRENT_TIMESTAMP as out_LOAD_DTTM,
''IN PROGRESS'' as out_STATUS,
:FREQUENCY as out_FREQUENCY,
SQ_Shortcut_to_MEMBER1.ID + 1 as out_ID,
SQ_Shortcut_to_MEMBER1.source_record_id
FROM
SQ_Shortcut_to_MEMBER1
);


-- Component exp_setvariable, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_setvariable AS
(
SELECT
exp_calc1.out_PRCS_NM as PRCS_NM,
exp_calc1.START_DTTM as START_DTTM,
exp_calc1.END_DTTM as END_DTTM,
exp_calc1.out_LOAD_DTTM as LOAD_DTTM,
exp_calc1.out_STATUS as STATUS,
exp_calc1.out_FREQUENCY as FREQUENCY,
exp_calc1.out_ID as ID,
exp_calc1.out_ID as var_Id,
exp_calc1.source_record_id
FROM
exp_calc1
);


-- Component Shortcut_to_ETL_LOAD_CTRL1, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.ETL_LOAD_CTRL
(
ID,
PRCS_NM,
START_DTTM,
END_DTTM,
LOAD_DTTM,
STATUS,
FREQUENCY
)
SELECT
exp_setvariable.ID as ID,
exp_setvariable.PRCS_NM as PRCS_NM,
exp_setvariable.START_DTTM as START_DTTM,
exp_setvariable.END_DTTM as END_DTTM,
exp_setvariable.LOAD_DTTM as LOAD_DTTM,
exp_setvariable.STATUS as STATUS,
exp_setvariable.FREQUENCY as FREQUENCY
FROM
exp_setvariable;


-- PIPELINE END FOR 2

END; ';