-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_FED_CREATE_PARAM_FILE("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_ETL_LOAD_CTRL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ETL_LOAD_CTRL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as START_DTTM,
$2 as END_DTTM,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  max(ETL_LOAD_CTRL.START_DTTM) START_DTTM,
 max(ETL_LOAD_CTRL.END_DTTM) END_DTTM
FROM
 DB_T_STAG_MEMBXREF_PROD.ETL_LOAD_CTRL
where prcs_nm =''FEDERATION'' and status =''IN PROGRESS''
) SRC
)
);


-- Component exp_hold_param_value, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_hold_param_value AS
(
SELECT
''[Global]'' as Global,
''$federation_start_dttm='' || chr ( 39 ) || to_char ( substr ( to_char ( SQ_ETL_LOAD_CTRL.START_DTTM , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) , 1 , 26 ) ) || chr ( 39 ) as out_START_DTTM,
''$federation_end_dttm='' || chr ( 39 ) || to_char ( substr ( to_char ( SQ_ETL_LOAD_CTRL.END_DTTM , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) , 1 , 26 ) ) || chr ( 39 ) as out_END_DTTM,
SQ_ETL_LOAD_CTRL.source_record_id
FROM
SQ_ETL_LOAD_CTRL
);


-- Component nrml_row_column, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE nrml_row_column AS
(
SELECT  * FROM
( /* start of inner SQL */
SELECT
exp_hold_param_value.Global as Column_in1,
exp_hold_param_value.out_START_DTTM as Column_in2,
exp_hold_param_value.out_END_DTTM as Column_in3,
exp_hold_param_value.source_record_id,
null "Column"
FROM
exp_hold_param_value
/* end of inner SQL */
)
 --UNPIVOT(Column) FOR REC_NO IN (Column_in1 AS REC1, Column_in2 AS REC2, Column_in3 AS REC3) UNPIVOT_TBL
);


-- Component Shortcut_to_Fed_Param_file, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE Shortcut_to_Fed_Param_file AS
(
SELECT
nrml_row_column."Column" as Param_value
FROM
nrml_row_column
);

copy into @my_internal_stage/Shortcut_to_Fed_Param_file from (select * from Shortcut_to_Fed_Param_file)
header=true
overwrite=true;

-- Component Shortcut_to_Fed_Param_file, Type EXPORT_DATA Exporting data
;


END; ';