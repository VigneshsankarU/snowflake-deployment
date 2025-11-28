-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CREATE_PARAMETER_FILE("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
       run_id STRING;
       PRCS_ID STRING;
       --start_dttm timestamp;
       stag_dbname STRING ;
   --    v_lineNo int;
       v_global_position int;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
	   --START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
	   --END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
       stag_dbname := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''STAG_DBNAME'' LIMIT 1);
  --      v_lineNo := 0;
        v_global_position := 1;
-- PIPELINE START FOR 1

-- Component SQ_edw_param_base_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_edw_param_base
(
ParameterLine varchar(100000),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_edw_param_base_SRC, Type IMPORT_DATA Importing Data
copy into SQ_edw_param_base
(ParameterLine)
from 
(select $1 
from @edw_stage/Parameter/edw_base/edw_param_base_static.txt)
file_format = ''PARAM_FILE_FORMAT'';


-- Component exp_CR_lineNo, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CR_lineNo AS
(
SELECT
regexp_replace(SQ_edw_param_base.ParameterLine,Chr ( 10 ),'''',1,0,''i'') as o_ParameterLine,
--:v_lineNo + 1 as v_lineNo,
--v_lineNo as o_lineNo,
SQ_edw_param_base.source_record_id as o_lineNo,
SQ_edw_param_base.source_record_id
FROM
SQ_edw_param_base
);


-- Component filtr_global_start_end_only, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE filtr_global_start_end_only AS
(
SELECT
exp_CR_lineNo.o_ParameterLine as ParameterLine,
exp_CR_lineNo.o_lineNo as o_lineNo,
exp_CR_lineNo.source_record_id
FROM
exp_CR_lineNo
WHERE POSITION(''[global]'',exp_CR_lineNo.o_ParameterLine) <> 0 OR ( POSITION(''['',exp_CR_lineNo.o_ParameterLine) <> 0 AND POSITION(''='',exp_CR_lineNo.o_ParameterLine) = 0 )
);


-- Component exp_filtering_global_positions, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_filtering_global_positions AS
(
SELECT
filtr_global_start_end_only.ParameterLine as ParameterLine,
filtr_global_start_end_only.o_lineNo as o_lineNo,
CASE WHEN :v_global_position > 0 THEN :v_global_position ELSE CASE WHEN POSITION(''[global]'',filtr_global_start_end_only.ParameterLine) <> 0 THEN filtr_global_start_end_only.o_lineNo ELSE :v_global_position END END as v_global_position,
CASE WHEN v_global_position = 0 THEN 1 ELSE 0 END as o_global_check,
filtr_global_start_end_only.source_record_id
FROM
filtr_global_start_end_only
);


-- Component filtr_global_section, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE filtr_global_section AS
(
SELECT
exp_filtering_global_positions.ParameterLine as ParameterLine,
exp_filtering_global_positions.o_lineNo as o_lineNo,
exp_filtering_global_positions.o_global_check as o_global_check,
exp_filtering_global_positions.source_record_id
FROM
exp_filtering_global_positions
WHERE exp_filtering_global_positions.o_global_check = 0
);


-- Component tgt_global_lineNo, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tgt_global_lineNo AS
(
SELECT
filtr_global_section.ParameterLine as ParamLine,
filtr_global_section.o_lineNo as LineNo
FROM
filtr_global_section
);


-- Component tgt_global_lineNo, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/tgt_global_lineNo.txt
from 
(select ParamLine, LineNo
from tgt_global_lineNo)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE
;



-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_edw_param_base1_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_edw_param_base1
(
ParameterLine varchar(100000),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_edw_param_base1_SRC, Type IMPORT_DATA Importing Data
copy into SQ_edw_param_base1
(ParameterLine)
from 
(select $1 
from @edw_stage/Parameter/edw_base/edw_param_base_static.txt)
file_format = ''PARAM_FILE_FORMAT'';
;


-- Component exp_line_no, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_line_no AS
(
SELECT
SQ_edw_param_base1.ParameterLine as ParameterLine,
--:v_lineNo + 1 as v_lineNo,
--v_lineNo as o_lineNo,
source_record_id as o_lineNo,
1 as dummy,
SQ_edw_param_base1.source_record_id
FROM
SQ_edw_param_base1
);


-- PIPELINE START FOR 2

-- Component SQ_edw_param_base2_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_edw_param_base2
(
ParameterLine varchar(100000),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_edw_param_base2_SRC, Type IMPORT_DATA Importing Data
copy into SQ_edw_param_base2
(ParameterLine)
from 
(select $1 
from @edw_stage/Parameter/edw_base/edw_param_base_static.txt)
file_format = ''PARAM_FILE_FORMAT'';
;


-- Component FIL_StartDateOnly, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FIL_StartDateOnly AS
(
SELECT
exp_line_no.ParameterLine as ParameterLine,
exp_line_no.o_lineNo as o_lineNo,
exp_line_no.dummy as dummy,
exp_line_no.source_record_id
FROM
exp_line_no
WHERE POSITION(''$start_dttm'',exp_line_no.ParameterLine) <> 0
);


-- PIPELINE START FOR 2

-- Component SQ_global_lineno_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_global_lineno
(
ParamName varchar(10000),
LineNo decimal,
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_global_lineno_SRC, Type IMPORT_DATA Importing Data
copy into SQ_global_lineno
(ParamName,
LineNo)
from 
(select $1 , $2
from @edw_stage/Parameter/edw_base/tgt_global_lineNo.txt)
file_format = ''CSV_FORMAT'';



-- PIPELINE START FOR 2

-- Component sq_edw_param_base3_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE sq_edw_param_base3
(
ParameterLine varchar(100000),
source_record_id number autoincrement start 1 increment 1
);


-- Component sq_edw_param_base3_SRC, Type IMPORT_DATA Importing Data
copy into sq_edw_param_base3
(ParameterLine)
from 
(select $1 
from @edw_stage/Parameter/edw_base/edw_param_base_static.txt)
file_format = ''PARAM_FILE_FORMAT'';
;


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_global_lineno.ParamName as ParamName,
SQ_global_lineno.LineNo as LineNo,
1 as dummy,
SQ_global_lineno.source_record_id
FROM
SQ_global_lineno
);


-- Component exp_remove_CR_add_lineNo, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_remove_CR_add_lineNo AS
(
SELECT
1 as dummy,
regexp_replace(SQ_edw_param_base2.ParameterLine,Chr ( 10 ),'''',1,0,''i'') as o_ParameterLine,
--:v_lineNo + 1 as v_lineNo,
--v_lineNo as o_lineNo,
source_record_id as o_lineNo,
SQ_edw_param_base2.source_record_id
FROM
SQ_edw_param_base2
);


-- Component exp_line_no1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_line_no1 AS
(
SELECT
sq_edw_param_base3.ParameterLine as ParameterLine,
--:v_lineNo + 1 as v_lineNo,
--v_lineNo as o_lineNo,
source_record_id as o_lineNo,
1 as dummy,
sq_edw_param_base3.source_record_id
FROM
sq_edw_param_base3
);


-- Component agg_line, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE agg_line AS
(
SELECT
MIN(exp_pass_through.ParamName) as ParamName,
MIN(exp_pass_through.LineNo) as LineNo,
MIN(exp_pass_through.dummy) as dummy,
min(LineNo) as minL,
max(LineNo) as maxL,
MIN(exp_pass_through.source_record_id) as source_record_id
FROM
exp_pass_through
);


-- Component FIL_EndDateOnly, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FIL_EndDateOnly AS
(
SELECT
exp_line_no1.ParameterLine as ParameterLine,
exp_line_no1.o_lineNo as o_lineNo,
exp_line_no1.dummy as dummy,
exp_line_no1.source_record_id
FROM
exp_line_no1
WHERE POSITION(''$end_dttm'',exp_line_no1.ParameterLine) <> 0
);


-- Component jnr_preTgt_temp_file, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE jnr_preTgt_temp_file AS
(
SELECT
exp_remove_CR_add_lineNo.dummy as dummy,
exp_remove_CR_add_lineNo.o_ParameterLine as o_ParameterLine,
exp_remove_CR_add_lineNo.o_lineNo as o_lineNo,
agg_line.dummy as dummy1,
agg_line.minL as minL,
agg_line.maxL as maxL,
row_number() over (order by 1) AS source_record_id
FROM
exp_remove_CR_add_lineNo
INNER JOIN agg_line ON agg_line.dummy = exp_remove_CR_add_lineNo.dummy
);


-- Component jnr_GlobalArea1, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE jnr_GlobalArea1 AS
(
SELECT
FIL_StartDateOnly.ParameterLine as ParameterLine,
FIL_StartDateOnly.o_lineNo as o_lineNo,
FIL_StartDateOnly.dummy as dummy1,
agg_line.dummy as dummy,
agg_line.minL as minL,
agg_line.maxL as maxL,
row_number() over (order by 1) AS source_record_id
FROM
FIL_StartDateOnly
LEFT OUTER JOIN agg_line ON agg_line.dummy = FIL_StartDateOnly.dummy
);


-- Component jnr_globalArea, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE jnr_globalArea AS
(
SELECT
FIL_EndDateOnly.ParameterLine as ParameterLine,
FIL_EndDateOnly.o_lineNo as o_lineNo,
FIL_EndDateOnly.dummy as dummy,
agg_line.dummy as dummy1,
agg_line.minL as minL,
agg_line.maxL as maxL,
row_number() over (order by 1) AS source_record_id
FROM
agg_line
INNER JOIN FIL_EndDateOnly ON agg_line.dummy = FIL_EndDateOnly.dummy
);


-- Component EXP_Extract_StrDtQuery, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_Extract_StrDtQuery AS
(
SELECT
jnr_GlobalArea1.ParameterLine as ParameterLine,
SUBSTR ( LTRIM ( RTRIM ( jnr_GlobalArea1.ParameterLine ) ) , POSITION(''='',jnr_GlobalArea1.ParameterLine) + 1 , LENGTH ( LTRIM ( RTRIM ( jnr_GlobalArea1.ParameterLine ) ) ) ) as v_sql_Query,
REPLACE(v_sql_Query,''$stag_dbname'',:stag_dbname) as sql_Query,
CASE WHEN jnr_GlobalArea1.o_lineNo > jnr_GlobalArea1.minL and jnr_GlobalArea1.o_lineNo < jnr_GlobalArea1.maxL THEN 0 ELSE CASE WHEN jnr_GlobalArea1.minL <> jnr_GlobalArea1.maxL and ( jnr_GlobalArea1.o_lineNo > jnr_GlobalArea1.maxL OR jnr_GlobalArea1.o_lineNo < jnr_GlobalArea1.minL ) THEN 1 ELSE CASE WHEN jnr_GlobalArea1.minL = jnr_GlobalArea1.maxL and jnr_GlobalArea1.o_lineNo < jnr_GlobalArea1.minL THEN 1 ELSE 0 END END END as o_flag,
jnr_GlobalArea1.source_record_id
FROM
jnr_GlobalArea1
);


-- Component EXP_Extract_EndDtQuery, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_Extract_EndDtQuery AS
(
SELECT
jnr_globalArea.ParameterLine as ParameterLine,
SUBSTR ( LTRIM ( RTRIM ( jnr_globalArea.ParameterLine ) ) , POSITION(''='',jnr_globalArea.ParameterLine) + 1 , LENGTH ( LTRIM ( RTRIM ( jnr_globalArea.ParameterLine ) ) ) ) as v_sql_Query,
REPLACE(v_sql_Query,''$stag_dbname'',:stag_dbname) as sql_Query,
CASE WHEN jnr_globalArea.o_lineNo > jnr_globalArea.minL and jnr_globalArea.o_lineNo < jnr_globalArea.maxL THEN 0 ELSE CASE WHEN jnr_globalArea.minL <> jnr_globalArea.maxL and ( jnr_globalArea.o_lineNo > jnr_globalArea.maxL OR jnr_globalArea.o_lineNo < jnr_globalArea.minL ) THEN 1 ELSE CASE WHEN jnr_globalArea.minL = jnr_globalArea.maxL and jnr_globalArea.o_lineNo < jnr_globalArea.minL THEN 1 ELSE 0 END END END as o_flag,
jnr_globalArea.source_record_id
FROM
jnr_globalArea
);


-- Component fil_only_global_parameters1, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_only_global_parameters1 AS
(
SELECT
EXP_Extract_EndDtQuery.ParameterLine as ParameterLine,
EXP_Extract_EndDtQuery.sql_Query as sql_Query,
EXP_Extract_EndDtQuery.o_flag as o_flag,
EXP_Extract_EndDtQuery.source_record_id
FROM
EXP_Extract_EndDtQuery
WHERE EXP_Extract_EndDtQuery.o_flag = 0
);


-- Component fil_only_global_parameters, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_only_global_parameters AS
(
SELECT
EXP_Extract_StrDtQuery.ParameterLine as ParameterLine,
EXP_Extract_StrDtQuery.sql_Query as sql_Query,
EXP_Extract_StrDtQuery.o_flag as o_flag,
EXP_Extract_StrDtQuery.source_record_id
FROM
EXP_Extract_StrDtQuery
WHERE EXP_Extract_StrDtQuery.o_flag = 0
);


/* No handler defined for type SQL_TRANSFORM, node SQL_EndDate1 */
CREATE OR REPLACE TEMPORARY TABLE SQL_EndDate1 as
(
    SELECT end_dttm ,
    row_number() over (order by 1) AS source_record_id
    FROM db_t_prod_stag.gw_etl_prcs_ctrl
);


/* No handler defined for type SQL_TRANSFORM, node SQL_EndDate */
CREATE OR REPLACE TEMPORARY TABLE SQL_EndDate as
(
SELECT (start_dttm - INTERVAL ''3 hour'') AS start_date ,
row_number() over (order by 1) AS source_record_id
FROM db_t_prod_stag.gw_etl_prcs_ctrl
);



-- Component EXP_Passthrough_EndDt1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_Passthrough_EndDt1 AS
(
SELECT
1 as dummy,
chr ( 39 ) || TO_CHAR ( SQL_EndDate1.end_dttm , ''YYYY-MM-DD HH24:MI:SS.US'' ) || chr ( 39 ) as o_END_DTTM,
SQL_EndDate1.source_record_id
FROM
SQL_EndDate1
);


-- Component EXP_Passthrough_StartDt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_Passthrough_StartDt AS
(
SELECT
1 as dummy,
chr ( 39 ) || TO_CHAR ( SQL_EndDate.Start_date , ''YYYY-MM-DD HH24:MI:SS.US'' ) || chr ( 39 ) as o_START_DTTM,
SQL_EndDate.source_record_id
FROM
SQL_EndDate
);


-- Component JNR_Start_End_Date, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE JNR_Start_End_Date AS
(
SELECT
EXP_Passthrough_StartDt.dummy as dummy1,
EXP_Passthrough_StartDt.o_START_DTTM as Startdate,
EXP_Passthrough_EndDt1.dummy as dummy,
EXP_Passthrough_EndDt1.o_END_DTTM as EndDate,
row_number() over (order by 1) AS source_record_id
FROM
EXP_Passthrough_EndDt1
INNER JOIN EXP_Passthrough_StartDt ON EXP_Passthrough_StartDt.dummy = EXP_Passthrough_EndDt1.dummy
);


-- Component jnr_source_SQL_global_lineNo, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE jnr_source_SQL_global_lineNo AS
(
SELECT
jnr_preTgt_temp_file.dummy as dummy,
jnr_preTgt_temp_file.o_ParameterLine as ParameterLine,
jnr_preTgt_temp_file.o_lineNo as o_lineNo,
JNR_Start_End_Date.dummy1 as dummy1,
JNR_Start_End_Date.Startdate as Startdate,
JNR_Start_End_Date.EndDate as EndDate,
jnr_preTgt_temp_file.minL as minL,
jnr_preTgt_temp_file.maxL as maxL,
row_number() over (order by 1) AS source_record_id
FROM
jnr_preTgt_temp_file
INNER JOIN JNR_Start_End_Date ON JNR_Start_End_Date.dummy1 = jnr_preTgt_temp_file.dummy
);


-- Component exp_replace_Values, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_replace_Values AS
(
SELECT
CASE WHEN jnr_source_SQL_global_lineNo.o_lineNo > jnr_source_SQL_global_lineNo.minL and jnr_source_SQL_global_lineNo.o_lineNo < jnr_source_SQL_global_lineNo.maxL THEN 0 ELSE CASE WHEN jnr_source_SQL_global_lineNo.minL <> jnr_source_SQL_global_lineNo.maxL and ( jnr_source_SQL_global_lineNo.o_lineNo > jnr_source_SQL_global_lineNo.maxL OR jnr_source_SQL_global_lineNo.o_lineNo < jnr_source_SQL_global_lineNo.minL ) THEN 1 ELSE CASE WHEN jnr_source_SQL_global_lineNo.minL = jnr_source_SQL_global_lineNo.maxL and jnr_source_SQL_global_lineNo.o_lineNo < jnr_source_SQL_global_lineNo.minL THEN 1 ELSE 0 END END END as flag,
CASE WHEN POSITION(''$start_dttm'',jnr_source_SQL_global_lineNo.ParameterLine) <> 0 and flag = 0 THEN ''$start_dttm = '' || jnr_source_SQL_global_lineNo.Startdate ELSE CASE WHEN POSITION(''$end_dttm'',jnr_source_SQL_global_lineNo.ParameterLine) <> 0 and flag = 0 THEN ''$end_dttm = '' || jnr_source_SQL_global_lineNo.EndDate ELSE jnr_source_SQL_global_lineNo.ParameterLine END END as o_ParameterName,
jnr_source_SQL_global_lineNo.source_record_id
FROM
jnr_source_SQL_global_lineNo
);


-- Component tgt_edw_param_stag, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tgt_edw_param_stag AS
(
SELECT
exp_replace_Values.o_ParameterName as ParameterLine
FROM
exp_replace_Values
);


-- Component tgt_edw_param_stag, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/tgt_edw_param_stag.txt
from 
(select ParameterLine
from tgt_edw_param_stag)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;



END; ';