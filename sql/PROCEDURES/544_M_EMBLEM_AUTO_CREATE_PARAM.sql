-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_EMBLEM_AUTO_CREATE_PARAM("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
wf_MONTH_ID varchar;
wf_CLM_LOSS_DT date;
wf_LOSS_TRS_DT date;
BEGIN 
set wf_MONTH_ID:=''1'';
set wf_CLM_LOSS_DT:=current_date();
set wf_LOSS_TRS_DT:=current_date();
-- Component sq_ECTL_MISPREM_HEADER, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_ECTL_MISPREM_HEADER AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as YEAR_ID,
$2 as MONTH_ID,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT extract( year from (cast(TO_DATE as date)) ) as YEAR_ID,extract (month from (cast(TO_DATE as date)) )as MONTH_ID 

FROM DB_T_CTRL_PROD.ECTL_MISPREM_HEADER where cast(cyc_end_dt as date) = (select max(cast(cyc_end_Dt as date)) from DB_T_CTRL_PROD.ECTL_MISPREM_HEADER)
) SRC
)
);


-- Component exp_CREATE_PARAM_FILE, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CREATE_PARAM_FILE AS
(
SELECT
( sq_ECTL_MISPREM_HEADER.YEAR_ID * 100 + sq_ECTL_MISPREM_HEADER.MONTH_ID ) as var_mth_id,
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_ECTL_MISPREM_HEADER.YEAR_ID * 10000 ) + ( sq_ECTL_MISPREM_HEADER.MONTH_ID * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_Last_day,
''[Global]'' as out_CONNECTION_param1,
'':wf_MONTH_ID='' || TO_CHAR ( var_mth_id ) as out_MONTH_ID_param2,
'':wf_CLM_LOSS_DT='' || TO_CHAR ( var_Last_day , ''YYYY-MM-DD'' ) as out_CLM_LOSS_DT_param2,
'':wf_LOSS_TRS_DT='' || TO_CHAR ( var_Last_day , ''YYYY-MM-DD'' ) as out_LOSS_TRS_DT_param2,
sq_ECTL_MISPREM_HEADER.source_record_id
FROM
sq_ECTL_MISPREM_HEADER
);

CREATE
OR REPLACE TEMPORARY TABLE nrm_COLUMNS_TO_RECORDS AS
SELECT
  *
FROM
  (
    SELECT
      exp_CREATE_PARAM_FILE.out_CONNECTION_param1 AS WRITE_RECORD_in1,
      exp_CREATE_PARAM_FILE.out_MONTH_ID_param2 AS WRITE_RECORD_in2,
      exp_CREATE_PARAM_FILE.out_CLM_LOSS_DT_param2 AS WRITE_RECORD_in3,
      exp_CREATE_PARAM_FILE.out_LOSS_TRS_DT_param2 AS WRITE_RECORD_in4,
      exp_CREATE_PARAM_FILE.source_record_id
    FROM
      exp_CREATE_PARAM_FILE
  ) UNPIVOT (
    WRITE_RECORD FOR REC_NO IN (
      WRITE_RECORD_in1,
      WRITE_RECORD_in2,
      WRITE_RECORD_in3,
      WRITE_RECORD_in4
    )
  );

-- Component AGG_LR_PARAM_INS, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE AGG_LR_PARAM_INS AS
(
SELECT
nrm_COLUMNS_TO_RECORDS.WRITE_RECORD as OUTPUT
FROM
nrm_COLUMNS_TO_RECORDS
);

COPY INTO @public.edw_stage/my_export_folder/AGG_LR_PARAM_INS_
FROM (SELECT * FROM AGG_LR_PARAM_INS)
HEADER = TRUE
OVERWRITE = TRUE;
-- Component AGG_LR_PARAM_INS, Type EXPORT_DATA Exporting data
;


END; ';