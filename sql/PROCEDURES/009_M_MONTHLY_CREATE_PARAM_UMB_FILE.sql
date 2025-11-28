-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_MONTHLY_CREATE_PARAM_UMB_FILE("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  PMWorkflowName STRING;
  PMSessionName STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):PMWorkflowName::STRING,
    ''s_m_monthly_create_param_umb_file''
  INTO
    PMWorkflowName,
    PMSessionName;

-- Component SQ_ETL_PRCS_CTRL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ETL_PRCS_CTRL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LAST_RUN_DT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT case when max(LAST_RUN_DT) is null then last_day(add_months(current_date,-1)) 

else max(LAST_RUN_DT) end as LAST_RUN_DT 

from

(SELECT DISTINCT JOB_START_DT AS LAST_RUN_DT

FROM DB_T_PROD_CORE.ETL_PRCS_CTRL where  SUB_PRCS_NM = ''S_M_MONTHLY_CREATE_PARAM_UMB_FILE''

and PRCS_NM = ''WF_CREATE_PARAM_FILE_UMB_MTHLY''and PRCS_STS = ''SUCCEEDED''

qualify row_number() over(partition by SUB_PRCS_NM,PRCS_NM order by job_start_dt desc )=1

)a
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
''[global]'' as out_global,
''$PMMergeSessParamFile=TRUE'' as out_sessionmerge,
''$CAL_START_MTH_ID='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) , 2 , ''0'' ) ) as out_cal_start_mth_id,
''$CAL_END_MTH_ID='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) , 2 , ''0'' ) ) as out_cal_end_mth_id,
''$CAL_START_DT='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ''01'' as out_cal_start_dt,
''$CAL_END_DT='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(SQ_ETL_PRCS_CTRL.LAST_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ( TO_CHAR ( DATE_PART(''DD'', TO_TIMESTAMP(LAST_DAY ( SQ_ETL_PRCS_CTRL.LAST_RUN_DT ))) ) ) as out_cal_end_dt,
SQ_ETL_PRCS_CTRL.source_record_id
FROM
SQ_ETL_PRCS_CTRL
);


-- Component NRMTRANS, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE NRMTRANS AS
(
SELECT source_record_id, ''REC1'' AS REC_NO, out_global AS param_item FROM EXPTRANS
UNION ALL
SELECT source_record_id, ''REC2'' AS REC_NO, out_sessionmerge AS param_item FROM EXPTRANS
UNION ALL
SELECT source_record_id, ''REC3'' AS REC_NO, out_cal_start_mth_id AS param_item FROM EXPTRANS
UNION ALL
SELECT source_record_id, ''REC4'' AS REC_NO, out_cal_end_mth_id AS param_item FROM EXPTRANS
UNION ALL
SELECT source_record_id, ''REC5'' AS REC_NO, out_cal_start_dt AS param_item FROM EXPTRANS
UNION ALL
SELECT source_record_id, ''REC6'' AS REC_NO, out_cal_end_dt AS param_item FROM EXPTRANS
);


-- Component tgt_edw_shared_append_file, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tgt_edw_shared_append_file AS
(
SELECT
NRMTRANS.param_item as Param_item
FROM
NRMTRANS
);


-- Component tgt_edw_shared_append_file, Type EXPORT_DATA Exporting data
;


END; ';