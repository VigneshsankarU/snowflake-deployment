-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_SP_MONTHLY_CREATE_PARAM_FILE("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_ETL_PRCS_CTRL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ETL_PRCS_CTRL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LAST_RUN_DT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
ETL_PRCS_CTRL.SRC_EXTRACT_DT as LAST_RUN_DT 
FROM DB_T_PROD_CORE.ETL_PRCS_CTRL
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
DATEADD(''MM'', 1, SQ_ETL_PRCS_CTRL.LAST_RUN_DT) as NEXT_RUN_DT,
''[global]'' as out_global,
''$PMMergeSessParamFile=TRUE'' as out_sessionmerge,
''$CAL_START_MTH_ID='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) ) || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) as out_cal_start_mth_id,
''$CAL_END_MTH_ID='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) ) || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) as out_cal_end_mth_id,
''$CAL_START_DT='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) ) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ''01'' as out_cal_start_dt,
''$CAL_END_DT='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) ) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ( TO_CHAR ( DATE_PART(''DD'', TO_TIMESTAMP(LAST_DAY ( NEXT_RUN_DT ))) ) ) as out_cal_end_dt,
''$LOAD_START_DT='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) ) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ''01'' as out_load_start_dt,
''$LOAD_END_DT='' || ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) ) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ( TO_CHAR ( DATE_PART(''DD'', TO_TIMESTAMP(LAST_DAY ( NEXT_RUN_DT ))) ) ) as out_load_end_dt,
( TO_DATE ( DATE_PART(''YYYY'', TO_TIMESTAMP(NEXT_RUN_DT)) || ''-'' || ( LPAD ( TO_CHAR ( DATE_PART(''MM'', TO_TIMESTAMP(NEXT_RUN_DT)) ) , 2 , ''0'' ) ) || ''-'' || ( TO_CHAR ( DATE_PART(''DD'', TO_TIMESTAMP(LAST_DAY ( NEXT_RUN_DT ))) ) ) , ''YYYY-MM-DD'' ) ) as var_src_extract_dt,
SQ_ETL_PRCS_CTRL.source_record_id
FROM
SQ_ETL_PRCS_CTRL
);


-- Component NRMTRANS, Type NORMALIZER 
-- Component NRMTRANS, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE NRMTRANS AS
(
SELECT ''REC1'' AS REC_NO, param_item_in1 AS param_item, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC2'', param_item_in2, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC3'', param_item_in3, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC4'', param_item_in4, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC5'', param_item_in5, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC6'', param_item_in6, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC7'', param_item_in7, source_record_id FROM EXPTRANS
UNION ALL
SELECT ''REC8'', param_item_in8, source_record_id FROM EXPTRANS
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