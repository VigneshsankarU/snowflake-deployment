-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_SP_LR_DAILY_CREATE_PARAM_FILE("PARAM_JSON" VARCHAR)
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
SELECT MAX(JOB_END_DT) as LOAD_DT FROM DB_T_PROD_CORE.ETL_PRCS_CTRL WHERE PRCS_STS = ''SUCCEEDED'' 

AND PRCS_STAG = ''EDW_DLY'' AND SRC_NM = ''GW'' AND SUB_PRCS_NM=''S_M_BASE_EDW_LOAD_COMPLETION_DAILY''
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
''$LOAD_END_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.LAST_RUN_DT , ''YYYY-MM-DD'' ) as out_load_end_dt,
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
SELECT source_record_id, ''REC3'' AS REC_NO, out_load_end_dt AS param_item FROM EXPTRANS
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