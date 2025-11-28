-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_DZ_GL_EVENTSTAGING_BC_MAX_TIMESTAMP("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_GL_EventStaging_BC, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_GL_EventStaging_BC AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as UpdateTS_stg,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT MAX(UpdateTS_stg)

FROM DB_T_PROD_STAG.GL_EVENTSTAGING_BC
) SRC
)
);


-- Component EXP_PARM_DATE, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_PARM_DATE AS
(
SELECT
CASE WHEN SQ_GL_EventStaging_BC.UpdateTS_stg IS NULL THEN CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP) ELSE SQ_GL_EventStaging_BC.UpdateTS_stg END as UpdateTS,
''[global]'' || CHR ( 10 ) as GLOBAL,
''$PMMergeSessParamFile=TRUE'' || CHR ( 10 ) as SESION,
''$Max_UpdateTS_stg='' || CHR ( 39 ) || UpdateTS || CHR ( 39 ) as O_MaxUpdateTS_stg,
SQ_GL_EventStaging_BC.source_record_id
FROM
SQ_GL_EventStaging_BC
);


-- Component NRM_SRC_PARAM_VALUE, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE NRM_SRC_PARAM_VALUE AS
SELECT 
  GLOBAL AS PARAMETER,
  ''REC1'' AS REC_NO,
  source_record_id
FROM EXP_PARM_DATE

UNION ALL

SELECT 
  SESION AS PARAMETER,
  ''REC2'' AS REC_NO,
  source_record_id
FROM EXP_PARM_DATE

UNION ALL

SELECT 
  O_MaxUpdateTS_stg AS PARAMETER,
  ''REC3'' AS REC_NO,
  source_record_id
FROM EXP_PARM_DATE;



-- Component GL_EventStaging_BC_max_timestamp, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE GL_EventStaging_BC_max_timestamp AS
(
SELECT
NRM_SRC_PARAM_VALUE.PARAMETER as Max_Timestamp_param
FROM
NRM_SRC_PARAM_VALUE
);


-- Component GL_EventStaging_BC_max_timestamp, Type EXPORT_DATA Exporting data
COPY INTO @my_internal_stage/my_export_folder/GL_EventStaging_BC_max_timestamp_
FROM (SELECT * FROM GL_EventStaging_BC_max_timestamp)
HEADER = TRUE
OVERWRITE = TRUE;


END; ';