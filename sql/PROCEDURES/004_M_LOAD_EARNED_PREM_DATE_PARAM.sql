-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_EARNED_PREM_DATE_PARAM("PARAM_JSON" VARCHAR)
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
    ''s_m_load_earned_prem_date_param''
  INTO
    PMWorkflowName,
    PMSessionName;

-- Component SQ_GW_CLOSEOUT_CTL1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_GW_CLOSEOUT_CTL1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ENDDATE,
$2 as STARTDATE,
$3 as EOM_DT,
$4 as source_record_id
FROM (
SELECT 
  SRC.*, 
  ROW_NUMBER() OVER (ORDER BY ENDDATE) AS source_record_id
FROM (
  SELECT DISTINCT
    DATEADD(day, -1, prog_date) AS ENDDATE,
    DATEADD(day, 1, DATE_TRUNC(''month'', prog_date)) AS STARTDATE
  FROM POLDATA.PROG_DATE
  WHERE PROG_DATE_KEY = ''CURDATE''
) SRC
)
);


-- Component EXP_PARM_DATE, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_PARM_DATE AS
(
SELECT
TO_CHAR ( SQ_GW_CLOSEOUT_CTL1.STARTDATE , ''YYYY-MM-DD'' ) as "START",
TO_CHAR ( SQ_GW_CLOSEOUT_CTL1.ENDDATE , ''YYYY-MM-DD'' ) as "END",
TO_CHAR ( SQ_GW_CLOSEOUT_CTL1.EOM_DT , ''YYYY-MM-DD'' ) as EOM,
''[global]'' || CHAR(10) || ''$PMMergeSessParamFile=TRUE'' || CHAR(10) || ''--------------------------------------------------'' || CHAR(10) || ''**  Parameters passed to wf_load_earned_prem_history'' || CHAR(10) ||
 ''--------------------------------------------------'' || CHAR(10) AS header,
''$Earned_prem_Start_dt='' || CHR ( 39 ) || "START" || CHR ( 39 ) as Earned_prem_Start_dt,
''$Earned_prem_End_dt='' || CHR ( 39 ) || "END" || CHR ( 39 ) || CHR ( 13 ) as Earned_prem_End_dt,
''$Earned_prem_Cal_EOM_dt='' || CHR ( 39 ) || EOM || CHR ( 39 ) || CHR ( 13 ) as Earned_prem_Cal_EOM_dt,
SQ_GW_CLOSEOUT_CTL1.source_record_id
FROM
SQ_GW_CLOSEOUT_CTL1
);


-- Component NRM_PARM_DATE, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE NRM_PARM_DATE AS
(
SELECT ''REC1'' AS REC_NO, source_record_id, header AS output FROM EXP_PARM_DATE
UNION ALL
SELECT ''REC2'' AS REC_NO, source_record_id, NULL AS output FROM EXP_PARM_DATE
UNION ALL
SELECT ''REC3'' AS REC_NO, source_record_id, Earned_prem_Cal_EOM_dt AS output FROM EXP_PARM_DATE
UNION ALL
SELECT ''REC4'' AS REC_NO, source_record_id, Earned_prem_Start_dt AS output FROM EXP_PARM_DATE
UNION ALL
SELECT ''REC5'' AS REC_NO, source_record_id, Earned_prem_End_dt AS output FROM EXP_PARM_DATE
);


-- Component MCAS_DATE_PARAM, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE MCAS_DATE_PARAM AS
(
SELECT
NRM_PARM_DATE.output as output
FROM
NRM_PARM_DATE
);


-- Component MCAS_DATE_PARAM, Type EXPORT_DATA Exporting data
;


END; ';