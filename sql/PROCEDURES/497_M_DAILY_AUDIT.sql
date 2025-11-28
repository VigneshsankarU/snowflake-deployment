-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_DAILY_AUDIT("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  list STRING;
  PRCS_ID_1 STRING;
  PMWorkflowName STRING;
  PMSessionName STRING;
BEGIN
set list :=''1'';
set PRCS_ID_1 :=''1'';
set PMWorkflowName :=''s_m_daily_audit'';
set PMSessionName :=''s_m_daily_audit'';


-- Component SQ_pc_policyterm_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyterm_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as workflow_name,
$2 as session_name,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT wf_sessions_list.workflow_name,wf_sessions_list.session_name as session_name FROM DB_T_PROD_STAG.wf_sessions_list

where wf_sessions_list.workflow_name IN(:list)
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_pc_policyterm_x.workflow_name as PRCS_NM,
SQ_pc_policyterm_x.session_name as SUB_PRCS_NM,
TO_NUMBER(:PRCS_ID_1) as PRCS_ID,
SQ_pc_policyterm_x.source_record_id
FROM
SQ_pc_policyterm_x
);


-- Component LKPTRANS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKPTRANS AS
(
SELECT
LKP.lkp_SUB_PRCS_NM,
EXPTRANS.source_record_id,
ROW_NUMBER() OVER(PARTITION BY EXPTRANS.source_record_id ORDER BY LKP.lkp_PRCS_NM asc,LKP.lkp_SUB_PRCS_NM asc,LKP.lkp_PRCS_ID asc) RNK
FROM
EXPTRANS
LEFT JOIN (
SELECT ETL_PRCS_CTRL.PRCS_NM as lkp_PRCS_NM, ETL_PRCS_CTRL.SUB_PRCS_NM as lkp_SUB_PRCS_NM, ETL_PRCS_CTRL.PRCS_ID as lkp_PRCS_ID FROM DB_T_PROD_CORE.ETL_PRCS_CTRL ETL_PRCS_CTRL
WHERE PRCS_STS=''SUCCEEDED''
) LKP ON LKP.lkp_PRCS_NM = EXPTRANS.PRCS_NM AND LKP.lkp_SUB_PRCS_NM = EXPTRANS.SUB_PRCS_NM AND LKP.lkp_PRCS_ID > EXPTRANS.PRCS_ID
QUALIFY RNK = 1
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
EXPTRANS.PRCS_NM as PRCS_NM,
EXPTRANS.SUB_PRCS_NM as SUB_PRCS_NM,
CASE WHEN LKPTRANS.lkp_SUB_PRCS_NM IS NULL THEN ''FAILED'' ELSE ''SUCCEEDED'' END as CONDITION,
EXPTRANS.source_record_id
FROM
EXPTRANS
INNER JOIN LKPTRANS ON EXPTRANS.source_record_id = LKPTRANS.source_record_id
);


-- Component AGGTRANS, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE AGGTRANS AS
(
SELECT
MIN(EXPTRANS1.PRCS_NM) as PRCS_NM,
MIN(EXPTRANS1.SUB_PRCS_NM) as SUB_PRCS_NM,
MIN(EXPTRANS1.CONDITION) as CONDITION,
13 as v_output,
MIN(v_output) as output,
COUNT_IF(CONDITION = ''FAILED'') as counter,
COUNT(*) as s_count,
MIN(EXPTRANS1.source_record_id) as source_record_id
FROM
EXPTRANS1
);


-- Component EXPTRANS2, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS2 AS
(
SELECT
AGGTRANS.output as output,
AGGTRANS.counter as v_c,
AGGTRANS.s_count - AGGTRANS.counter as v_s,
AGGTRANS.source_record_id
FROM
AGGTRANS
);


-- Component tg_def, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tg_def AS
(
SELECT
EXPTRANS2.output as output
FROM
EXPTRANS2
);


-- Component tg_def, Type EXPORT_DATA Exporting data
COPY INTO @my_internal_stage/my_export_folder/tg_def_
FROM (SELECT * FROM tg_def)
HEADER = TRUE
OVERWRITE = TRUE;


END; ';