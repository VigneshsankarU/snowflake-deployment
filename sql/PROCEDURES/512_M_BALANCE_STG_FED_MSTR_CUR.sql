-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BALANCE_STG_FED_MSTR_CUR("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

declare 
PROJ_ID int;
BATCH_IND varchar;
PMMappingName varchar;
TABLE_NM varchar;

BEGIN 
PROJ_ID :=4;
BATCH_IND := ''Y'';
PMMappingName:= ''m_balance_stg_fed_mstr_cur'';
TABLE_NM :=''STG_FED_MSTR_CUR'';

-- Component SQ_Shortcut_to_STG_FED_MSTR_CUR, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_STG_FED_MSTR_CUR AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as STG_REC_COUNT,
$2 as STG_DOLLAR_AMT,
$3 as TRG_REC_COUNT,
$4 as TRG_DOLLAR_AMT,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT a.STG_REC_COUNT, a.STG_DOLLAR_AMT, b.REC_COUNT AS "TRG_REC_COUNT", b.DOLLAR_AMT AS "TRG_DOLLAR_AMT"

FROM (SELECT COUNT(*) AS "STG_REC_COUNT", SUM(AMOUNT_PAID) AS "STG_DOLLAR_AMT" 

FROM db_t_stag_prod.STG_FED_MSTR_CUR) a , DB_T_STAG_PROD.STG_FED_MSTR_TRG b
) SRC
)
);


-- Component exp_abort, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_abort AS
(
SELECT
SQ_Shortcut_to_STG_FED_MSTR_CUR.STG_REC_COUNT as STG_REC_COUNT,
SQ_Shortcut_to_STG_FED_MSTR_CUR.TRG_REC_COUNT as TRG_REC_COUNT,
--CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.STG_REC_COUNT != SQ_Shortcut_to_STG_FED_MSTR_CUR.TRG_REC_COUNT THEN RAISE_ERROR(''Record count staging ('' || to_char ( SQ_Shortcut_to_STG_FED_MSTR_CUR.STG_REC_COUNT ) || '') not matched with trigger file recorc count ('' || to_char ( SQ_Shortcut_to_STG_FED_MSTR_CUR.TRG_REC_COUNT ) || '')'') ELSE CASE WHEN SQ_Shortcut_to_STG_FED_MSTR_CUR.STG_DOLLAR_AMT != SQ_Shortcut_to_STG_FED_MSTR_CUR.TRG_DOLLAR_AMT THEN RAISE_ERROR(''Dollar Amount of staging ('' || to_char ( SQ_Shortcut_to_STG_FED_MSTR_CUR.STG_DOLLAR_AMT ) || '') not matched with trigger file recorc count ('' || to_char ( SQ_Shortcut_to_STG_FED_MSTR_CUR.TRG_DOLLAR_AMT ) || '')'') ELSE null END END as var_abort,
:PROJ_ID as PROJECT_ID,
:BATCH_IND as BATCH_ACTIVE_IND,
:PMMappingName as PROGRAM_NM,
:TABLE_NM as TABLE_NAME,
SQ_Shortcut_to_STG_FED_MSTR_CUR.source_record_id
FROM
SQ_Shortcut_to_STG_FED_MSTR_CUR
);


-- Component m_lkp_Control_Tables, Type MAPPLET 
--MAPPLET NOT REGISTERED: m_lkp_Control_Tables, mapplet instance m_lkp_Control_Tables;
call m_lkp_Control_Tables(''exp_abort'');

-- Component exp_Audit_Log, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Audit_Log AS
(
SELECT
m_lkp_Control_Tables.mplt_BATCH_ID as mplt_BATCH_ID,
m_lkp_Control_Tables.mplt_PRGM_ID as mplt_PRGM_ID,
m_lkp_Control_Tables.mplt_TABLE_ID as mplt_TABLE_ID,
exp_abort.TRG_REC_COUNT as TRG_REC_COUNT,
exp_abort.STG_REC_COUNT as STG_REC_COUNT,
''Load Successful'' as MESSAGE,
CURRENT_TIMESTAMP as INS_TS,
exp_abort.source_record_id
FROM
exp_abort
INNER JOIN m_lkp_Control_Tables ON exp_abort.source_record_id = m_lkp_Control_Tables.source_record_id
);


-- Component Shortcut_to_ECTL_AUDIT_LOG, Type TARGET 
INSERT INTO DB_T_CTRL_FIN_PROD.ECTL_AUDIT_LOG
(
ECTL_BATCH_ID,
ECTL_PRGM_ID,
ECTL_TABLE_ID,
ECTL_SRC_ROW_CNT,
ECTL_TRGT_INS_CNT,
ECTL_MESSAGE_TXT,
ECTL_ORIG_INS_TS
)
SELECT
exp_Audit_Log.mplt_BATCH_ID as ECTL_BATCH_ID,
exp_Audit_Log.mplt_PRGM_ID as ECTL_PRGM_ID,
exp_Audit_Log.mplt_TABLE_ID as ECTL_TABLE_ID,
exp_Audit_Log.TRG_REC_COUNT as ECTL_SRC_ROW_CNT,
exp_Audit_Log.STG_REC_COUNT as ECTL_TRGT_INS_CNT,
exp_Audit_Log.MESSAGE as ECTL_MESSAGE_TXT,
exp_Audit_Log.INS_TS as ECTL_ORIG_INS_TS
FROM
exp_Audit_Log;


END; ';