-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_AUDIT_STG_TO_DW_CLAIMS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
    run_id STRING;
    workflow_name STRING;
    session_name STRING;
    TGT_TBL_NM STRING;
    BATCH_ACTIVE_IND STRING;
    ACT_PROJ_ID STRING;
    SRC_TBL_NM STRING;
    TABLE_NM STRING;
    PRGM_NM STRING;
    mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE STRING;
BEGIN 
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_AUDIT_STG_TO_DW_CLAIMS'';
    TGT_TBL_NM := public.func_get_scoped_param(:run_id, ''TGT_TBL_NM'', :workflow_name, :worklet_name, :session_name);
    BATCH_ACTIVE_IND := public.func_get_scoped_param(:run_id, ''BATCH_ACTIVE_IND'', :workflow_name, :worklet_name, :session_name);
    ACT_PROJ_ID := public.func_get_scoped_param(:run_id, ''ACT_PROJ_ID'', :workflow_name, :worklet_name, :session_name);
    SRC_TBL_NM := public.func_get_scoped_param(:run_id, ''SRC_TBL_NM'', :workflow_name, :worklet_name, :session_name);
    TABLE_NM := public.func_get_scoped_param(:run_id, ''TABLE_NM'', :workflow_name, :worklet_name, :session_name);
    PRGM_NM := public.func_get_scoped_param(:run_id, ''PRGM_NM'', :workflow_name, :worklet_name, :session_name);

-- Component sq_CLMF_CLAIM_CORE, Type SOURCE 
EXECUTE IMMEDIATE ''''''
CREATE OR REPLACE TEMPORARY TABLE sq_CLMF_CLAIM_CORE AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TGT_INS_CNT,
$2 as TGT_UPD_CNT,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

A.TGT_INS_CNT,B.TGT_UPD_CNT FROM 

(SELECT COUNT(*) AS TGT_INS_CNT FROM '' || TGT_TBL_NM || '' A, DB_T_CTRL_FIN_PROD.ECTL_BATCH_INFO B WHERE ECTL_TX_CD=''''NEW'''' AND ECTL_ORIG_INS_TS >= ECTL_BATCH_START_TS  AND ECTL_ACTIVE_IND='''''' || BATCH_ACTIVE_IND || '''''' AND ECTL_PROJ_ID='' || ACT_PROJ_ID || '') A,

(SELECT COUNT(*) AS TGT_UPD_CNT FROM '' || TGT_TBL_NM || '' A,DB_T_CTRL_FIN_PROD.ECTL_BATCH_INFO B WHERE ECTL_TX_CD=''''UPD'''' AND ECTL_MODIFY_TS >= ECTL_BATCH_START_TS  AND ECTL_ACTIVE_IND='''''' || BATCH_ACTIVE_IND || '''''' AND ECTL_PROJ_ID='' || ACT_PROJ_ID || '') B
) SRC
)
);
'''''';

-- Component exp_HOLD_TGT, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_HOLD_TGT AS
(
SELECT
sq_CLMF_CLAIM_CORE.TGT_INS_CNT as TGT_INS_CNT,
sq_CLMF_CLAIM_CORE.TGT_UPD_CNT as TGT_UPD_CNT,
1 as DUMMY,
sq_CLMF_CLAIM_CORE.source_record_id
FROM
sq_CLMF_CLAIM_CORE
);


-- Component sq_STG_CLMF_CLM_NO_TRL_AUTO, Type SOURCE 
EXECUTE IMMEDIATE ''
CREATE OR REPLACE TEMPORARY TABLE sq_STG_CLMF_CLM_NO_TRL_AUTO AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SRC_CNT,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT (K.C1)CNT FROM '' || SRC_TBL_NM || ''
) SRC
)
);
'';

-- Component exp_HOLD_SRC, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_HOLD_SRC AS
(
SELECT
TO_NUMBER(sq_STG_CLMF_CLM_NO_TRL_AUTO.SRC_CNT) as out_SRC_CNT,
1 as DUMMY,
sq_STG_CLMF_CLM_NO_TRL_AUTO.source_record_id
FROM
sq_STG_CLMF_CLM_NO_TRL_AUTO
);


-- Component jnr_SRC_TGT, Type JOINER 
CREATE OR REPLACE TEMPORARY TABLE jnr_SRC_TGT AS
(
SELECT
exp_HOLD_SRC.out_SRC_CNT as SRC_CNT,
exp_HOLD_SRC.DUMMY as DUMMY_SRC,
exp_HOLD_TGT.TGT_INS_CNT as TGT_INS_CNT,
exp_HOLD_TGT.TGT_UPD_CNT as TGT_UPD_CNT,
exp_HOLD_TGT.DUMMY as DUMMY_TGT,
row_number() over (order by 1) AS source_record_id
FROM
exp_HOLD_TGT
INNER JOIN exp_HOLD_SRC ON exp_HOLD_TGT.DUMMY = exp_HOLD_SRC.DUMMY
);


-- Component exp_MSG_DESC, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_MSG_DESC AS
(
SELECT
jnr_SRC_TGT.SRC_CNT as SRC_CNT,
jnr_SRC_TGT.TGT_INS_CNT as TGT_INS,
jnr_SRC_TGT.SRC_CNT - jnr_SRC_TGT.TGT_INS_CNT as TGT_REG,
jnr_SRC_TGT.TGT_UPD_CNT as TGT_UPD,
0 as TGT_DEL,
LTRIM ( RTRIM ( :TABLE_NM ) ) || '' - AUDIT INFORMATION SUCCESSFULLY LOADED'' as MSG_DESC,
CURRENT_TIMESTAMP as out_ORGN_TS,
:PRGM_NM as in_PRGM_NM,
:ACT_PROJ_ID as in_ACT_PROJ_ID,
:BATCH_ACTIVE_IND as in_BATCH_ACTIVE_IND,
LTRIM ( RTRIM ( :TABLE_NM ) ) as in_TABLE_NM,
jnr_SRC_TGT.source_record_id
FROM
jnr_SRC_TGT
);


-- Component mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE, Type MAPPLET 
CALL PUBLIC.mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE(''exp_MSG_DESC'');

-- Component ECTL_AUDIT_LOG_INS, Type TARGET 
INSERT INTO DB_T_CTRL_FIN_PROD.ECTL_AUDIT_LOG
(
ECTL_BATCH_ID,
ECTL_PRGM_ID,
ECTL_TABLE_ID,
ECTL_SRC_ROW_CNT,
ECTL_TRGT_INS_CNT,
ECTL_TRGT_UPD_CNT,
ECTL_TRGT_DEL_CNT,
ECTL_TRGT_REJ_CNT,
ECTL_MESSAGE_TXT,
ECTL_ORIG_INS_TS
)
SELECT
mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE.out_ECTL_BATCH_ID as ECTL_BATCH_ID,
mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE.out_ECTL_PRGM_ID as ECTL_PRGM_ID,
mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE.out_ECTL_TABLE_ID as ECTL_TABLE_ID,
exp_MSG_DESC.SRC_CNT as ECTL_SRC_ROW_CNT,
exp_MSG_DESC.TGT_INS as ECTL_TRGT_INS_CNT,
exp_MSG_DESC.TGT_UPD as ECTL_TRGT_UPD_CNT,
exp_MSG_DESC.TGT_DEL as ECTL_TRGT_DEL_CNT,
exp_MSG_DESC.TGT_REG as ECTL_TRGT_REJ_CNT,
exp_MSG_DESC.MSG_DESC as ECTL_MESSAGE_TXT,
exp_MSG_DESC.out_ORGN_TS as ECTL_ORIG_INS_TS
FROM
exp_MSG_DESC
INNER JOIN mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE ON exp_MSG_DESC.source_record_id = mplt_LKP_ECTL_BATCH_PRGM_VALIDATION_CORE.source_record_id;

END; ';