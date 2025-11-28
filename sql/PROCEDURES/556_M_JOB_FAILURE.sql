-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_JOB_FAILURE("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  run_id STRING;
  workflow_name STRING;
  PRCS_ID STRING;
  SRC_CNT STRING;
  TGT_CNT STRING;
  SUB_PRCS_NM STRING;
  SRC_NM STRING;
BEGIN
  SELECT run_id, workflow_name INTO run_id, workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1;
  session_name := ''s_m_job_failure'';
  PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
  SRC_CNT := public.func_get_scoped_param(:run_id, ''SRC_CNT'', :workflow_name, :worklet_name, :session_name);
  TGT_CNT := public.func_get_scoped_param(:run_id, ''TGT_CNT'', :workflow_name, :worklet_name, :session_name);
  SUB_PRCS_NM := public.func_get_scoped_param(:run_id, ''SUB_PRCS_NM'', :workflow_name, :worklet_name, :session_name);
  SRC_NM := public.func_get_scoped_param(:run_id, ''SRC_NM'', :workflow_name, :worklet_name, :session_name);

-- Component sql_etl_prcs_ctrl_upd, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sql_etl_prcs_ctrl_upd AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRCS_STS,
$2 as PRCS_STAG,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  ''FAILED'' as PRCS_STS ,''BASE'' as PRCS_STAG 

/* FROM DB_SP_PROD.EDW_SEQ_GEN where LGL_TBLNAME=''BASE_PRCS_ID'' */
) SRC
)
);


-- Component exp_prcs_src_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_src_pass AS
(
SELECT
:PRCS_ID as o_process_id,
:SRC_CNT as SRC_CNT,
:TGT_CNT as TGT_CNT,
:SUB_PRCS_NAME as SUB_PRCS_NAME,
CURRENT_TIMESTAMP as o_END_DT,
:SRC_NM as SRC_NM,
sql_etl_prcs_ctrl_upd.PRCS_STS as PRCS_STS,
sql_etl_prcs_ctrl_upd.PRCS_STAG as PRCS_STAG,
sql_etl_prcs_ctrl_upd.source_record_id
FROM
sql_etl_prcs_ctrl_upd
);


-- Component upd_stg_prcs_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_prcs_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_prcs_src_pass.o_process_id as PRCS_ID,
exp_prcs_src_pass.SRC_CNT as SRC_CNT,
exp_prcs_src_pass.TGT_CNT as TGT_CNT,
exp_prcs_src_pass.o_END_DT as END_DT,
exp_prcs_src_pass.PRCS_STS as PRCS_STS,
exp_prcs_src_pass.PRCS_STAG as PRCS_STAG,
exp_prcs_src_pass.SUB_PRCS_NAME as SUB_PRCS_NAME,
exp_prcs_src_pass.SRC_NM as SRC_NM,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_prcs_src_pass
);


-- Component exp_prcs_tgt_pass_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_tgt_pass_upd AS
(
SELECT
upd_stg_prcs_upd.PRCS_ID as PRCS_ID,
upd_stg_prcs_upd.END_DT as END_DT,
upd_stg_prcs_upd.PRCS_STS as PRCS_STS,
upd_stg_prcs_upd.SRC_CNT as SRC_CNT,
upd_stg_prcs_upd.TGT_CNT as TGT_CNT,
upd_stg_prcs_upd.PRCS_STAG as PRCS_STAG,
upd_stg_prcs_upd.SUB_PRCS_NAME as SUB_PRCS_NAME,
upd_stg_prcs_upd.SRC_NM as SRC_NM,
upd_stg_prcs_upd.source_record_id
FROM
upd_stg_prcs_upd
);


-- Component etl_prcs_ctrl_upd_1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.ETL_PRCS_CTRL
USING exp_prcs_tgt_pass_upd ON (ETL_PRCS_CTRL.PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID AND ETL_PRCS_CTRL.PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG)
WHEN MATCHED THEN UPDATE
SET
PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID,
PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG,
SRC_NM = exp_prcs_tgt_pass_upd.SRC_NM,
SUB_PRCS_NM = exp_prcs_tgt_pass_upd.SUB_PRCS_NAME,
JOB_END_DT = exp_prcs_tgt_pass_upd.END_DT,
PRCS_STS = exp_prcs_tgt_pass_upd.PRCS_STS,
SRC_CNT = exp_prcs_tgt_pass_upd.SRC_CNT,
TGT_CNT = exp_prcs_tgt_pass_upd.TGT_CNT;

INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_job_failure'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''SrcSuccessRows'', (SELECT COUNT(*) FROM sql_etl_prcs_ctrl_upd),
  ''TgtSuccessRows'', (
    SELECT COUNT(*)
    FROM DB_T_PROD_CORE.ETL_PRCS_CTRL tgt
    JOIN exp_prcs_tgt_pass_upd src
      ON tgt.PRCS_ID = src.PRCS_ID
    AND tgt.PRCS_STAG = src.PRCS_STAG
    WHERE tgt.SRC_NM != src.SRC_NM
      OR tgt.SUB_PRCS_NM != src.SUB_PRCS_NAME
      OR tgt.JOB_END_DT != src.END_DT
      OR tgt.PRCS_STS != src.PRCS_STS
      OR tgt.SRC_CNT != src.SRC_CNT
      OR tgt.TGT_CNT != src.TGT_CNT
  )
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_job_failure'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );

END; ';