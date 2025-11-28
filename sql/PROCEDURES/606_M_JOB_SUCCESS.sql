-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_JOB_SUCCESS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  PRCS_ID STRING;
  SRC_CNT STRING;
  TGT_CNT STRING;
  SUB_PRCS_NM STRING;
  SRC_NM STRING;
  SRC_EXTRACT_DT STRING;
  v_start_time TIMESTAMP;
  TgtSuccessRows NUMBER;
BEGIN
  run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  session_name := ''s_m_job_success'';
  PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
  SRC_CNT := public.func_get_scoped_param(:run_id, ''SRC_CNT'', :workflow_name, :worklet_name, :session_name);
  TGT_CNT := public.func_get_scoped_param(:run_id, ''TGT_CNT'', :workflow_name, :worklet_name, :session_name);
  SUB_PRCS_NM := public.func_get_scoped_param(:run_id, ''SUB_PRCS_NM'', :workflow_name, :worklet_name, :session_name);
  SRC_NM := public.func_get_scoped_param(:run_id, ''SRC_NM'', :workflow_name, :worklet_name, :session_name);
  SRC_EXTRACT_DT := public.func_get_scoped_param(:run_id, ''SRC_EXTRACT_DT'', :workflow_name, :worklet_name, :session_name);
  v_start_time := CURRENT_TIMESTAMP();

-- Component src_src_edw_gen_seq, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_src_edw_gen_seq AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRCS_STAG,
$2 as PRCS_STS,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT ''BASE'' as  PRCS_STAG, ''SUCCEEDED'' as PRCS_STS 

/* FROM DB_SP_PROD.EDW_SEQ_GEN where LGL_TBLNAME=''DZ_PRCS_ID'' */
) SRC
)
);


-- Component exp_prcs_id_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_id_upd AS
(
SELECT
:PRCS_ID as PRCS_ID,
:SRC_NM as SRC_NM,
:SUB_PRCS_NM as SUB_PRCS_NM,
CURRENT_TIMESTAMP as END_DT,
:SRC_EXTRACT_DT as SRC_EXTRACT_DT,
:SRC_CNT as SRC_CNT,
:TGT_CNT as TGT_CNT,
src_src_edw_gen_seq.PRCS_STAG as PRCS_STAG,
src_src_edw_gen_seq.PRCS_STS as PRCS_STS,
src_src_edw_gen_seq.source_record_id
FROM
src_src_edw_gen_seq
);


-- Component upd_stg_prcs_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_prcs_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_prcs_id_upd.PRCS_ID as PRCS_ID,
exp_prcs_id_upd.PRCS_STAG as PRCS_STAG,
exp_prcs_id_upd.SRC_NM as SRC_NM,
exp_prcs_id_upd.SUB_PRCS_NM as SUB_PRCS_NM,
exp_prcs_id_upd.END_DT as END_DT,
exp_prcs_id_upd.SRC_EXTRACT_DT as SRC_EXTRACT_DT,
exp_prcs_id_upd.PRCS_STS as PRCS_STS,
exp_prcs_id_upd.SRC_CNT as SRC_CNT,
exp_prcs_id_upd.TGT_CNT as TGT_CNT,
exp_prcs_id_upd.source_record_id as source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_prcs_id_upd
);


-- Component exp_prcs_tgt_pass_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_tgt_pass_upd AS
(
SELECT
upd_stg_prcs_upd.PRCS_ID as PRCS_ID,
upd_stg_prcs_upd.PRCS_STAG as PRCS_STAG,
upd_stg_prcs_upd.SRC_NM as SRC_NM,
upd_stg_prcs_upd.SUB_PRCS_NM as SUB_PRCS_NM,
upd_stg_prcs_upd.END_DT as END_DT,
upd_stg_prcs_upd.SRC_EXTRACT_DT as SRC_EXTRACT_DT,
upd_stg_prcs_upd.PRCS_STS as PRCS_STS,
upd_stg_prcs_upd.SRC_CNT as SRC_CNT,
upd_stg_prcs_upd.TGT_CNT as TGT_CNT,
upd_stg_prcs_upd.source_record_id
FROM
upd_stg_prcs_upd
);

TgtSuccessRows := (SELECT COUNT(*) FROM exp_prcs_tgt_pass_upd);

-- Component tgt_etl_prcs_ctrl_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.ETL_PRCS_CTRL
USING exp_prcs_tgt_pass_upd ON (ETL_PRCS_CTRL.PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID AND ETL_PRCS_CTRL.PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG)
WHEN MATCHED THEN UPDATE
SET
PRCS_ID = exp_prcs_tgt_pass_upd.PRCS_ID,
PRCS_STAG = exp_prcs_tgt_pass_upd.PRCS_STAG,
SRC_NM = exp_prcs_tgt_pass_upd.SRC_NM,
SUB_PRCS_NM = exp_prcs_tgt_pass_upd.SUB_PRCS_NM,
JOB_END_DT = exp_prcs_tgt_pass_upd.END_DT,
SRC_EXTRACT_DT = exp_prcs_tgt_pass_upd.SRC_EXTRACT_DT,
PRCS_STS = exp_prcs_tgt_pass_upd.PRCS_STS,
SRC_CNT = exp_prcs_tgt_pass_upd.SRC_CNT,
TGT_CNT = exp_prcs_tgt_pass_upd.TGT_CNT;

INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_job_success'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''SrcSuccessRows'', (SELECT COUNT(*) FROM src_src_edw_gen_seq),
  ''TgtSuccessRows'', :TgtSuccessRows
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_job_success'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );

END; ';