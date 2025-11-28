-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_GET_PRCS_ID("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  PRCS_ID STRING;
  v_start_time TIMESTAMP;
  v_var_json VARIANT;
  v_out_seq_num STRING;
  v_unique_count NUMBER;
  DUPLICATE_EXCEPTION EXCEPTION (-20001, ''Duplicate entry for PRCS_ID AND PRCS_STAG'');
BEGIN
  run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  session_name := ''s_m_get_prcs_id'';
  PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
  v_start_time := CURRENT_TIMESTAMP();

-- Component sql_etl_prcs_ctrl_ins, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sql_etl_prcs_ctrl_ins AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PRCS_STAG,
$2 as PRCS_STS,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT ''BASE'' as  PRCS_STAG, ''STARTED'' as PRCS_STS 

/* FROM DB_SP_PROD.EDW_SEQ_GEN where LGL_TBLNAME=''BASE_PRCS_ID'' */
) SRC
)
);


-- Component exp_prcs_id_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_id_src AS
(
SELECT
''BASE_PRCS_ID'' as PRCS_ID,
sql_etl_prcs_ctrl_ins.PRCS_STAG as PRCS_STAG,
''GW'' as SRC_NM,
:workflow_name as PRCS_NM,
:session_name as SUB_PRCS_NM,
CURRENT_TIMESTAMP as START_DT,
sql_etl_prcs_ctrl_ins.PRCS_STS as PRCS_STS,
sql_etl_prcs_ctrl_ins.source_record_id
FROM
sql_etl_prcs_ctrl_ins
);


-- Component sp_process_id, Type STORED_PROCEDURE 
CALL DB_SP_PROD.EDW_GEN_SRGTE_KEY(''BASE_PRCS_ID'') INTO v_out_seq_num;

PRCS_ID := v_out_seq_num;

CREATE OR REPLACE TEMPORARY TABLE sp_process_id AS (
  SELECT 
    exp_prcs_id_src.source_record_id,
    :v_out_seq_num AS OUT_SEQ_NUM
  FROM exp_prcs_id_src
);

MERGE INTO control_params tgt
USING (
  SELECT
    :run_id AS run_id,
    ''2_worklet'' AS scope_type,
    CONCAT(:workflow_name, '':'', :worklet_name) AS scope_name,
    ''PRCS_ID'' AS param_name,
    :v_out_seq_num AS param_value
) src
ON tgt.run_id = src.run_id
  AND tgt.scope_type = src.scope_type
  AND tgt.scope_name = src.scope_name
  AND tgt.param_name = src.param_name
WHEN MATCHED THEN
  UPDATE SET param_value = src.param_value, insert_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (run_id, scope_type, scope_name, param_name, param_value, insert_ts)
  VALUES (src.run_id, src.scope_type, src.scope_name, src.param_name, src.param_value, CURRENT_TIMESTAMP());


-- Component upd_stg_prcs_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_prcs_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
sp_process_id.OUT_SEQ_NUM as OUT_SEQ_NUM,
exp_prcs_id_src.PRCS_STAG as PRCS_STAG,
exp_prcs_id_src.PRCS_NM as PRCS_NM,
exp_prcs_id_src.START_DT as START_DT,
exp_prcs_id_src.PRCS_STS as PRCS_STS,
exp_prcs_id_src.SUB_PRCS_NM as SUB_PRCS_NM,
exp_prcs_id_src.SRC_NM as SRC_NM,
exp_prcs_id_src.source_record_id as source_record_id
FROM
exp_prcs_id_src
LEFT JOIN sp_process_id ON exp_prcs_id_src.source_record_id = sp_process_id.source_record_id
);


-- Component exp_prcs_tgt_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prcs_tgt_pass AS
(
SELECT
GREATEST(COALESCE(:PRCS_ID,0),upd_stg_prcs_ins.OUT_SEQ_NUM) as o_temp,
upd_stg_prcs_ins.PRCS_STAG as PRCS_STAG,
upd_stg_prcs_ins.PRCS_NM as PRCS_NM,
upd_stg_prcs_ins.START_DT as START_DT,
upd_stg_prcs_ins.PRCS_STS as PRCS_STS,
upd_stg_prcs_ins.SUB_PRCS_NM as SUB_PRCS_NM,
upd_stg_prcs_ins.SRC_NM as SRC_NM,
upd_stg_prcs_ins.source_record_id
FROM
upd_stg_prcs_ins
);

SELECT COUNT(*) INTO v_unique_count FROM DB_T_PROD_CORE.ETL_PRCS_CTRL
WHERE PRCS_ID = :PRCS_ID AND PRCS_STAG = ''BASE'';

IF (v_unique_count > 0) THEN
  RAISE DUPLICATE_EXCEPTION;
END IF;

-- Component etl_prcs_ctrl_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.ETL_PRCS_CTRL
(
PRCS_ID,
PRCS_STAG,
SRC_NM,
PRCS_NM,
SUB_PRCS_NM,
JOB_START_DT,
PRCS_STS
)
SELECT
exp_prcs_tgt_pass.o_temp as PRCS_ID,
exp_prcs_tgt_pass.PRCS_STAG as PRCS_STAG,
exp_prcs_tgt_pass.SRC_NM as SRC_NM,
exp_prcs_tgt_pass.PRCS_NM as PRCS_NM,
exp_prcs_tgt_pass.SUB_PRCS_NM as SUB_PRCS_NM,
exp_prcs_tgt_pass.START_DT as JOB_START_DT,
exp_prcs_tgt_pass.PRCS_STS as PRCS_STS
FROM
exp_prcs_tgt_pass;

INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_get_prcs_id'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''PRCS_ID'', :PRCS_ID
);

RETURN PRCS_ID;

EXCEPTION WHEN OTHER THEN
    v_var_json := OBJECT_CONSTRUCT(''SQLERRM'', :sqlerrm);
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
      SELECT  :run_id, :worklet_name, ''m_get_prcs_id'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), :v_var_json;

END; ';