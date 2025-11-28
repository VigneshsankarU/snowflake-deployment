-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.S_M_JOB_SUCCESS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  v_run_id STRING;
  workflow_name STRING;
  prcs_id STRING;
  sub_prcs_nm STRING;
  src_nm STRING;
  src_extract_dt STRING;
  src_cnt STRING;
  tgt_cnt STRING;
  v_start_time TIMESTAMP;
BEGIN
  v_run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  prcs_id := func_get_scoped_param(:v_run_id, ''WKLT_PRCS_ID'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  sub_prcs_nm := func_get_scoped_param(:v_run_id, ''WKLT_SUB_PRCS_NAME'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  src_nm := func_get_scoped_param(:v_run_id, ''WKLT_SRC_NM'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  src_extract_dt := func_get_scoped_param(:v_run_id, ''WKLT_SRC_EXTRACT_DT'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  src_cnt := func_get_scoped_param(:v_run_id, ''WKLT_SRC_CNT'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  tgt_cnt := func_get_scoped_param(:v_run_id, ''WKLT_TGT_CNT'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  v_start_time := CURRENT_TIMESTAMP();

  -- Insert pre-session variables
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_job_success'', ''PRCS_ID'', :prcs_id);
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_job_success'', ''SUB_PRCS_NM'', :sub_prcs_nm);
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_job_success'', ''SRC_NM'', :src_nm);
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_job_success'', ''SRC_EXTRACT_DT'', :src_extract_dt);
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_job_success'', ''SRC_CNT'', :src_cnt);
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_job_success'', ''TGT_CNT'', :tgt_cnt);
  
  -- Call mapping stored procedure
  CALL m_job_success(:worklet_name);

  INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
  SELECT :v_run_id, :worklet_name, ''s_m_job_success'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(''worklet_name'', :worklet_name);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
      SELECT  :v_run_id, :worklet_name, ''s_m_job_success'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(''SQLERRM'', :sqlerrm);
END;
';