-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.S_M_GET_PRCS_ID("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  v_run_id STRING;
  workflow_name STRING;
  prcs_id STRING;
  v_start_time STRING;
  runtime_exception EXCEPTION (-20001, ''There was an error running the mapping.'');
BEGIN
  v_run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  prcs_id := func_get_scoped_param(:v_run_id, ''WKLT_PRCS_ID'', :workflow_name, :worklet_name, ''s_m_get_prcs_id'');
  v_start_time := CURRENT_TIMESTAMP();
  
  CALL sp_set_param(:v_run_id, ''1_session'', ''s_m_get_prcs_id'', ''PRCS_ID'', :prcs_id);

  -- Call mapping stored procedure
  CALL m_get_prcs_id(:worklet_name) into :prcs_id;

  CALL sp_set_param(:v_run_id, ''2_worklet'', :workflow_name || '':'' || :worklet_name, ''WKLT_PRCS_ID'', :prcs_id);

  INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
  SELECT :v_run_id, :worklet_name, ''s_m_get_prcs_id'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), NULL;

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
      SELECT  :v_run_id, :worklet_name, ''s_m_get_prcs_id'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(''SQLERRM'', :sqlerrm);
    
    RAISE runtime_exception;

END;
';