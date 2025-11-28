-- Object Type: TASKS
-- Level: L3, ExecOrder: 229

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRIORLOSS_CLMPMT_INSUPD__S_M_BASE_PRIORLOSS_CLMPMT
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRIORLOSS_CLMPMT_INSUPD__S_M_GET_PRCS_ID
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_run_id WHERE worklet_name = 'wklt_base_priorloss_clmpmt_insupd' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_get_prcs_id'
       AND worklet_name = 'wklt_base_priorloss_clmpmt_insupd'
     ORDER BY task_start_dttm DESC
     LIMIT 1) = 'SUCCEEDED'
  ) THEN
    CALL m_base_priorloss_clmpmt('wklt_base_priorloss_clmpmt_insupd');
  END IF;
END;