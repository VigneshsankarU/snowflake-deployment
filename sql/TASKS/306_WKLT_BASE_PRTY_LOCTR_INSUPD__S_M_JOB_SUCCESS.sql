-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_LOCTR_INSUPD__S_M_JOB_SUCCESS
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_LOCTR_INSUPD__SET_RECORD_CNT
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_run_id WHERE worklet_name = 'wklt_base_prty_loctr_insupd' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_base_prty_loctr_insupd'
       AND worklet_name = 'wklt_base_prty_loctr_insupd'
     ORDER BY task_start_dttm DESC
     LIMIT 1) = 'SUCCEEDED'
  ) THEN
    CALL s_m_job_success('wklt_base_prty_loctr_insupd');
  END IF;
END;