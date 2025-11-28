-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_AGMT_INSUPD__S_M_JOB_FAILURE
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_AGMT_INSUPD__SET_RECORD_CNT
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_run_id WHERE worklet_name = 'wklt_base_prty_agmt_insupd' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_base_prty_agmt'
       AND worklet_name = 'wklt_base_prty_agmt_insupd'
     ORDER BY task_start_dttm DESC
     LIMIT 1) = 'FAILED'
  ) THEN
    CALL s_m_job_failure('wklt_base_prty_agmt_insupd');
  END IF;
END;