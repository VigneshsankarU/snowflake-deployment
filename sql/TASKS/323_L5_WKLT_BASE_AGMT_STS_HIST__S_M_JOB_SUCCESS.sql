-- Object Type: TASKS
-- Level: L5, ExecOrder: 190

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_STS_HIST__S_M_JOB_SUCCESS
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_STS_HIST__SET_RECORD_CNT
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_run_id WHERE worklet_name = 'wklt_base_agmt_sts_hist' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_base_agmt_sts_hist'
     ORDER BY insert_ts DESC
     LIMIT 1) = 'SUCCEEDED'
  ) THEN
    CALL s_m_job_success('wklt_base_agmt_sts_hist');
  END IF;
END;