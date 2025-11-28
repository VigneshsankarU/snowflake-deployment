-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_MBRSHP_INS_UPD__S_M_JOB_FAILURE
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_MBRSHP_INS_UPD__SET_RECORD_CNT
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_run_id WHERE worklet_name = 'wklt_base_agmt_mbrshp_ins_upd' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_base_agmt_mbrshp_ins_upd'
       AND worklet_name = 'wklt_base_agmt_mbrshp_ins_upd'
     ORDER BY task_start_dttm DESC
     LIMIT 1) = 'FAILED'
  ) THEN
    CALL s_m_job_failure('wklt_base_agmt_mbrshp_ins_upd');
  END IF;
END;