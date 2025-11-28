-- Object Type: TASKS
-- Level: L3, ExecOrder: 193

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED__S_M_BASE_AGT_ACCTBLY_BI_FEED
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED__S_M_GET_PRCS_ID
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_worklet WHERE worklet_name = 'wklt_base_agt_acctbly_bi_feed' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_get_prcs_id'
     ORDER BY insert_ts DESC
     LIMIT 1) = 'SUCCEEDED'
  ) THEN
    CALL m_base_agt_acctbly_bi_feed('wklt_base_agt_acctbly_bi_feed');
  END IF;
END;