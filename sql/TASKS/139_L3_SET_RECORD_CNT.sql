-- Object Type: TASKS
-- Level: L3, ExecOrder: 3

create or replace task ALFA_EDW_DEV.PUBLIC.SET_RECORD_CNT
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.S_M_BASE_AGT_ACCTBLY_BI_FEED
	as INSERT INTO control_params (run_id, param_name, param_value, insert_ts)
VALUES (
  SYSTEM$GET_PREDECESSOR_RETURN_VALUE('wklt_base_agt_acctbly_bi_feed'),
  'WKLT_SUB_PRCS_NAME',
  's_m_base_agt_acctbly_bi_feed',
  CURRENT_TIMESTAMP());