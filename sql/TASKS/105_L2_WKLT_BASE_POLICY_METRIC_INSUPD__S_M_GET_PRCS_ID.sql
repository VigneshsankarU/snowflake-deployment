-- Object Type: TASKS
-- Level: L2, ExecOrder: 222

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_POLICY_METRIC_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_POLICY_METRIC_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_policy_metric_insupd');