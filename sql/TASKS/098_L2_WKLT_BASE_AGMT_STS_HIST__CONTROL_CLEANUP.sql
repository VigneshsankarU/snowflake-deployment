-- Object Type: TASKS
-- Level: L2, ExecOrder: 185

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_STS_HIST__CONTROL_CLEANUP
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_STS_HIST_ROOT
	as CALL sp_control_cleanup('wklt_base_agmt_sts_hist', FALSE);