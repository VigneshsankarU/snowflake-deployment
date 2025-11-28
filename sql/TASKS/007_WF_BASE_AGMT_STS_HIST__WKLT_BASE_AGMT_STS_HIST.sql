-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_AGMT_STS_HIST__WKLT_BASE_AGMT_STS_HIST
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_AGMT_STS_HIST_ROOT
	as CALL sp_launch_worklet('wf_base_agmt_sts_hist', 'wklt_base_agmt_sts_hist');