-- Object Type: TASKS
-- Level: L3, ExecOrder: 65

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_PRIOR_LOSS__WKLT_BASE_PRIORLOSS_CLMPMT_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_PRIOR_LOSS__WKLT_BASE_PRIORLOSS_INSUPD
	as CALL sp_launch_worklet('wf_base_prior_loss', 'wklt_base_priorloss_clmpmt_insupd');