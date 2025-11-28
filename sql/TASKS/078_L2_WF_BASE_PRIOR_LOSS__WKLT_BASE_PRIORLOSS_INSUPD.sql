-- Object Type: TASKS
-- Level: L2, ExecOrder: 64

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_PRIOR_LOSS__WKLT_BASE_PRIORLOSS_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_PRIOR_LOSS_ROOT
	as CALL sp_launch_worklet('wf_base_prior_loss', 'wklt_base_priorloss_insupd');