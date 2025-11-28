-- Object Type: TASKS
-- Level: L4, ExecOrder: 56

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_REAL_ESTAT_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_APLCTN_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_real_estat_insupd');