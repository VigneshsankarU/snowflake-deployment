-- Object Type: TASKS
-- Level: L3, ExecOrder: 23

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_ADDR_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_IDNTFTN
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_prty_addr_insupd');