-- Object Type: TASKS
-- Level: L5, ExecOrder: 32

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_STS_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_AGMT_ASSET_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_prty_sts_insupd');