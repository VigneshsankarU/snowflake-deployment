-- Object Type: TASKS
-- Level: L4, ExecOrder: 31

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_AGMT_ASSET_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_QUOTN_ASSET_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_prty_agmt_asset_insupd');