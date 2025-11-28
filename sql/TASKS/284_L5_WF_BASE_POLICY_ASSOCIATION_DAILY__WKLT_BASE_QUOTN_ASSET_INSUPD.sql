-- Object Type: TASKS
-- Level: L5, ExecOrder: 52

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_QUOTN_ASSET_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_QUOTN_AGMT_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_quotn_asset_insupd');