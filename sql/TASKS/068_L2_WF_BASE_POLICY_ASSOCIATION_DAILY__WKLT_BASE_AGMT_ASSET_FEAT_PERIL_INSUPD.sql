-- Object Type: TASKS
-- Level: L2, ExecOrder: 9

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_ASSET_FEAT_PERIL_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY_ROOT
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_agmt_asset_feat_peril_insupd');