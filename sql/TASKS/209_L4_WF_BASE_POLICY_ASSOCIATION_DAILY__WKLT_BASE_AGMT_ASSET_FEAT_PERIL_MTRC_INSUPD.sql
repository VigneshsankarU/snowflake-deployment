-- Object Type: TASKS
-- Level: L4, ExecOrder: 12

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_ASSET_FEAT_PERIL_MTRC_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_FEAT_PERIL_MTRC_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_agmt_asset_feat_peril_mtrc_insupd');