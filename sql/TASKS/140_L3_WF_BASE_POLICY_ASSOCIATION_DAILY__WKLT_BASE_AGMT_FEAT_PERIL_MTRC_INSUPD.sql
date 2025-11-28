-- Object Type: TASKS
-- Level: L3, ExecOrder: 11

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_FEAT_PERIL_MTRC_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_ASSET_FEAT_PERIL_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_agmt_feat_peril_mtrc_insupd');