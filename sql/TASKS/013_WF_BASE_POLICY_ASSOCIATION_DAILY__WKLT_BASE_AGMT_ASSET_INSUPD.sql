-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_ASSET_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_AGMT_MBRSHP_INS_UPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_agmt_asset_insupd');