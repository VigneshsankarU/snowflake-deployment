-- Object Type: TASKS
-- Level: L12, ExecOrder: 58

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PLCY_CVGE_MTRC_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PLCY_ASSET_CVGE_MTRC_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_plcy_cvge_mtrc_insupd');