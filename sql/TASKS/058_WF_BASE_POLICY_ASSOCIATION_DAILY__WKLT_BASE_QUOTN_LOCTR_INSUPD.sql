-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_QUOTN_LOCTR_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_QUOTN_MBRSHP_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_quotn_loctr_insupd');