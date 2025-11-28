-- Object Type: TASKS
-- Level: L4, ExecOrder: 51

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_QUOTN_AGMT_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_QUOTN_LOCTR_INSUPD
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_quotn_agmt_insupd');