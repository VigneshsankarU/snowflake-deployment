-- Object Type: TASKS
-- Level: L6, ExecOrder: 26

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_ASSET_LOCTR_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY__WKLT_BASE_PRTY_ADDR_ELCTRNC_ADDR
	as CALL sp_launch_worklet('wf_base_policy_association_daily', 'wklt_base_prty_asset_loctr_insupd');