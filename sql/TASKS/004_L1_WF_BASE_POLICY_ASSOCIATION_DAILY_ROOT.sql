-- Object Type: TASKS
-- Level: L1, ExecOrder: 8

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_POLICY_ASSOCIATION_DAILY_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as CALL sp_load_params_and_generate_run_id('/Parameter/edw_base/edw_param_base.txt', 'wf_base_policy_association_daily');