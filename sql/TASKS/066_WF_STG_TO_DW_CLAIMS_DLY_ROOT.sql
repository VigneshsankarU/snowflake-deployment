-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WF_STG_TO_DW_CLAIMS_DLY_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as CALL public.sp_load_params_and_generate_run_id('/Parameter/Claims/PROGRAM_NAMES.txt', 'wf_STG_TO_DW_CLAIMS_DLY');