-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PLCY_CVGE_COST_MTRC_INSUPD_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as SELECT 1;