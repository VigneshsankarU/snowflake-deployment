-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_LOCTR_INSUPD_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as SELECT 1;