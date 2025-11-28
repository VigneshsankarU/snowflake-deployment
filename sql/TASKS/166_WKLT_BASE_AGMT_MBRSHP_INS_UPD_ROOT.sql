-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_MBRSHP_INS_UPD_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as SELECT 1;