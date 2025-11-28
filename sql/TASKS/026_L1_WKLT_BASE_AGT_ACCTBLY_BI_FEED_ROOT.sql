-- Object Type: TASKS
-- Level: L1, ExecOrder: 191

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as SELECT 1;