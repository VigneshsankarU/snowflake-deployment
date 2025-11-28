-- Object Type: TASKS
-- Level: L1, ExecOrder: 82

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_ASSET_DTL_CODE_XREF_INSUPD_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as SELECT 1;