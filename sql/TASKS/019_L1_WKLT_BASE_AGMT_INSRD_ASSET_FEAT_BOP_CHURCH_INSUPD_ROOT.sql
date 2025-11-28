-- Object Type: TASKS
-- Level: L1, ExecOrder: 148

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_INSRD_ASSET_FEAT_BOP_CHURCH_INSUPD_ROOT
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 0 1 1 * UTC'
	as SELECT 1;