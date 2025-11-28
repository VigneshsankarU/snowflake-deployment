-- Object Type: TASKS
-- Level: L2, ExecOrder: 284

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_DRVG_HIST_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_DRVG_HIST_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_prty_drvg_hist_insupd');