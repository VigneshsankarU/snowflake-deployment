-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_LOCTR_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_LOCTR_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_agmt_loctr_insupd');