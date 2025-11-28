-- Object Type: TASKS
-- Level: L2, ExecOrder: 374

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_FEAT_LOCTR_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_FEAT_LOCTR_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_quotn_feat_loctr_insupd');