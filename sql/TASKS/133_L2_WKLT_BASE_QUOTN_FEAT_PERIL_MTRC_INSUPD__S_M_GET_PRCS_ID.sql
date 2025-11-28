-- Object Type: TASKS
-- Level: L2, ExecOrder: 392

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_FEAT_PERIL_MTRC_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_FEAT_PERIL_MTRC_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_quotn_feat_peril_mtrc_insupd');