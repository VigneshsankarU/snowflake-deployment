-- Object Type: TASKS
-- Level: L2, ExecOrder: 155

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_INSU_ASST_FEAT_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_INSU_ASST_FEAT_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_agmt_insu_asst_feat_insupd');