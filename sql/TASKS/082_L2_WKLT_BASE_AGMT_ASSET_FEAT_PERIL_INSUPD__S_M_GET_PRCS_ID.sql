-- Object Type: TASKS
-- Level: L2, ExecOrder: 89

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_ASSET_FEAT_PERIL_INSUPD__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_ASSET_FEAT_PERIL_INSUPD_ROOT
	as CALL s_m_get_prcs_id('wklt_base_agmt_asset_feat_peril_insupd');