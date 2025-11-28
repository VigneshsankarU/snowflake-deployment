-- Object Type: TASKS
-- Level: L3, ExecOrder: 387

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_FEAT_PERIL_INSUPD__S_M_BASE_QUOTN_FEAT_PERIL_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_QUOTN_FEAT_PERIL_INSUPD__S_M_GET_PRCS_ID
	as CALL m_base_quotn_feat_peril_insupd('wklt_base_quotn_feat_peril_insupd');