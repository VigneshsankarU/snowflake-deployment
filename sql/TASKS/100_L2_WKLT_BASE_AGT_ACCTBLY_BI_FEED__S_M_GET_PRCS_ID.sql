-- Object Type: TASKS
-- Level: L2, ExecOrder: 192

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED_ROOT
	as CALL s_m_get_prcs_id('wklt_base_agt_acctbly_bi_feed');