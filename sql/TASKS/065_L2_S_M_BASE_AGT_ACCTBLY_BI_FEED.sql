-- Object Type: TASKS
-- Level: L2, ExecOrder: 2

create or replace task ALFA_EDW_DEV.PUBLIC.S_M_BASE_AGT_ACCTBLY_BI_FEED
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.S_M_GET_PRCS_ID
	as CALL m_base_agt_acctbly_bi_feed(SYSTEM$GET_PREDECESSOR_RETURN_VALUE('wklt_base_agt_acctbly_bi_feed'));