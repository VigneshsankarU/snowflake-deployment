-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	as CALL m_get_prcs_id(SYSTEM$GET_PREDECESSOR_RETURN_VALUE('wklt_base_agt_acctbly_bi_feed'));