-- Object Type: TASKS
-- Level: L2, ExecOrder: 242

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_ADDR_ELCTRNC_ADDR__S_M_GET_PRCS_ID
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_ADDR_ELCTRNC_ADDR_ROOT
	as CALL s_m_get_prcs_id('wklt_base_prty_addr_elctrnc_addr');