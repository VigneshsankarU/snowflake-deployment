-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRIORLOSS_INSUPD__S_M_BASE_PRIOR_LOSS
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRIORLOSS_INSUPD__S_M_GET_PRCS_ID
	as CALL m_base_prior_loss('wklt_base_priorloss_insupd');