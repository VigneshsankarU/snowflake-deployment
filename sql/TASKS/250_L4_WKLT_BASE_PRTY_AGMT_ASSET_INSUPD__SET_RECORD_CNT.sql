-- Object Type: TASKS
-- Level: L4, ExecOrder: 262

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_AGMT_ASSET_INSUPD__SET_RECORD_CNT
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PRTY_AGMT_ASSET_INSUPD__S_M_BASE_PRTY_AGMT_ASSET_INSUPD
	as CALL sp_set_params_for_worklet(
    'wklt_base_prty_agmt_asset_insupd', 
    OBJECT_CONSTRUCT(
      'WKLT_SRC_EXTRACT_DT', '@QUERY:SELECT cs.var_json:StartTime::STRING FROM control_status cs WHERE cs.task_name = ''m_base_prty_agmt_asset_insupd'' ORDER BY task_start_dttm DESC LIMIT 1',
      'WKLT_SRC_CNT', '@QUERY:SELECT cs.var_json:SrcSuccessRows::STRING FROM control_status cs WHERE cs.task_name = ''m_base_prty_agmt_asset_insupd'' ORDER BY task_start_dttm DESC LIMIT 1',
      'WKLT_TGT_CNT', '@QUERY:SELECT cs.var_json:TgtSuccessRows::STRING FROM control_status cs WHERE cs.task_name = ''m_base_prty_agmt_asset_insupd'' ORDER BY task_start_dttm DESC LIMIT 1',
      'WKLT_SUB_PRCS_NAME', 's_m_base_prty_agmt_asset_insupd',
      'WKLT_SRC_NM', 'GW'
    )
  );