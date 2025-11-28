-- Object Type: TASKS
-- Level: L3, ExecOrder: 205

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PLCY_ASSET_CVGE_MTRC_INSUPD__S_M_BASE_PLCY_ASSET_CVGE_MTRC_INSUPD
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_PLCY_ASSET_CVGE_MTRC_INSUPD__S_M_GET_PRCS_ID
	as BEGIN
  IF (
    (SELECT task_status
     FROM control_status
     WHERE run_id = (SELECT run_id FROM control_run_id WHERE worklet_name = 'wklt_base_plcy_asset_cvge_mtrc_insupd' ORDER BY insert_ts DESC LIMIT 1)
       AND task_name = 'm_get_prcs_id'
       AND worklet_name = 'wklt_base_plcy_asset_cvge_mtrc_insupd'
     ORDER BY task_start_dttm DESC
     LIMIT 1) = 'SUCCEEDED'
  ) THEN
    CALL m_base_plcy_asset_cvge_mtrc_insupd('wklt_base_plcy_asset_cvge_mtrc_insupd');
  END IF;
END;