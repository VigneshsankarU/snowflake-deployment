-- Object Type: TASKS
-- Level: L4, ExecOrder: 188

create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_STS_HIST__SET_RECORD_CNT
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGMT_STS_HIST__S_M_BASE_AGMT_STS_HIST
	as MERGE INTO control_params tgt
USING (
  SELECT
    cr.run_id,
    '2_worklet' AS scope_type,
    'wklt_base_agmt_sts_hist' AS scope_name,
    param_name,
    param_value
  FROM control_run_id cr
  CROSS JOIN LATERAL (
    SELECT 'WKLT_SUB_PRCS_NAME' AS param_name, 's_m_base_agmt_sts_hist' AS param_value UNION ALL
    SELECT 'WKLT_SRC_NM', 'GW' UNION ALL
    SELECT 'WKLT_SRC_EXTRACT_DT', cs.var_json:StartTime::STRING FROM control_status cs WHERE cs.task_name = 'm_base_agmt_sts_hist' AND cs.run_id = cr.run_id UNION ALL
    SELECT 'WKLT_SRC_CNT', cs.var_json:SrcSuccessRows::STRING FROM control_status cs WHERE cs.task_name = 'm_base_agmt_sts_hist' AND cs.run_id = cr.run_id UNION ALL
    SELECT 'WKLT_TGT_CNT', cs.var_json:TgtSuccessRows::STRING FROM control_status cs WHERE cs.task_name = 'm_base_agmt_sts_hist' AND cs.run_id = cr.run_id
  ) vals
  WHERE cr.worklet_name = 'wklt_base_agmt_sts_hist'
  ORDER BY cr.insert_ts DESC
  LIMIT 1
) src
ON tgt.run_id = src.run_id
AND tgt.scope_type = src.scope_type
AND tgt.scope_name = src.scope_name
AND tgt.param_name = src.param_name
WHEN MATCHED THEN
  UPDATE SET param_value = src.param_value, insert_ts = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (run_id, scope_type, scope_name, param_name, param_value, insert_ts)
  VALUES (src.run_id, src.scope_type, src.scope_name, src.param_name, src.param_value, CURRENT_TIMESTAMP());