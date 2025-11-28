-- Object Type: TASKS
create or replace task ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED__SET_RECORD_CNT
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WKLT_BASE_AGT_ACCTBLY_BI_FEED__S_M_BASE_AGT_ACCTBLY_BI_FEED
	as INSERT INTO control_params (run_id, param_name, param_value, insert_ts)
SELECT run_id, param_name, param_value, CURRENT_TIMESTAMP()
FROM (
  SELECT run_id,
         'WKLT_SUB_PRCS_NAME' AS param_name,
         's_m_base_agt_acctbly_bi_feed' AS param_value
  FROM (SELECT run_id FROM control_worklet WHERE worklet_name = 'wklt_base_agt_acctbly_bi_feed' ORDER BY insert_ts DESC LIMIT 1)

  UNION ALL

  SELECT run_id, 'WKLT_SRC_NM', 'EDW'
  FROM (SELECT run_id FROM control_worklet WHERE worklet_name = 'wklt_base_agt_acctbly_bi_feed' ORDER BY insert_ts DESC LIMIT 1)

  UNION ALL

  SELECT cs.run_id,
         'WKLT_SRC_EXTRACT_DT',
         cs.var_json:StartTime::STRING
  FROM control_status cs
  WHERE cs.task_name = 'm_base_agt_acctbly_bi_feed'
    AND cs.run_id = (SELECT run_id FROM control_worklet WHERE worklet_name = 'wklt_base_agt_acctbly_bi_feed' ORDER BY insert_ts DESC LIMIT 1)

  UNION ALL

  SELECT cs.run_id,
         'WKLT_SRC_CNT',
         cs.var_json:SrcSuccessRows::STRING
  FROM control_status cs
  WHERE cs.task_name = 'm_base_agt_acctbly_bi_feed'
    AND cs.run_id = (SELECT run_id FROM control_worklet WHERE worklet_name = 'wklt_base_agt_acctbly_bi_feed' ORDER BY insert_ts DESC LIMIT 1)

  UNION ALL

  SELECT cs.run_id,
         'WKLT_TGT_CNT',
         cs.var_json:TgtSuccessRows::STRING
  FROM control_status cs
  WHERE cs.task_name = 'm_base_agt_acctbly_bi_feed'
    AND cs.run_id = (SELECT run_id FROM control_worklet WHERE worklet_name = 'wklt_base_agt_acctbly_bi_feed' ORDER BY insert_ts DESC LIMIT 1)
);