-- Object Type: TASKS
-- Level: L2, ExecOrder: 5

create or replace task ALFA_EDW_DEV.PUBLIC.WF_BASE_7_POINTS_BI_FEED_MONTHLY__WKLT_BASE_AGT_ACCTBLY_BI_FEED
	warehouse=COMPUTE_WH
	after ALFA_EDW_DEV.PUBLIC.WF_BASE_7_POINTS_BI_FEED_MONTHLY_ROOT
	as CALL sp_launch_worklet('wklt_base_agt_acctbly_bi_feed', (SELECT run_id FROM control_worklet WHERE worklet_name = 'wf_base_7_points_bi_feed_monthly' ORDER BY insert_ts DESC LIMIT 1));