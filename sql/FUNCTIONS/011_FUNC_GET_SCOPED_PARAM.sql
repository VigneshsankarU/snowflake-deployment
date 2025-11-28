-- Object Type: FUNCTIONS
CREATE OR REPLACE FUNCTION ALFA_EDW_DEV.PUBLIC.FUNC_GET_SCOPED_PARAM("RUN_ID" VARCHAR, "PARAM_NAME" VARCHAR, "WORKFLOW_NAME" VARCHAR, "WORKLET_NAME" VARCHAR, "SESSION_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS '
    SELECT param_value
    FROM control_params cp
    WHERE cp.run_id = run_id
      AND cp.param_name = param_name
      AND (
        (cp.scope_type = ''1_session'' AND cp.scope_name = session_name)
        OR (cp.scope_type = ''2_worklet'' AND cp.scope_name = CONCAT(workflow_name, '':'', worklet_name))
        OR (cp.scope_type = ''3_workflow'' AND cp.scope_name = workflow_name)
        OR (cp.scope_type = ''4_global'' AND cp.scope_name IS NULL)
      )
    ORDER BY cp.scope_type
    LIMIT 1
';