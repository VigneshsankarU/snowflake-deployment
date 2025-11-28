-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_SET_PARAMS_DYNAMIC("WORKLET_NAME" VARCHAR, "PARAM_OBJECT" OBJECT)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  v_run_id STRING;
  param_key STRING;
  param_value STRING;
  last_qid STRING;
  resolved_value STRING;
BEGIN
  -- Get latest run_id for the worklet
  SELECT run_id INTO v_run_id
  FROM control_run_id
  WHERE worklet_name = :worklet_name
  ORDER BY insert_ts DESC
  LIMIT 1;

  -- Loop over each key-value pair in the param_object
  FOR param_row IN (
    SELECT key AS param_key, value::STRING AS param_value
    FROM TABLE(FLATTEN(INPUT => :param_object))
  ) DO

    -- Check if param_value starts with ''@QUERY:''
    IF(LEFT(param_row.param_value, 7) = ''@QUERY:'') THEN
      -- Execute the query part dynamically and get single value
      EXECUTE IMMEDIATE SUBSTR(param_row.param_value, 8);
      last_qid := LAST_QUERY_ID();
      SELECT $1 INTO resolved_value FROM TABLE(RESULT_SCAN(:last_qid));
    ELSE
      resolved_value := param_row.param_value;
    END IF;

    -- Upsert into control_params
    MERGE INTO control_params tgt
    USING (
      SELECT
        :v_run_id AS run_id,
        ''2_worklet'' AS scope_type,
        :worklet_name AS scope_name,
        param_row.param_key AS param_name,
        resolved_value AS param_value
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

  END FOR;

END;
';