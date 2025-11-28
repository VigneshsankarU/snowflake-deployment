-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_SET_PARAMS_FOR_WORKLET("WORKLET_NAME" VARCHAR, "PARAM_OBJECT" OBJECT)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  v_run_id STRING;
  workflow_name STRING;
  param_key STRING;
  param_value STRING;
  last_qid STRING;
  resolved_value STRING;
  c1 CURSOR FOR
    SELECT key, value::STRING as value
    FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?)));
BEGIN
  -- Get latest run_id for the worklet
  v_run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
  workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);

  -- Loop over each key-value pair in the param_object
  OPEN c1 USING (:param_object::STRING);
  FOR param_row IN c1 DO
    param_key := param_row.key;
    resolved_value := param_row.value;
    
    -- Check if value starts with ''@QUERY:''
    IF (LEFT(param_row.value, 7) = ''@QUERY:'') THEN
        -- Run dynamic query
        EXECUTE IMMEDIATE SUBSTR(param_row.value, 8);
        last_qid := LAST_QUERY_ID();

        resolved_value := (SELECT $1 FROM TABLE(RESULT_SCAN(:last_qid)) LIMIT 1);
    ELSE
        -- Literal value
        resolved_value := param_row.value;
    END IF;


    -- Upsert into control_params
    MERGE INTO control_params tgt
    USING (
      SELECT
        :v_run_id AS run_id,
        ''2_worklet'' AS scope_type,
        :workflow_name || '':'' || :worklet_name AS scope_name,
        :param_key AS param_name,
        :resolved_value AS param_value
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

  CLOSE c1;

END;
';